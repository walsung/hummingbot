#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import yaml
from decimal import Decimal
from typing import Dict, List, Optional, Set

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.runnable_base import RunnableBase


class ForceClosePerpTrades(RunnableBase):
    """
    A strategy that force closes all open perpetual positions for a specified exchange.
    
    Usage:
    1. Configure the target exchange in the YAML config file
    2. Run the strategy to close all positions
    3. The strategy will stop automatically once all positions are closed
    """
    
    _logger = None
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
    
    # Required fields for the YAML config
    @classmethod
    def config_fields(cls) -> Dict[str, ClientFieldData]:
        return {
            "exchange": ClientFieldData(
                key="exchange",
                prompt="Enter the name of the perpetual exchange to close positions on",
                required=True,
                is_connect_key=True,
                prompt_on_new=True,
            ),
            "config_file_path": ClientFieldData(
                key="config_file_path",
                prompt="(Optional) Enter the path to the configuration file",
                required=False,
                is_connect_key=False,
                prompt_on_new=False,
            ),
        }
    
    def __init__(
        self,
        client_config_map: ClientConfigAdapter,
        exchange: str,
        config_file_path: Optional[str] = None,
        update_interval: float = 1.0,
    ):
        """Initialize the ForceClosePerpTrades strategy
        
        :param client_config_map: Global client configuration
        :param exchange: Name of the exchange to close positions on
        :param config_file_path: Path to the config file (optional)
        :param update_interval: How frequently to update the strategy
        """
        self._client_config_map = client_config_map
        self._exchange = exchange
        self._config_file_path = config_file_path
        
        # Default configuration values
        self._update_interval = update_interval
        self._max_retry_attempts = 3
        self._slippage_tolerance = Decimal("0.005")  # 0.5%
        self._max_position_close_time = 60  # seconds
        self._log_level = "INFO"
        
        # Load additional configuration from YAML if provided
        self._config = {}
        if config_file_path and os.path.exists(config_file_path):
            with open(config_file_path, "r") as file:
                self._config = yaml.safe_load(file)
                self._parse_config()
                self.logger().info(f"Loaded configuration from {config_file_path}")
        
        # Apply configuration
        self._set_log_level()
        
        # Initialize parent class with updated interval
        super().__init__(self._update_interval)
        
        # Keep track of positions we already attempted to close to avoid duplicate orders
        self._closing_positions: Set[str] = set()
        self._closed_positions: Set[str] = set()
        self._retry_counts: Dict[str, int] = {}
        self._position_close_start_times: Dict[str, float] = {}
        self._connector = None
        
        # Flag to ensure we clean up and stop after all positions are closed
        self._all_positions_closed = False
    
    def _parse_config(self):
        """Parse values from the loaded config file"""
        if not self._config:
            return
            
        # Extract configuration values with defaults
        if "update_interval" in self._config:
            self._update_interval = float(self._config["update_interval"])
            
        if "max_retry_attempts" in self._config:
            self._max_retry_attempts = int(self._config["max_retry_attempts"])
            
        if "slippage_tolerance" in self._config:
            self._slippage_tolerance = Decimal(str(self._config["slippage_tolerance"]))
            
        if "max_position_close_time" in self._config:
            self._max_position_close_time = int(self._config["max_position_close_time"])
            
        if "log_level" in self._config:
            self._log_level = self._config["log_level"]
            
        # Override exchange if specified in config
        if "exchange" in self._config and not self._exchange:
            self._exchange = self._config["exchange"]
    
    def _set_log_level(self):
        """Set the logger level based on configuration"""
        log_level = logging.INFO
        
        if self._log_level == "DEBUG":
            log_level = logging.DEBUG
        elif self._log_level == "INFO":
            log_level = logging.INFO
        elif self._log_level == "WARNING":
            log_level = logging.WARNING
        elif self._log_level == "ERROR":
            log_level = logging.ERROR
            
        self.logger().setLevel(log_level)
        
    async def on_start(self):
        """Called when the strategy starts"""
        from hummingbot.client.hummingbot_application import HummingbotApplication
        
        # Get the exchange connector
        try:
            self._connector = HummingbotApplication.main_application().markets[self._exchange]
            
            # Check if the connector is perpetual
            if not hasattr(self._connector, "account_positions"):
                self.logger().error(f"Exchange {self._exchange} is not a perpetual exchange.")
                self.stop()
                return
                
            self.logger().info(f"Starting force close of all positions on {self._exchange}")
            self.logger().info(f"Configuration: Update interval: {self._update_interval}s, "
                              f"Max retries: {self._max_retry_attempts}, "
                              f"Slippage tolerance: {self._slippage_tolerance*100}%, "
                              f"Max close time: {self._max_position_close_time}s")
            
            # Log current positions
            positions = self._connector.account_positions
            if not positions:
                self.logger().info(f"No open positions found on {self._exchange}")
                self._all_positions_closed = True
                self.stop()
                return
                
            for pos_key, position in positions.items():
                self.logger().info(
                    f"Found position: {position.trading_pair}, "
                    f"Side: {position.position_side}, "
                    f"Amount: {position.amount}, "
                    f"Entry Price: {position.entry_price}, "
                    f"Leverage: {position.leverage}x"
                )
                
        except KeyError:
            self.logger().error(f"Exchange {self._exchange} not found or not initialized.")
            self.stop()
            
    async def control_task(self):
        """Main control loop that runs on every update interval"""
        if self._connector is None or self._all_positions_closed:
            self.stop()
            return
            
        # Get current positions
        current_positions = self._connector.account_positions
        
        # Check if all positions are closed
        if not current_positions:
            if self._closing_positions:
                self.logger().info("All positions have been closed successfully!")
                self._all_positions_closed = True
                self.stop()
            else:
                self.logger().info(f"No open positions found on {self._exchange}")
                self._all_positions_closed = True
                self.stop()
            return
            
        # Track positions that timed out
        timed_out_positions = set()
        current_time = time.time()
        
        # Close each position
        for pos_key, position in current_positions.items():
            # Check if position has timed out
            if pos_key in self._position_close_start_times:
                elapsed_time = current_time - self._position_close_start_times[pos_key]
                if elapsed_time > self._max_position_close_time:
                    self.logger().warning(
                        f"Position {pos_key} close timed out after {elapsed_time:.1f}s. "
                        f"Will retry."
                    )
                    timed_out_positions.add(pos_key)
            
            # Check if we need to close this position
            if (pos_key not in self._closing_positions and 
                pos_key not in self._closed_positions) or pos_key in timed_out_positions:
                
                # Check retry count
                if pos_key in self._retry_counts and self._retry_counts[pos_key] >= self._max_retry_attempts:
                    self.logger().error(
                        f"Failed to close position {pos_key} after {self._max_retry_attempts} attempts. "
                        f"Manual intervention required."
                    )
                    continue
                
                # Attempt to close the position
                await self._close_position(position)
                
                # Update tracking data
                if pos_key not in self._retry_counts:
                    self._retry_counts[pos_key] = 0
                
                if pos_key in timed_out_positions:
                    self._retry_counts[pos_key] += 1
                    self.logger().info(f"Retry #{self._retry_counts[pos_key]} for position {pos_key}")
                
                self._position_close_start_times[pos_key] = current_time
                self._closing_positions.add(pos_key)
                
        # Update closed positions by comparing with current positions
        closed_positions = self._closing_positions - set(current_positions.keys())
        if closed_positions:
            for pos_key in closed_positions:
                self.logger().info(f"Position {pos_key} closed successfully")
                self._closed_positions.add(pos_key)
            self._closing_positions -= closed_positions
            
    async def _close_position(self, position: Position):
        """Close a single position
        
        :param position: Position object to close
        """
        try:
            # Determine the trade type to close the position
            side = TradeType.BUY if position.position_side == PositionSide.SHORT else TradeType.SELL
            
            # Log closure attempt
            self.logger().info(
                f"Closing position: {position.trading_pair}, "
                f"Side: {position.position_side}, "
                f"Amount: {abs(position.amount)}, "
                f"Action: {'BUY' if side == TradeType.BUY else 'SELL'}"
            )
            
            # Get current market price
            current_price = self._connector.get_price_by_type(
                position.trading_pair, 
                side
            )
            
            # Apply slippage tolerance to price for limit orders
            # For BUY orders (closing SHORT), increase price
            # For SELL orders (closing LONG), decrease price
            adjusted_price = current_price
            if side == TradeType.BUY:
                adjusted_price = current_price * (Decimal("1") + self._slippage_tolerance)
            else:
                adjusted_price = current_price * (Decimal("1") - self._slippage_tolerance)
            
            # Create a market order to close the position
            client_order_id = await self._connector._create_order(
                trade_type=side,
                order_id=f"close_{position.trading_pair}_{position.position_side}_{self._connector.current_timestamp}",
                trading_pair=position.trading_pair,
                amount=abs(position.amount),
                order_type=OrderType.MARKET,
                price=adjusted_price,  # For MARKET orders, this is used as a maximum slippage price
                position_action=PositionAction.CLOSE,
            )
            
            self.logger().info(
                f"Order {client_order_id} submitted to close position {position.trading_pair} "
                f"({position.position_side})"
            )
            
        except Exception as e:
            self.logger().error(
                f"Error closing position {position.trading_pair} ({position.position_side}): {e}",
                exc_info=True
            )
            # Don't increment retry count here, since we'll retry on the next control_task loop

    def on_stop(self):
        """Called when the strategy stops"""
        if self._all_positions_closed:
            self.logger().info(f"Strategy completed. All positions on {self._exchange} have been closed.")
        else:
            # Log positions that couldn't be closed
            remaining_positions = self._connector.account_positions if self._connector else {}
            if remaining_positions:
                self.logger().warning(f"Strategy stopped with {len(remaining_positions)} positions still open:")
                for pos_key, position in remaining_positions.items():
                    self.logger().warning(
                        f"Remaining position: {position.trading_pair}, "
                        f"Side: {position.position_side}, "
                        f"Amount: {position.amount}"
                    )
            else:
                self.logger().warning(f"Strategy stopped before confirming all positions were closed!")


def main():
    """
    This function is used when running the script standalone from the command line.
    When running as a script from Hummingbot, the bot creates and starts the strategy directly.
    """
    import argparse
    from hummingbot.client.config.config_helpers import ClientConfigAdapter
    from hummingbot.client.config.global_config_map import global_config_map
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Force close all perpetual positions on an exchange")
    parser.add_argument("-e", "--exchange", type=str, help="Exchange name (e.g., binance_perpetual)")
    parser.add_argument("-c", "--config", type=str, help="Path to configuration file")
    parser.add_argument("-t", "--timeout", type=int, default=300, 
                        help="Maximum run time in seconds before forcing exit")
    args = parser.parse_args()
    
    # Create sample config
    client_config_map = ClientConfigAdapter(global_config_map)
    
    # Set exchange and config from command line or defaults
    exchange = args.exchange or "binance_perpetual"
    config_path = args.config or "config.yml"
    timeout = args.timeout
    
    # Create and run strategy
    strategy = ForceClosePerpTrades(
        client_config_map=client_config_map,
        exchange=exchange,
        config_file_path=config_path,
    )
    
    # Run the strategy
    async def run_strategy():
        strategy.start()
        # Run for specified timeout
        await asyncio.sleep(timeout)
        if strategy.status != RunnableStatus.TERMINATED:
            print(f"Strategy timed out after {timeout} seconds. Stopping.")
            strategy.stop()
    
    # Run the event loop
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_strategy())
    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Stopping strategy...")
        strategy.stop()


# This is the entry point for the script when run directly from Hummingbot
def run(args=None):
    """
    Wrapper to run the script from the Hummingbot client.
    This is the entry point that Hummingbot uses when running with start --script.
    """
    # When running from hummingbot, we need to handle arguments differently
    # as they come from the --conf parameter
    if isinstance(args, dict):
        # The strategy will be created and run by the script handler in Hummingbot
        return args
    else:
        # If not running from Hummingbot, run the main function
        main()

if __name__ == "__main__":
    main() 