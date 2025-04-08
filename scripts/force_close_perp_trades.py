#!/usr/bin/env python3
import time
from decimal import Decimal
from typing import Dict, List, Set, Optional, Union, Any

from pydantic import Field, validator
# Import the correct V2 base classes
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionSide, TradeType
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig


# V2-compliant configuration class
class ForceCloseConfig(StrategyV2ConfigBase):
    """Configuration for the ForceClosePerpTrades strategy"""
    # Strategy info
    strategy_name: str = "force_close_perp_trades"
    
    # Exchange settings
    exchange: str = Field(
        ..., # Required field
        description="The exchange connector name to close positions on"
    )
    
    # Operating parameters
    max_retry_attempts: int = Field(
        default=3,
        description="Maximum number of retries for failed orders"
    )
    slippage_tolerance: Decimal = Field(
        default=Decimal("0.005"),
        description="Maximum allowed slippage (0.005 = 0.5%)"
    )
    max_position_close_time: int = Field(
        default=60,
        description="Seconds to wait before retrying a close attempt"
    )
    # Add the fields that were causing validation errors
    log_level: str = Field(
        default="INFO",
        description="Log level for this specific script (Options: DEBUG, INFO, WARNING, ERROR)"
    )
    update_interval: float = Field(
        default=1.0,
        description="How frequently the strategy checks positions (in seconds)"
    )
    
    # Empty candles_config list - required for StrategyV2Base
    candles_config: List[CandlesConfig] = Field(
        default_factory=list,
        description="Candle configuration (not used in this strategy)"
    )
    
    # Define markets property correctly
    @property
    def markets(self) -> Dict[str, Set[str]]:
        """Return the markets dictionary for this strategy"""
        return {self.exchange: set()}


class ForceClosePerpTrades(StrategyV2Base):
    """
    A V2 strategy to force close all perpetual positions on a specified exchange.
    
    This strategy:
    1. Connects to the specified exchange
    2. Finds all open perpetual positions
    3. Places market orders to close all positions
    4. Monitors for successful closure and retries if needed
    5. Stops once all positions are closed or maximum retries reached
    """
    
    # CRITICAL: StrategyV2 REQUIRES this class variable
    markets = {}  # Set properly in init_markets
    
    @classmethod
    def init_markets(cls, config: ForceCloseConfig):
        """Initialize markets from the config (V2 pattern)"""
        # Ensure it's a dictionary with proper structure for .items() method
        cls.markets = {config.exchange: set()}
        return cls.markets
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: ForceCloseConfig):
        """Initialize the strategy according to V2 pattern"""
        # Pass all arguments to parent constructor
        super().__init__(connectors, config)
        
        # Store configuration
        self._config = config
        self._exchange = config.exchange
        
        # Tracking variables
        self._closing_positions: Set[str] = set()
        self._closed_positions: Set[str] = set()
        self._retry_counts: Dict[str, int] = {}
        self._position_close_start_times: Dict[str, float] = {}
        self._all_positions_closed = False
        self._initial_check_done = False
        self._stop_initiated = False
        
    def on_tick(self):
        """Main control loop (V2 uses on_tick, not tick)"""
        if not self.ready_to_trade:
            return
        
        if self._all_positions_closed:
            if not self._stop_initiated:
                self.logger().info("All positions are closed. Stopping strategy.")
                self.close_execution_by("all_positions_closed")  # V2 pattern
                self._stop_initiated = True
            return
        
        connector = self.connectors.get(self._exchange)
        if not connector:
            self.logger().error(f"Exchange connector '{self._exchange}' not found.")
            self.close_execution_by("connector_not_found")
            return
        
        # Check connector supports perpetual trading
        if not hasattr(connector, "account_positions"):
            self.logger().error(f"Exchange {self._exchange} does not support perpetual trading.")
            self.close_execution_by("not_perpetual_exchange")
            return
        
        # Initial position check
        if not self._initial_check_done:
            self._perform_initial_check(connector)
            if self._all_positions_closed:
                self.close_execution_by("no_positions_found")
                return
            self._initial_check_done = True
        
        # Process any open positions
        self._process_positions(connector)
    
    def _perform_initial_check(self, connector):
        """Initial check and logging of positions"""
        self.logger().info(f"==== Force Close Perpetual Trades (V2) on {self._exchange} ====")
        self.logger().info(
            f"Settings: Max retries: {self._config.max_retry_attempts}, "
            f"Slippage tolerance: {self._config.slippage_tolerance * 100:.2f}%, "
            f"Close timeout: {self._config.max_position_close_time}s"
        )
        
        try:
            positions = connector.account_positions
            if not positions:
                self.logger().info("No open positions found.")
                self._all_positions_closed = True
                return
            
            self.logger().info(f"Found {len(positions)} positions to close:")
            for pos_key, position in positions.items():
                self._retry_counts[pos_key] = 0  # Initialize retry count
                self.logger().info(
                    f"  - {position.trading_pair} | {position.position_side} | "
                    f"Amount: {position.amount} | Entry: {position.entry_price}"
                )
        except Exception as e:
            self.logger().error(f"Error fetching positions: {e}", exc_info=True)
            self.close_execution_by("error_fetching_positions")
            return
        
        self._initial_check_done = True
    
    def _process_positions(self, connector):
        """Process all positions and attempt to close them"""
        current_positions = connector.account_positions
        if not current_positions:
            self.logger().info("All positions have been closed.")
            self._all_positions_closed = True
            return
            
        timed_out_positions = set()
        current_time = time.time()
        
        # Check for timeouts
        for pos_key in list(self._position_close_start_times.keys()):
            if pos_key not in current_positions:
                continue  # Position already closed
            
            elapsed_time = current_time - self._position_close_start_times[pos_key]
            if elapsed_time > self._config.max_position_close_time:
                self.logger().warning(
                    f"Position {pos_key} close timed out after {elapsed_time:.1f}s."
                )
                if pos_key in self._closing_positions:
                    self._closing_positions.remove(pos_key)
                timed_out_positions.add(pos_key)
        
        # Process each open position
        for pos_key, position in list(current_positions.items()):
            needs_close = (pos_key not in self._closing_positions and 
                         pos_key not in self._closed_positions)
            needs_retry = pos_key in timed_out_positions
            
            if needs_close or needs_retry:
                # Check retry count
                current_retry_count = self._retry_counts.get(pos_key, 0)
                next_retry_count = current_retry_count + 1
                
                if next_retry_count > self._config.max_retry_attempts:
                    self.logger().error(
                        f"Position {pos_key} failed to close after {self._config.max_retry_attempts} attempts."
                    )
                    self._closed_positions.add(pos_key)
                    if pos_key in self._closing_positions:
                        self._closing_positions.remove(pos_key)
                    continue
                
                # Attempt to close the position
                retry_text = f"(Retry #{next_retry_count})" if needs_retry or current_retry_count > 0 else ""
                self.logger().info(f"Closing position {pos_key} {retry_text}")
                self._close_position(position)
                
                # Update tracking
                self._retry_counts[pos_key] = next_retry_count
                self._position_close_start_times[pos_key] = current_time
                self._closing_positions.add(pos_key)
                if pos_key in self._closed_positions:
                    self._closed_positions.remove(pos_key)
        
        # Update confirmed closed positions
        confirmed_closed = self._closing_positions - set(current_positions.keys())
        if confirmed_closed:
            for pos_key in confirmed_closed:
                if pos_key not in self._closed_positions:
                    self.logger().info(f"Position {pos_key} closed successfully.")
                    self._closed_positions.add(pos_key)
            self._closing_positions -= confirmed_closed
    
    def _close_position(self, position):
        """Submit a market order to close the position"""
        connector = self.connectors.get(self._exchange)
        if not connector:
            return
        
        pos_key = connector.position_key(position.trading_pair, position.position_side)
        
        try:
            # Determine order parameters
            side = TradeType.BUY if position.position_side == PositionSide.SHORT else TradeType.SELL
            amount = abs(position.amount)
            
            # Safety checks
            if amount <= Decimal("0"):
                self.logger().warning(f"Skipping {pos_key}: Invalid amount {amount}")
                self._closed_positions.add(pos_key)
                if pos_key in self._closing_positions:
                    self._closing_positions.remove(pos_key)
                return
            
            # Get trading rules
            trading_rule = connector.trading_rules.get(position.trading_pair)
            if trading_rule and amount < trading_rule.min_order_size:
                self.logger().warning(
                    f"Skipping {pos_key}: Amount {amount} below min size {trading_rule.min_order_size}"
                )
                self._closed_positions.add(pos_key)
                if pos_key in self._closing_positions:
                    self._closing_positions.remove(pos_key)
                return
            
            # Get price for slippage calculation
            price = None
            try:
                price = connector.get_price_by_type(position.trading_pair, side)
            except Exception:
                try:
                    price = connector.get_mid_price(position.trading_pair)
                    self.logger().warning(
                        f"Using mid price for {position.trading_pair} (order book price unavailable)"
                    )
                except Exception as e:
                    self.logger().error(f"Cannot get price for {position.trading_pair}: {e}")
                    if pos_key in self._closing_positions:
                        self._closing_positions.remove(pos_key)
                    return
            
            # Apply slippage tolerance
            if side == TradeType.BUY:  # Closing short, willing to pay more
                adjusted_price = price * (Decimal("1") + self._config.slippage_tolerance)
            else:  # Closing long, willing to accept less
                adjusted_price = price * (Decimal("1") - self._config.slippage_tolerance)
            
            # Quantize price if trading rules available
            if trading_rule:
                adjusted_price = connector.quantize_order_price(position.trading_pair, adjusted_price)
                
            # V2 uses create_executor actions
            # But for simple script, we can use direct connector methods
            if side == TradeType.BUY:
                order_id = connector.buy(
                    trading_pair=position.trading_pair,
                    amount=amount,
                    order_type=OrderType.MARKET,
                    price=adjusted_price,
                    position_action=PositionAction.CLOSE
                )
            else:
                order_id = connector.sell(
                    trading_pair=position.trading_pair,
                    amount=amount,
                    order_type=OrderType.MARKET,
                    price=adjusted_price,
                    position_action=PositionAction.CLOSE
                )
            
            self.logger().info(
                f"Submitted order to close {amount} {position.trading_pair} "
                f"({position.position_side}) at {adjusted_price}"
            )
        
        except Exception as e:
            self.logger().error(f"Error closing position {pos_key}: {e}", exc_info=True)
            if pos_key in self._closing_positions:
                self._closing_positions.remove(pos_key)
            if pos_key in self._position_close_start_times:
                del self._position_close_start_times[pos_key]
    
    def close_execution_by(self, reason: str):
        """V2 pattern for closing strategy execution"""
        self.logger().info(f"Stopping strategy due to: {reason}")
        self._log_final_status()
        # V2Base doesn't have stop() method, but will stop through executor orchestration
    
    def _log_final_status(self):
        """Log final strategy status"""
        self.logger().info(f"==== Force Close Strategy Summary for {self._exchange} ====")
        
        # Get final positions
        connector = self.connectors.get(self._exchange)
        remaining_positions = {}
        
        if connector and hasattr(connector, "account_positions"):
            try:
                remaining_positions = connector.account_positions
            except Exception as e:
                self.logger().error(f"Error fetching final positions: {e}")
        
        # Log results
        if self._all_positions_closed:
            self.logger().info("All targeted positions have been closed successfully.")
        else:
            # Log positions that couldn't be closed
            failed_positions = []
            for key, pos in remaining_positions.items():
                if key in self._retry_counts:
                    failed_positions.append(
                        f"{pos.trading_pair} {pos.position_side} {pos.amount} "
                        f"(Attempts: {self._retry_counts.get(key, 'unknown')})"
                    )
            
            if failed_positions:
                self.logger().error(
                    f"MANUAL INTERVENTION NEEDED: {len(failed_positions)} positions could not be closed:"
                )
                for pos_info in failed_positions:
                    self.logger().error(f"  - {pos_info}")
        
        self.logger().info("==== Force Close Strategy Completed ====")

# Required to register the strategy with Hummingbot
def start(config: ForceCloseConfig):
    """Entry point called by Hummingbot"""
    # Initialize our class-level markets variable
    ForceClosePerpTrades.init_markets(config)
    
    # Return the class itself - Hummingbot will instantiate it later
    return ForceClosePerpTrades 