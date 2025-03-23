# hummingbot/strategy_v2/controllers/arbitrage_controller.py

from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class ArbitrageControllerConfig(ControllerConfigBase):
    """
    Configuration for the arbitrage controller.
    """
    controller_type: str = "arbitrage"
    
    # Trading pair to monitor (now optional)
    trading_pair: Optional[str] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trading pair to monitor (e.g., ETH-USDT) or press enter to scan all pairs: ",
            prompt_on_new=True))
    
    # First connector (exchange)
    connector1: str = Field(
        default="binance",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the first connector name (e.g., binance): ",
            prompt_on_new=True))
    
    # Second connector (exchange)
    connector2: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the second connector name (e.g., kucoin): ",
            prompt_on_new=True))
    
    # Minimum price difference percentage to trigger trades
    min_profitability: Decimal = Field(
        default=Decimal("0.5"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the minimum price difference percentage to trigger trades (e.g., 0.5 for 0.5%): ",
            prompt_on_new=True))
    
    # Order amount in quote currency
    order_amount: Decimal = Field(
        default=Decimal("100"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the order amount in quote currency (e.g., 100 USDT): ",
            prompt_on_new=True))
    
    # Cooldown time between trades (in seconds)
    cooldown_time: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the cooldown time between trades in seconds (e.g., 60): ",
            prompt_on_new=True))
    
    # Maximum number of concurrent arbitrage positions
    max_concurrent_positions: int = Field(
        default=3,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the maximum number of concurrent arbitrage positions (e.g., 3): ",
            prompt_on_new=True))
    
    @validator('min_profitability', pre=True)
    def validate_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return v
    
    @validator('order_amount', pre=True)
    def validate_order_amount(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return v
    
    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        """
        Updates the markets dictionary with the trading pairs needed for this controller.
        """
        if self.connector1 not in markets:
            markets[self.connector1] = set()
        if self.connector2 not in markets:
            markets[self.connector2] = set()
        
        # Only add the specific trading pair if provided
        if self.trading_pair:
            markets[self.connector1].add(self.trading_pair)
            markets[self.connector2].add(self.trading_pair)
        
        return markets


class ArbitrageController(ControllerBase):
    """
    Controller for arbitrage strategy.
    """
    
    def __init__(self, config: ArbitrageControllerConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.last_trade_timestamp = 0
        self.common_trading_pairs = []
        self.pair_last_trade_timestamps = {}
        self.pair_processed_data = {}
    
    async def start(self):
        """
        Starts the controller and initializes trading pairs.
        """
        await super().start()
        
        # Initialize trading pairs
        if self.config.trading_pair:
            self.common_trading_pairs = [self.config.trading_pair]
        else:
            self.common_trading_pairs = self.find_common_trading_pairs()
        
        self.pair_last_trade_timestamps = {pair: 0 for pair in self.common_trading_pairs}
        self.pair_processed_data = {pair: {} for pair in self.common_trading_pairs}
        
        # Initialize processed data for the main display
        self.processed_data = {
            "opportunities": [],
            "active_pairs": []
        }
    
    def find_common_trading_pairs(self) -> List[str]:
        """
        Finds trading pairs that are common to both exchanges.
        """
        connector1_pairs = self.market_data_provider.get_trading_pairs(self.config.connector1)
        connector2_pairs = self.market_data_provider.get_trading_pairs(self.config.connector2)
        
        # Find common pairs
        common_pairs = list(set(connector1_pairs).intersection(set(connector2_pairs)))
        return common_pairs
    
    def generate_signal(self, price1: float, price2: float) -> int:
        """
        Generates a trading signal based on the price difference.
        Returns:
            1: Buy on connector2, sell on connector1
            -1: Buy on connector1, sell on connector2
            0: No trade
        """
        price_diff_pct = ((price1 - price2) / price2) * 100
        
        if price_diff_pct >= self.config.min_profitability:
            return 1  # Buy on connector2, sell on connector1
        elif price_diff_pct <= -self.config.min_profitability:
            return -1  # Buy on connector1, sell on connector2
        else:
            return 0  # No trade
    
    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determines what actions to take based on the current market conditions.
        This method is called periodically by the controller base class.
        """
        if not self.common_trading_pairs:
            # Not initialized yet, no actions
            return []
            
        actions = []
        current_time = self.market_data_provider.time()
        opportunities = self.processed_data.get("opportunities", [])
        
        # Check for active executors that should be stopped
        active_executors = [e for e in self.executors_info if e.is_active]
        for executor_info in active_executors:
            try:
                # Extract trading pair from executor ID
                pair = next((p for p in self.common_trading_pairs if p in executor_info.id), None)
                if not pair:
                    continue
                    
                # Get current price data
                price1 = self.market_data_provider.get_price(self.config.connector1, pair)
                price2 = self.market_data_provider.get_price(self.config.connector2, pair)
                
                # Skip if prices are not available
                if price1 is None or price2 is None or price1 == 0 or price2 == 0:
                    continue
                    
                # Calculate current price difference
                price_diff_pct = ((price1 - price2) / price2) * 100
                
                # Check if we should close the position (price difference is close to zero)
                if abs(float(price_diff_pct)) < 0.1:
                    actions.append(StopExecutorAction(
                        controller_id=self.config.id,
                        executor_id=executor_info.id,
                        keep_position=False
                    ))
                    self.logger().info(f"Stopping executor {executor_info.id} as price difference is now {price_diff_pct:.4f}%")
            except Exception as e:
                self.logger().error(f"Error checking executor {executor_info.id}: {e}")
        
        # Check if we can create a new executor
        if opportunities and len(active_executors) < self.config.max_concurrent_positions:
            # Take the best opportunity
            best_pair, best_diff, signal = opportunities[0]
            
            # Check if we already have an executor for this pair
            if not any(best_pair in e.id for e in active_executors):
                try:
                    if signal == 1:  # Buy on connector2, sell on connector1
                        actions.append(self.create_arbitrage_executor(
                            trading_pair=best_pair,
                            buying_market=ConnectorPair(connector_name=self.config.connector2, trading_pair=best_pair),
                            selling_market=ConnectorPair(connector_name=self.config.connector1, trading_pair=best_pair),
                            signal=signal
                        ))
                        self.logger().info(f"Creating arbitrage executor for {best_pair}: Buy on {self.config.connector2}, Sell on {self.config.connector1}, Profit: {best_diff:.2f}%")
                    elif signal == -1:  # Buy on connector1, sell on connector2
                        actions.append(self.create_arbitrage_executor(
                            trading_pair=best_pair,
                            buying_market=ConnectorPair(connector_name=self.config.connector1, trading_pair=best_pair),
                            selling_market=ConnectorPair(connector_name=self.config.connector2, trading_pair=best_pair),
                            signal=signal
                        ))
                        self.logger().info(f"Creating arbitrage executor for {best_pair}: Buy on {self.config.connector1}, Sell on {self.config.connector2}, Profit: {best_diff:.2f}%")
                    
                    # Update last trade timestamp for this pair
                    self.pair_last_trade_timestamps[best_pair] = current_time
                except Exception as e:
                    self.logger().error(f"Error creating executor for {best_pair}: {e}")
        
        return actions
    
    def create_arbitrage_executor(self, trading_pair: str, buying_market: ConnectorPair, selling_market: ConnectorPair, signal: int) -> CreateExecutorAction:
        """
        Creates an arbitrage executor configuration.
        """
        # Calculate the order amount in base currency
        base_asset = trading_pair.split("-")[0]
        quote_asset = trading_pair.split("-")[1]
        
        # Get the price on the buying exchange
        buy_price = self.market_data_provider.get_price(buying_market.connector_name, buying_market.trading_pair)
        
        # Calculate the amount of base asset to buy
        order_amount_base = self.config.order_amount / buy_price
        
        # Create a unique ID for the executor
        signal_str = "buy_2_sell_1" if signal == 1 else "buy_1_sell_2"
        executor_id = f"arbitrage_{trading_pair}_{signal_str}_{int(self.market_data_provider.time())}"
        
        # Create the executor config
        executor_config = ArbitrageExecutorConfig(
            id=executor_id,
            buying_market=buying_market,
            selling_market=selling_market,
            order_amount=order_amount_base,
            min_profitability=self.config.min_profitability,
            max_retries=3
        )
        
        return CreateExecutorAction(
            controller_id=self.config.id,
            executor_config=executor_config
        )
    
    def to_format_status(self) -> List[str]:
        """
        Returns the status of the controller in a formatted string.
        """
        lines = []
        
        # Add general information
        lines.append(f"Arbitrage Controller - Monitoring {len(self.common_trading_pairs)} pairs")
        lines.append(f"Exchanges: {self.config.connector1} and {self.config.connector2}")
        lines.append(f"Min Profitability: {self.config.min_profitability}%")
        lines.append(f"Order Amount: {self.config.order_amount} {self.common_trading_pairs[0].split('-')[1] if self.common_trading_pairs else 'USDT'}")
        
        # Add top opportunities
        opportunities = self.processed_data.get("opportunities", [])
        if opportunities:
            lines.append("\nTop Arbitrage Opportunities:")
            for i, (pair, diff_pct, signal) in enumerate(opportunities):
                direction = "Buy on 2, Sell on 1" if signal == 1 else "Buy on 1, Sell on 2"
                lines.append(f"  {i+1}. {pair}: {diff_pct:.2f}% - {direction}")
        else:
            lines.append("\nNo arbitrage opportunities found")
        
        # Add active executors
        active_executors = [e for e in self.executors_info if e.is_active]
        if active_executors:
            lines.append("\nActive Arbitrage Positions:")
            for executor in active_executors:
                # Extract trading pair from executor ID
                pair = next((p for p in self.common_trading_pairs if p in executor.id), "Unknown")
                quote_asset = pair.split('-')[1] if pair != "Unknown" else ""
                
                lines.append(f"  ID: {executor.id}")
                lines.append(f"  Pair: {pair}")
                lines.append(f"  Net PnL: {executor.net_pnl_quote} {quote_asset}")
                lines.append(f"  Created: {executor.timestamp}")
                lines.append("")
        
        return lines

    async def control_loop(self):
        """
        Main control loop for the arbitrage controller.
        Handles initialization, error recovery, and opportunity detection.
        """
        try:
            # Check if market data provider is ready
            if not self.market_data_provider.ready:
                self.logger().info("Market data provider not ready. Waiting...")
                await asyncio.sleep(5.0)
                return
            
            # Verify connectors exist and are ready
            for connector_name in [self.config.connector1, self.config.connector2]:
                if connector_name not in self.market_data_provider.connectors:
                    self.logger().error(f"Connector '{connector_name}' not found in available connectors.")
                    self.logger().info(f"Available connectors: {list(self.market_data_provider.connectors.keys())}")
                    await asyncio.sleep(10.0)
                    return
                
                # Check if connector is ready
                if not self.market_data_provider.connectors[connector_name].ready:
                    self.logger().info(f"Connector '{connector_name}' not ready yet. Waiting...")
                    await asyncio.sleep(5.0)
                    return
            
            # Initialize trading pairs if needed
            if not self.common_trading_pairs:
                if self.config.trading_pair:
                    # Check if the specified trading pair exists on both exchanges
                    pair_on_ex1 = self.config.trading_pair in self.market_data_provider.get_trading_pairs(self.config.connector1)
                    pair_on_ex2 = self.config.trading_pair in self.market_data_provider.get_trading_pairs(self.config.connector2)
                    
                    if not pair_on_ex1 or not pair_on_ex2:
                        missing_on = []
                        if not pair_on_ex1:
                            missing_on.append(self.config.connector1)
                        if not pair_on_ex2:
                            missing_on.append(self.config.connector2)
                            
                        self.logger().error(f"Trading pair '{self.config.trading_pair}' not available on: {', '.join(missing_on)}")
                        await asyncio.sleep(30.0)
                        return
                    
                    self.common_trading_pairs = [self.config.trading_pair]
                    self.logger().info(f"Using configured trading pair: {self.config.trading_pair}")
                else:
                    # Find common pairs between exchanges
                    try:
                        self.common_trading_pairs = self.find_common_trading_pairs()
                        if not self.common_trading_pairs:
                            self.logger().warning("No common trading pairs found between exchanges.")
                            await asyncio.sleep(30.0)
                            return
                        self.logger().info(f"Found {len(self.common_trading_pairs)} common trading pairs.")
                    except Exception as e:
                        self.logger().error(f"Error finding common pairs: {str(e)}", exc_info=True)
                        await asyncio.sleep(10.0)
                        return
                
                # Initialize tracking data structures
                self.pair_last_trade_timestamps = {pair: 0 for pair in self.common_trading_pairs}
                self.pair_processed_data = {pair: {} for pair in self.common_trading_pairs}
            
            # Process arbitrage opportunities
            current_time = self.market_data_provider.time()
            opportunities = []
            
            # Process each trading pair
            for trading_pair in self.common_trading_pairs:
                try:
                    # Skip pairs in cooldown
                    if current_time - self.pair_last_trade_timestamps.get(trading_pair, 0) < self.config.cooldown_time:
                        continue
                    
                    # Get current prices
                    price1 = self.market_data_provider.get_price(self.config.connector1, trading_pair)
                    price2 = self.market_data_provider.get_price(self.config.connector2, trading_pair)
                    
                    # Skip if either price is None or zero
                    if price1 is None or price2 is None or price1 == 0 or price2 == 0:
                        continue
                    
                    # Calculate price difference and generate signal
                    signal = self.generate_signal(float(price1), float(price2))
                    price_diff_pct = ((price1 - price2) / price2) * 100
                    
                    # Store processed data for this pair
                    self.pair_processed_data[trading_pair] = {
                        "price_1": price1,
                        "price_2": price2,
                        "price_diff": price1 - price2,
                        "price_diff_pct": price_diff_pct,
                        "signal": signal
                    }
                    
                    # If there's a signal, add to opportunities
                    if signal != 0:
                        opportunities.append((trading_pair, abs(price_diff_pct), signal))
                    
                except Exception as e:
                    # Log error but continue with next pair
                    self.logger().error(f"Error processing pair {trading_pair}: {e}")
            
            # Update processed data for display
            if opportunities:
                # Sort opportunities by price difference percentage (descending)
                opportunities.sort(key=lambda x: x[1], reverse=True)
                self.processed_data["opportunities"] = opportunities[:5]
            else:
                self.processed_data["opportunities"] = []
            
            # Update active pairs
            active_executors = [e for e in self.executors_info if e.is_active]
            self.processed_data["active_pairs"] = [e.id for e in active_executors]
            
        except Exception as e:
            self.logger().error(f"Unexpected error in control loop: {str(e)}", exc_info=True)
            # Don't sleep here as the base controller will handle the sleep interval