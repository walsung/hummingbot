"""
Rebalance Perpetual Strategy

This strategy rebalances perpetual futures positions based on configured thresholds.
"""

import logging
import pandas as pd
import time
import asyncio
import random
import os
from decimal import Decimal
from typing import Dict, List, Optional, Set, ClassVar, Any

from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import OrderType, TradeType, PositionMode
from hummingbot.core.data_type import common
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import DirectionalTradingControllerConfigBase
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.logger import HummingbotLogger


# Define the controller class
class RebalancePerpetualController(ControllerBase):
    """Controller for the rebalance perpetual strategy"""
    
    def __init__(self, config: ControllerConfigBase, market_data_provider, actions_queue):
        super().__init__(config, market_data_provider, actions_queue)
        self.config = config
        
    async def control_task(self):
        """Main control loop"""
        # This is a placeholder - the actual strategy logic is in the main class
        await asyncio.sleep(1)
        
    async def stop(self):
        """Stop the controller"""
        self.logger().info("Stopping rebalance perpetual controller")
        await super().stop()


class RebalancePerpetualControllerConfig(DirectionalTradingControllerConfigBase):
    """Configuration for the rebalance perpetual controller"""
    controller_name: str = "rebalance_perpetual_controller"
    controller_type: str = "rebalance_perpetual"
    
    # Trading parameters
    threshold: float = Field(
        default=0.05,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter threshold percentage for rebalancing (e.g., 0.05 for 5%): ",
            prompt_on_new=True))
    
    target_value: float = Field(
        default=200,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter target position value in quote currency: ",
            prompt_on_new=True))
    
    is_buy: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable buy orders? (True/False): ",
            prompt_on_new=True))
    
    # Additional parameters
    sell_markup_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter sell markup percentage (e.g., 0.1 for 0.1%): ",
            prompt_on_new=True))
    
    buy_discount_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter buy discount percentage (e.g., 0.1 for 0.1%): ",
            prompt_on_new=True))
    
    def get_controller_class(self):
        """Return the controller class for this config"""
        return RebalancePerpetualController


class RebalancePerpetualConfig(StrategyV2ConfigBase):
    """Configuration parameters for the Rebalance Perpetual strategy"""
    
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    markets: Dict[str, Set[str]] = Field(default_factory=dict)
    candles_config: List[CandlesConfig] = Field(default_factory=list)
    controllers_config: List[ControllerConfigBase] = Field(default_factory=list)
    
    # Strategy-specific fields
    connector_name: str = Field(
        default="binance_perpetual", 
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the perpetual connector name (e.g., binance_perpetual): ",
            prompt_on_new=True))
    
    position_mode: str = Field(
        default="HEDGE", 
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter position mode (HEDGE or ONEWAY): ",
            prompt_on_new=True))
    
    leverage: int = Field(
        default=4, 
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter leverage to use for trading: ",
            prompt_on_new=True))
    
    max_leverage: int = Field(
        default=4,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter maximum leverage allowed: ",
            prompt_on_new=True))
    
    min_leverage: int = Field(
        default=1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter minimum leverage allowed: ",
            prompt_on_new=True))
    
    trading_pairs: List[str] = Field(
        default_factory=list,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter trading pairs separated by commas: ",
            prompt_on_new=True))
    
    order_type: str = Field(
        default="LIMIT",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter order type (LIMIT or MARKET): ",
            prompt_on_new=True))
    
    threshold: float = Field(
        default=0.05,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter threshold percentage for rebalancing (e.g., 0.05 for 5%): ",
            prompt_on_new=True))
    
    target_value: float = Field(
        default=200,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter target position value in quote currency: ",
            prompt_on_new=True))
    
    is_buy: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable buy orders? (True/False): ",
            prompt_on_new=True))
    
    sell_markup_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter sell markup percentage (e.g., 0.1 for 0.1%): ",
            prompt_on_new=True))
    
    buy_discount_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter buy discount percentage (e.g., 0.1 for 0.1%): ",
            prompt_on_new=True))
    
    max_pairs_per_cycle: int = Field(
        default=5,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter maximum pairs to process per cycle: ",
            prompt_on_new=True))
    
    delay_between_orders_sec: float = Field(
        default=0.5,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter delay between orders in seconds: ",
            prompt_on_new=True))
    
    default_min_amount: float = Field(
        default=0.001,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter default minimum order amount: ",
            prompt_on_new=True))
    
    buy_interval: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter interval between rebalance checks (seconds): ",
            prompt_on_new=True))
    
    # Add validators as needed
    @validator('trading_pairs', pre=True, allow_reuse=True)
    def parse_trading_pairs(cls, v):
        if isinstance(v, str):
            return [pair.strip() for pair in v.split(',')]
        return v
    
    def load_controller_configs(self) -> List[ControllerConfigBase]:
        controller_config = RebalancePerpetualControllerConfig(
            controller_name="rebalance_perpetual_controller",
            controller_type="rebalance_perpetual",
            connector_name=self.connector_name,
            position_mode=self.position_mode,
            leverage=self.leverage,
            trading_pair=self.trading_pairs[0] if self.trading_pairs else "",
            threshold=self.threshold,
            target_value=self.target_value,
            is_buy=self.is_buy,
            sell_markup_pct=self.sell_markup_pct,
            buy_discount_pct=self.buy_discount_pct
        )
        return [controller_config]


class RebalancePerpetual(StrategyV2Base):
    """
    This strategy rebalances perpetual futures positions based on configured thresholds.
    """
    _logger = None
    markets: ClassVar[Dict[str, Set[str]]] = {}
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
    
    @classmethod
    def init_markets(cls, config: RebalancePerpetualConfig):
        """Initialize the markets for the strategy"""
        cls.markets = {config.connector_name: set(config.trading_pairs)}
        return cls.markets
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[RebalancePerpetualConfig] = None):
        if config is None:
            config = RebalancePerpetualConfig()
        
        # Initialize with empty connectors if needed
        if not connectors:
            self.logger().warning("No connectors available. The strategy will prompt you to create one.")
            # We'll continue initialization but will prompt for connector creation later
            super().__init__(connectors, config)
            self.config = config
            self.connector_name = config.connector_name
            self.ready_to_trade = False
            return
        
        # Make sure the connector exists before trying to access it
        if config.connector_name not in connectors:
            available_connectors = list(connectors.keys())
            self.logger().warning(f"Connector '{config.connector_name}' not found. Available connectors: {available_connectors}")
            
            # Prompt for connector selection if available
            if available_connectors:
                # Use the first available connector as default
                config.connector_name = available_connectors[0]
                self.logger().info(f"Using '{config.connector_name}' as fallback connector")
            else:
                self.logger().warning("No connectors available. Please run 'connect binance_perpetual' or another perpetual connector.")
                super().__init__(connectors, config)
                self.config = config
                self.connector_name = config.connector_name
                self.ready_to_trade = False
                return
        
        super().__init__(connectors, config)
        self.config = config
        
        # Initialize timestamp for interval checking
        self.last_ordered_ts = 0
        
        # Store configuration
        self.connector_name = config.connector_name
        self.connector = connectors[self.connector_name]
        
        # Position settings
        self.position_mode = PositionMode[config.position_mode.upper()] if isinstance(config.position_mode, str) else config.position_mode
        self.leverage = config.leverage
        self.max_leverage = config.max_leverage
        self.min_leverage = config.min_leverage
        
        # Strategy parameters
        self.rb = {
            "connector_name": self.connector_name,
            "trading_pair": config.trading_pairs,
            "is_buy": config.is_buy,
            "threshold": Decimal(str(config.threshold)),
            "target_value": Decimal(str(config.target_value)),
            "status": "",
        }
        
        # Trading pairs
        self.trading_pair = self.rb["trading_pair"]
        
        # Check interval in seconds
        self.buy_interval = config.buy_interval
        
        # Store the current price of the asset and the dict of order
        self.price = {}
        self.activate_order_id = {}
        self.asset_value = {}
        
        # Hedge fund variables
        self.set_leverage_flag = False
        
        # Minimal quantity
        self.min_amount = {}
        
        # Create throttler
        self.create_throttler()
        
        # Validate trading pairs and set leverage
        self.validate_trading_pairs()
        self.check_and_set_leverage()
        
        # Initialize the strategy
        self.initialize_strategy()

    def create_throttler(self):
        """Create a throttler with exchange's specific rate limits"""
        try:
            rate_limits = []
            rate_limits.append(RateLimit(limit_id="GET", limit=10, time_interval=1))
            rate_limits.append(RateLimit(limit_id="POST", limit=10, time_interval=1))
            self.throttler = AsyncThrottler(rate_limits=rate_limits)
        except Exception as e:
            self.logger().error(f"Error creating throttler: {str(e)}")

    def initialize_strategy(self):
        """Initialize the strategy"""
        self.logger().info("Initializing Rebalance Perpetual strategy...")
        self.logger().info(f"Connector: {self.connector_name}")
        self.logger().info(f"Position Mode: {self.position_mode}")
        self.logger().info(f"Leverage: {self.leverage}")
        self.logger().info(f"Trading Pairs: {self.trading_pair}")
        self.logger().info(f"Threshold: {self.rb['threshold']}")
        self.logger().info(f"Target Value: {self.rb['target_value']}")
        self.logger().info(f"Is Buy: {self.rb['is_buy']}")
        
        # Initialize minimum amounts for each trading pair
        for tp in self.trading_pair:
            self.min_amount[tp] = Decimal(str(self.config.default_min_amount))
        
        self.logger().info("Strategy initialization complete")

    def validate_trading_pairs(self):
        """Validate trading pairs and set minimum amounts"""
        valid_trading_pairs = []
        
        for tp in self.trading_pair:
            try:
                # Check if the trading pair exists on the exchange
                self.connector.get_mid_price(tp)
                valid_trading_pairs.append(tp)
            except Exception as e:
                self.logger().warning(f"Trading pair {tp} is not valid: {str(e)}")
        
        # Update trading pairs with only valid ones
        self.trading_pair = valid_trading_pairs
        self.rb["trading_pair"] = valid_trading_pairs
        
        self.logger().info(f"Using {len(valid_trading_pairs)} valid trading pairs: {valid_trading_pairs}")

    def check_and_set_leverage(self):
        if not self.set_leverage_flag:
            perp_connector = self.connector
            try:
                perp_connector.set_position_mode(self.position_mode)
                
                # Set leverage for each validated trading pair
                for trading_pair in self.trading_pair:
                    try:
                        perp_connector.set_leverage(
                            trading_pair=trading_pair, leverage=self.leverage
                        )
                        self.logger().info(f"Set leverage to {self.leverage} for {trading_pair}")
                    except Exception as e:
                        self.logger().warning(f"Error setting leverage for {trading_pair}: {str(e)}")
                
                self.logger().info(
                    f"Leverage setting completed for {len(self.trading_pair)} trading pairs"
                )
            except Exception as e:
                self.logger().error(f"Error setting position mode: {str(e)}")
            
            self.set_leverage_flag = True
    
    def on_tick(self):
        """
        Main strategy logic, execute at an interval
        """
        # Check if connectors are available
        if not self.ready_to_trade:
            self.logger().warning("Strategy not ready to trade. Please run 'connect binance_perpetual' or another perpetual connector.")
            return
        
        # Check if it reaches the next checkpoint interval
        if self.last_ordered_ts < (self.current_timestamp - self.buy_interval):
            # Calculate the value of the position and compare with the target value
            if self.rb.get("status") == "":
                # Initialize the position
                self.init_rebalance()
            elif self.rb["status"] == "ACTIVATE":
                try:
                    # Use safe_ensure_future to run these operations with rate limiting
                    safe_ensure_future(self.rate_limited_operations())
                except Exception as e:
                    self.logger().error(f"Error in on_tick: {str(e)}")
            self.last_ordered_ts = self.current_timestamp               
    
    # Cancel all order
    def cancel_all_order(self):
        for exchange in self.connectors.values():
            safe_ensure_future(exchange.cancel_all(timeout_seconds=6))

    # Initialization
    def init_rebalance(self):
        self.logger().info("Starting Rebalance Perpetual strategy...")
        self.rb["status"] = "ACTIVATE"
        self.market = self.rb["connector_name"]

    def get_balance(self):
        """
        Get the current balance status:
        1. Retrieve balance
        2. Get all trading pair status
        3. Calculate every trading pair value
        4. Calculate the gain/loss
        """
        self.logger().info("Retrieving order amount")
        balance = self.get_balance_df()
        
        # Fix the Series to float conversion error
        usdt_balance = balance.loc[balance['Asset'] == "USDT", 'Total Balance']
        if not usdt_balance.empty:
            self.balance = Decimal(float(usdt_balance.iloc[0]))
        else:
            self.logger().warning("USDT balance not found")
            self.balance = Decimal("0")
        
        # Get all positions
        df1 = self.connectors[self.rb["connector_name"]].account_positions
        total_unrealized_pnl = 0
        total_asset_value = 0
        for tp in self.trading_pair:
            position_pair = tp + "LONG"
            unrealized_pnl = 0
            if position_pair in df1:
                amount = Decimal(df1[position_pair].amount)
                unrealized_pnl = Decimal(df1[position_pair].unrealized_pnl)
            else:
                amount = 0
            price = Decimal(self.connectors[self.rb["connector_name"]].get_mid_price(tp))
            self.price[tp] = price
            self.asset_value[tp] = amount * price
            total_asset_value = self.asset_value[tp]
            total_unrealized_pnl = unrealized_pnl + total_unrealized_pnl

    def create_order(self):
        """
        Create order based on the difference between base asset value and target value
        1. If position value more than target value +threshold, sell 
        2. If position value less than target value -threshold, buy
        3. If within the threshold, then create both buy and sell orders
        """
        rb = self.rb.copy()
        # Process only a limited number of trading pairs at a time to avoid rate limits
        processed_pairs = 0
        max_pairs_per_cycle = self.config.max_pairs_per_cycle
        
        for tp in self.asset_value:
            if processed_pairs >= max_pairs_per_cycle:
                break
            
            if self.asset_value[tp] >= rb["target_value"] * (1 + rb["threshold"]):
                # Sell order: when position value is high
                self.sell(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT,
                    self.price[tp] * (1 + Decimal(str(self.config.sell_markup_pct)) / Decimal("100")),
                    common.PositionAction.CLOSE
                )
                processed_pairs += 1
                time.sleep(self.config.delay_between_orders_sec)  # Add delay between orders
            
            elif self.asset_value[tp] < rb["target_value"] * (1 - rb["threshold"]):
                # Open order: when position value is low
                self.buy(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT,
                    self.price[tp] * (1 - Decimal(str(self.config.buy_discount_pct)) / Decimal("100")),
                    common.PositionAction.OPEN
                )
                processed_pairs += 1
                time.sleep(self.config.delay_between_orders_sec)  # Add delay between orders
            
            else:
                # Only place one order (not both) to reduce API calls
                if random.choice([True, False]):
                    self.sell(
                        self.rb["connector_name"], 
                        tp,
                        max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                        OrderType.LIMIT,
                        self.price[tp] * (1 + Decimal(str(self.config.sell_markup_pct)) / Decimal("100")),
                        common.PositionAction.CLOSE
                    )
                else:
                    self.buy(
                        self.rb["connector_name"], 
                        tp,
                        max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                        OrderType.LIMIT,
                        self.price[tp] * (1 - Decimal(str(self.config.buy_discount_pct)) / Decimal("100")),
                        common.PositionAction.OPEN
                    )
                processed_pairs += 1
                time.sleep(self.config.delay_between_orders_sec)  # Add delay between orders
                
    # Format output
    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders. 
        This function is called when status command is issued.
        """                
        if not self.ready_to_trade:
            return """
    Market connectors are not ready.

    Please follow these steps:
    1. Run 'connect binance_perpetual' (or another perpetual connector)
    2. Restart the strategy with 'start --script rebalance_perpetual.py'
    """
        lines = []
        try:
            warning_lines = []
            warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))
            positions_df = self.get_positions_df()
            lines.extend(["", "  Positions:"] + ["     " + line for line in positions_df.to_string(index=False).split("\n")])
            orders_df = self.active_orders_df()
            lines.extend(["", "  Active Orders:"] + ["    " + line for line in orders_df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "   No active maker orders."])

        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return '\n'.join(lines)

    # Retrieve the position stats
    def get_positions_df(self) -> pd.DataFrame:
        """
        Returns a data frame for all asset positions for displaying purpose.
        """          
        columns: List[str] = ["Exchange", 
                              "Trading Pair", 
                              "Amount", 
                              "Entry Price", "Current Price",
                              "Unrealized pnl", 
                              "Percentage" 
                            ]
        data: List[Any] = []
        dc_position = self.connectors[self.connector_name].account_positions
        for trading_pair in dc_position:
            amount = Decimal(dc_position[trading_pair].amount)
            entry_price = Decimal(dc_position[trading_pair].entry_price)
            current_price = Decimal(self.connectors[self.connector_name].get_mid_price(trading_pair))
            unrealized_pnl = Decimal(dc_position[trading_pair].unrealized_pnl)
            percentage = round(unrealized_pnl/(abs(amount)*entry_price),4)
            tp = trading_pair.replace("LONG","")
            tp = tp.replace("SHORT","")
            data.append([self.connector_name, 
                         trading_pair, 
                         amount, 
                         entry_price, 
                         current_price, 
                         unrealized_pnl,
                         percentage])
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Exchange", "Trading Pair"], inplace=True)    
        return df

    async def rate_limited_operations(self):
        """Run operations with rate limiting to avoid API limits"""
        try:
            # Cancel all orders (POST operation)
            async with self.throttler.execute_task(limit_id="POST"):
                self.cancel_all_order()
            
            # Longer wait between operations
            await asyncio.sleep(5)
            
            # Get balance (GET operation)
            async with self.throttler.execute_task(limit_id="GET"):
                self.get_balance()
            
            # Longer wait between operations
            await asyncio.sleep(5)
            
            # Create order (POST operation)
            async with self.throttler.execute_task(limit_id="POST"):
                self.create_order()
        except Exception as e:
            self.logger().error(f"Error in rate_limited_operations: {str(e)}")