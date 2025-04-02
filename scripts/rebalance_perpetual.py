"""
StrategyV2 implementation for rebalancing perpetual futures positions.
This strategy manages position sizes based on target values and thresholds.

Connector supported: bybit_perpetual
"""

import logging
import pandas as pd
import numpy as np
import time
import asyncio
import random
from decimal import Decimal
from typing import ClassVar, Dict, List, Optional, Set, Tuple, Union, Any
from pydantic import Field, validator

from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from enum import Enum


class PositionMode(str, Enum):
    HEDGE = "Hedge"
    ONEWAY = "OneWay"


class RebalancePerpetualConfig(StrategyV2ConfigBase):
    """Configuration for the Rebalance Perpetual strategy"""
    script_file_name: str = Field(
        default="rebalance_perpetual.py",
        client_data=None,
    )
    
    # Exchange settings
    connector_name: str = Field(
        default="bybit_perpetual",
        client_data=ClientFieldData(
            prompt="Enter the connector name (e.g. bybit_perpetual)",
        )
    )
    
    # Trading pairs
    trading_pairs: List[str] = Field(
        default=["BTC-USDT", "ETH-USDT"],
        client_data=ClientFieldData(
            prompt="Enter the list of trading pairs (comma-separated)",
            prompt_on_new=True,
        )
    )
    
    # Position settings
    position_mode: str = Field(
        default="Hedge",
        client_data=ClientFieldData(
            prompt="Enter position mode (Hedge or OneWay)",
            prompt_on_new=True,
        )
    )
    leverage: int = Field(
        default=4,
        client_data=ClientFieldData(
            prompt="Enter leverage value",
            prompt_on_new=True,
        )
    )
    max_leverage: int = Field(
        default=4,
        client_data=ClientFieldData(
            prompt="Enter maximum leverage allowed",
        )
    )
    min_leverage: int = Field(
        default=2,
        client_data=ClientFieldData(
            prompt="Enter minimum leverage allowed",
        )
    )
    
    # Strategy parameters
    is_buy: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt="Do you want to buy? (True/False)",
            prompt_on_new=True,
        )
    )
    threshold: float = Field(
        default=0.05,
        client_data=ClientFieldData(
            prompt="Enter threshold value (e.g. 0.05 for 5%)",
            prompt_on_new=True,
        )
    )
    target_value: float = Field(
        default=200,
        client_data=ClientFieldData(
            prompt="Enter target position value in USD",
            prompt_on_new=True,
        )
    )
    buy_interval: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt="Enter checking interval in seconds",
            prompt_on_new=True,
        )
    )
    order_type: str = Field(
        default="LIMIT",
        client_data=ClientFieldData(
            prompt="Enter order type (LIMIT or MARKET)",
        )
    )
    sell_markup_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt="Enter percentage markup for sell orders",
        )
    )
    buy_discount_pct: float = Field(
        default=0.1,
        client_data=ClientFieldData(
            prompt="Enter percentage discount for buy orders",
        )
    )
    max_pairs_per_cycle: int = Field(
        default=5,
        client_data=ClientFieldData(
            prompt="Enter maximum pairs to process per cycle",
        )
    )
    delay_between_orders_sec: float = Field(
        default=0.5,
        client_data=ClientFieldData(
            prompt="Enter delay between orders in seconds",
        )
    )
    default_min_amount: float = Field(
        default=0.001,
        client_data=ClientFieldData(
            prompt="Enter default minimum order amount",
        )
    )
    
    # Replace the rate_limits field with direct parameters
    get_request_limit: int = Field(
        default=20,
        client_data=ClientFieldData(
            prompt="Enter GET request rate limit per second",
            prompt_on_new=False,
        )
    )
    
    post_request_limit: int = Field(
        default=5,
        client_data=ClientFieldData(
            prompt="Enter POST request rate limit per second",
            prompt_on_new=False,
        )
    )
    
    rate_limit_time_interval: float = Field(
        default=1.0,
        client_data=ClientFieldData(
            prompt="Enter rate limit time interval in seconds",
            prompt_on_new=False,
        )
    )
    
    # Min order amounts (will be moved to config)
    min_order_amounts: Dict[str, float] = Field(
        default={},
        client_data=ClientFieldData(
            prompt=None,  # This is too complex for direct prompting
            prompt_on_new=False,
        )
    )
    
    # Override the inherited candles_config with an empty list
    candles_config: List[CandlesConfig] = Field(
        default_factory=list,  # Empty list by default
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    # Override the inherited markets field too
    markets: Dict[str, Set[str]] = Field(
        default_factory=dict,  # Empty dict by default
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    @validator("trading_pairs", pre=True, allow_reuse=True)
    def validate_trading_pairs(cls, v):
        """Validate and format trading pairs"""
        if isinstance(v, str):
            return [pair.strip() for pair in v.split(",")]
        return v


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
        
        # Call super().__init__ first
        super().__init__(connectors, config)
        self.config = config
        
        # --- Initialize essential attributes early ---
        self.connector_name = config.connector_name
        self.buy_interval = config.buy_interval
        self.last_ordered_ts = 0
        self.ready_to_trade = False  # Assume not ready initially
        self.connector = None  # Initialize connector attribute
        self.rb = {
            "connector_name": self.connector_name,
            "trading_pair": config.trading_pairs,
            "is_buy": config.is_buy,
            "threshold": Decimal(str(config.threshold)),
            "target_value": Decimal(str(config.target_value)),
            "status": "",
        }
        self.trading_pair = self.rb["trading_pair"]
        self.price = {}
        self.activate_order_id = {}
        self.asset_value = {}
        self.set_leverage_flag = False
        self.min_amount = {}
        # --- End of early initialization ---
        
        # Check if connectors dictionary is provided
        if not connectors:
            self.logger().warning("No connectors dictionary provided. Strategy cannot start.")
            # self.ready_to_trade remains False
            return  # Exit initialization
        
        # Check if the configured connector exists in the provided dictionary
        if self.connector_name not in connectors:
            available_connectors = list(connectors.keys())
            self.logger().warning(f"Connector '{self.connector_name}' not found. Available connectors: {available_connectors}")
            # Attempt to use the first available connector as a fallback (consider if this is desired behavior)
            if available_connectors:
                self.connector_name = available_connectors[0]
                self.config.connector_name = self.connector_name  # Update config object as well
                self.rb["connector_name"] = self.connector_name  # Update rb dictionary
                self.logger().info(f"Using '{self.connector_name}' as fallback connector.")
                self.connector = connectors[self.connector_name]
            else:
                self.logger().error("No available connectors to use. Strategy cannot start.")
                # self.ready_to_trade remains False
                return  # Exit initialization
        else:
            # Assign the connector if it exists
            self.connector = connectors[self.connector_name]
        
        # Check connector readiness
        if not self.connector or not self.connector.ready:
            self.logger().warning(f"Connector {self.connector_name} is not ready. Please wait or run 'connect {self.connector_name}'.")
            # self.ready_to_trade remains False
            # Don't return here, allow on_tick to handle the not ready state
        else:
            self.ready_to_trade = True  # Set to True only if connector exists and is ready
        
        # Set up the position mode and leverage (only for perpetual connectors)
        if hasattr(self.connector, "set_leverage"):
            self.logger().info(f"Setting leverage to {config.leverage}")
            for tp in self.trading_pair:
                try:
                    self.connector.set_leverage(trading_pair=tp, leverage=config.leverage)
                except Exception as e:
                    self.logger().error(f"Error setting leverage for {tp}: {str(e)}")
        
        if hasattr(self.connector, "set_position_mode"):
            try:
                pos_mode = PositionMode[config.position_mode.upper()]
                self.logger().info(f"Setting position mode to {pos_mode.value}")
                self.connector.set_position_mode(pos_mode)
            except Exception as e:
                self.logger().error(f"Error setting position mode: {str(e)}")
        
        # Load minimum order amounts from config or use defaults
        self.initialize_min_amounts()
        
        # Initialize min notional value (can be retrieved from exchange info or set as a fallback)
        self.min_notional = Decimal("10.0")  # Default minimum notional value in USDT
        
        # Initialize the throttler using configs
        self.create_throttler()
        
        # Validate that the trading pairs exist on the exchange
        self.validate_trading_pairs()
        
        # Log initialization completed
        self.logger().info("Starting Rebalance Perpetual strategy...")
    
    def initialize_strategy(self):
        """Additional initialization after connector is ready"""
        # This method will be called after the connector is ready
        # Additional setup can be done here
        pass
    
    def initialize_min_amounts(self):
        """Initialize minimum order amounts for each trading pair"""
        # Use values from config if present, otherwise use a small default value
        for tp in self.trading_pair:
            if tp in self.config.min_order_amounts:
                self.min_amount[tp] = Decimal(str(self.config.min_order_amounts[tp]))
            else:
                self.min_amount[tp] = Decimal(str(self.config.default_min_amount))
    
    def create_throttler(self):
        """Create a throttler using configured rate limits"""
        try:
            # Create rate limits directly from config fields
            configured_rate_limits = [
                RateLimit(limit_id="GET", limit=self.config.get_request_limit, time_interval=self.config.rate_limit_time_interval),
                RateLimit(limit_id="POST", limit=self.config.post_request_limit, time_interval=self.config.rate_limit_time_interval)
            ]

            self.throttler = AsyncThrottler(rate_limits=configured_rate_limits)
            self.logger().info(f"Throttler created with limits: {[str(rl) for rl in configured_rate_limits]}")
        except Exception as e:
            self.logger().error(f"Error creating throttler: {str(e)}")
            self.throttler = None  # Ensure throttler is None on error to prevent later issues
    
    def validate_trading_pairs(self):
        """Validate trading pairs before any other operations"""
        perp_connector = self.connector
        valid_trading_pairs = []
        
        # Get all available trading pairs from the exchange
        all_exchange_trading_pairs = perp_connector._trading_pairs
        self.logger().info(f"Available trading pairs on {self.connector_name}: {all_exchange_trading_pairs}")
        
        # Filter our trading pairs list to only include valid ones
        for trading_pair in self.trading_pair:
            try:
                # Try to get the exchange symbol - this will fail if the pair doesn't exist
                exchange_symbol = perp_connector.exchange_symbol_associated_to_pair(trading_pair)
                valid_trading_pairs.append(trading_pair)
                self.logger().info(f"Validated trading pair: {trading_pair}")
            except Exception as e:
                self.logger().warning(f"Trading pair {trading_pair} not available on {self.connector_name}: {str(e)}. Skipping.")
        
        # Update trading_pair list with only valid pairs
        self.trading_pair = valid_trading_pairs
        self.rb["trading_pair"] = valid_trading_pairs
        
        # Also update min_amount dictionary to only include valid pairs
        valid_min_amounts = {}
        for tp in valid_trading_pairs:
            if tp in self.min_amount:
                valid_min_amounts[tp] = self.min_amount[tp]
        self.min_amount = valid_min_amounts
    
    def on_tick(self):
        """
        The main logic for the strategy, executed at every tick.
        Checks if it's time to execute a rebalance operation.
        """
        # First check if ready to trade
        if not self.ready_to_trade:
            if self.connector and self.connector.ready:
                self.ready_to_trade = True
                self.initialize_strategy()  # Initialize strategy now that connector is ready
            else:
                # Not ready to trade, log warning and exit
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
    
    def cancel_all_order(self):
        """Cancel all active orders"""
        for exchange in self.connectors.values():
            safe_ensure_future(exchange.cancel_all(timeout_seconds=6))
    
    def init_rebalance(self):
        """Initialize the rebalance strategy"""
        self.logger().info("Starting Initialization.....")
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
        Create orders based on the difference between base asset value and target value
        1. If position value more than target value +threshold, sell 
        2. If position value less than target value -threshold, buy
        3. If within the threshold, create either buy or sell order randomly
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
                    self.price[tp] * Decimal("1.001"),
                    PositionAction.CLOSE
                )
                processed_pairs += 1
                time.sleep(self.config.delay_between_orders_sec)  # Add delay between orders
            
            elif self.asset_value[tp] < rb["target_value"] * (1 - rb["threshold"]):
                # Buy order: when position value is low
                self.buy(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT,
                    self.price[tp] * Decimal("0.9999"),
                    PositionAction.OPEN
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
                        self.price[tp] * Decimal(1 + self.config.sell_markup_pct/100),
                        PositionAction.CLOSE
                    )
                else:
                    self.buy(
                        self.rb["connector_name"], 
                        tp,
                        max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                        OrderType.LIMIT,
                        self.price[tp] * Decimal(1 - self.config.buy_discount_pct/100),
                        PositionAction.OPEN
                    )
                processed_pairs += 1
                time.sleep(self.config.delay_between_orders_sec)  # Add delay between orders
    
    def format_status(self) -> str:
        """
        Returns status of the current strategy on user balances and current active orders.
        This function is called when status command is issued.
        """                
        if not self.ready_to_trade:
            return "Market connectors are not ready"
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
    
    def get_positions_df(self) -> pd.DataFrame:
        """
        Returns a data frame for all asset positions for displaying purpose.
        """          
        columns = ["Exchange", "Trading Pair", "Amount", "Entry Price", "Current Price", 
                  "Unrealized PnL", "Percentage"]
        data = []
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

    def get_strategy_config_class() -> type:
        """Return the config class for strategy"""
        return RebalancePerpetualConfig