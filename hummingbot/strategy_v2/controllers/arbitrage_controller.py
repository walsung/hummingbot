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
    
    # Trading pair to monitor (must be the same on both exchanges)
    trading_pair: str = Field(
        default="ETH-USDT",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trading pair to monitor (e.g., ETH-USDT): ",
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
        """
        actions = []
        
        # Check if we're in cooldown period
        current_time = self.market_data_provider.time()
        if current_time - self.last_trade_timestamp < self.config.cooldown_time:
            return actions
        
        # Get current prices
        price1 = self.market_data_provider.get_price(self.config.connector1, self.config.trading_pair)
        price2 = self.market_data_provider.get_price(self.config.connector2, self.config.trading_pair)
        
        # Calculate price difference and generate signal
        signal = self.generate_signal(float(price1), float(price2))
        
        # Store processed data
        self.processed_data.update({
            "price_1": price1,
            "price_2": price2,
            "price_diff": price1 - price2,
            "price_diff_pct": ((price1 - price2) / price2) * 100,
            "signal": signal
        })
        
        # Check for active executors
        active_executors = [e for e in self.executors_info if e.is_active]
        
        # Check if we should close any positions (price difference is close to zero)
        for executor_info in active_executors:
            # Extract the original signal from the executor ID
            executor_signal = 1 if "buy_2_sell_1" in executor_info.id else -1 if "buy_1_sell_2" in executor_info.id else 0
            
            # If the price difference is close to zero (within 0.1%), close the position
            if abs(float(self.processed_data["price_diff_pct"])) < 0.1:
                actions.append(StopExecutorAction(
                    controller_id=self.config.id,
                    executor_id=executor_info.id,
                    keep_position=False
                ))
        
        # Check if we should create new positions
        if signal != 0 and len(active_executors) < self.config.max_concurrent_positions:
            # Check if we already have a position with the same signal
            existing_signals = []
            for executor_info in active_executors:
                if "buy_2_sell_1" in executor_info.id:
                    existing_signals.append(1)
                elif "buy_1_sell_2" in executor_info.id:
                    existing_signals.append(-1)
            
            # Only create a new position if we don't already have one with the same signal
            if signal not in existing_signals:
                if signal == 1:  # Buy on connector2, sell on connector1
                    actions.append(self.create_arbitrage_executor(
                        buying_market=ConnectorPair(connector_name=self.config.connector2, trading_pair=self.config.trading_pair),
                        selling_market=ConnectorPair(connector_name=self.config.connector1, trading_pair=self.config.trading_pair),
                        signal=signal
                    ))
                elif signal == -1:  # Buy on connector1, sell on connector2
                    actions.append(self.create_arbitrage_executor(
                        buying_market=ConnectorPair(connector_name=self.config.connector1, trading_pair=self.config.trading_pair),
                        selling_market=ConnectorPair(connector_name=self.config.connector2, trading_pair=self.config.trading_pair),
                        signal=signal
                    ))
                
                # Update last trade timestamp
                self.last_trade_timestamp = current_time
        
        return actions
    
    def create_arbitrage_executor(self, buying_market: ConnectorPair, selling_market: ConnectorPair, signal: int) -> CreateExecutorAction:
        """
        Creates an arbitrage executor configuration.
        """
        # Calculate the order amount in base currency
        base_asset = self.config.trading_pair.split("-")[0]
        quote_asset = self.config.trading_pair.split("-")[1]
        
        # Get the price on the buying exchange
        buy_price = self.market_data_provider.get_price(buying_market.connector_name, buying_market.trading_pair)
        
        # Calculate the amount of base asset to buy
        order_amount_base = self.config.order_amount / buy_price
        
        # Create a unique ID for the executor
        signal_str = "buy_2_sell_1" if signal == 1 else "buy_1_sell_2"
        executor_id = f"arbitrage_{signal_str}_{int(self.market_data_provider.time())}"
        
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
        
        # Add current prices and difference
        if "price_1" in self.processed_data and "price_2" in self.processed_data:
            lines.append(f"Exchange 1 ({self.config.connector1}) Price: {self.processed_data['price_1']}")
            lines.append(f"Exchange 2 ({self.config.connector2}) Price: {self.processed_data['price_2']}")
            lines.append(f"Price Difference: {self.processed_data['price_diff']}")
            lines.append(f"Price Difference %: {self.processed_data['price_diff_pct']:.4f}%")
            lines.append(f"Signal: {self.processed_data['signal']}")
        
        # Add active executors
        active_executors = [e for e in self.executors_info if e.is_active]
        if active_executors:
            lines.append("\nActive Arbitrage Positions:")
            for executor in active_executors:
                lines.append(f"  ID: {executor.id}")
                lines.append(f"  Net PnL: {executor.net_pnl_quote} {self.config.trading_pair.split('-')[1]}")
                lines.append(f"  Created: {executor.timestamp}")
                lines.append("")
        
        # Add recent data if available
        df = self.processed_data.get("features", pd.DataFrame())
        if not df.empty:
            lines.append("\nRecent Data:")
            lines.append(format_df_for_printout(df.tail(5), table_format="psql"))
        
        return lines