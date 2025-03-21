# scripts/arbitrage_strategy.py

import asyncio
import logging
import os
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, ClassVar

import pandas as pd
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.connector.connector_base import ConnectorBase


class ArbitrageStrategyConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    markets: Dict[str, Set[str]] = Field(default_factory=dict)
    candles_config: List[CandlesConfig] = Field(default_factory=list)
    
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
    
    # Trading pair to monitor (optional)
    trading_pair: Optional[str] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trading pair (e.g., BTC-USDT) or press enter to scan all pairs: ",
            prompt_on_new=True))
    
    # Minimum profitability
    min_profitability: Decimal = Field(
        default=Decimal("0.5"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter minimum profitability percentage: ",
            prompt_on_new=True))
    
    # Order amount
    order_amount: Decimal = Field(
        default=Decimal("100"),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter order amount in quote currency: ",
            prompt_on_new=True))
    
    # Cooldown time
    cooldown_time: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter cooldown time between trades (seconds): ",
            prompt_on_new=True))
    
    # Max concurrent positions
    max_concurrent_positions: int = Field(
        default=3,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter maximum number of concurrent positions: ",
            prompt_on_new=True))

    def load_controller_configs(self) -> List[ControllerConfigBase]:
        controller_config = ArbitrageControllerConfig(
            controller_name="arbitrage_controller",
            trading_pair=self.trading_pair,
            connector1=self.connector1,
            connector2=self.connector2,
            min_profitability=self.min_profitability,
            order_amount=self.order_amount,
            cooldown_time=self.cooldown_time,
            max_concurrent_positions=self.max_concurrent_positions
        )
        return [controller_config]


# Define the ArbitrageExecutorConfig class
class ArbitrageExecutorConfig:
    def __init__(self, id: str, buying_market: ConnectorPair, selling_market: ConnectorPair, 
                 order_amount: Decimal, min_profitability: Decimal, max_retries: int = 3):
        self.id = id
        self.type = "arbitrage_executor"
        self.buying_market = buying_market
        self.selling_market = selling_market
        self.order_amount = order_amount
        self.min_profitability = min_profitability
        self.max_retries = max_retries


# Define the ArbitrageControllerConfig class
class ArbitrageControllerConfig(ControllerConfigBase):
    """
    Configuration for the arbitrage controller.
    """
    controller_name: str = "arbitrage_controller"
    controller_type: str = "arbitrage"
    candles_config: List[CandlesConfig] = Field(default_factory=list)
    
    # First connector (exchange)
    connector1: str
    
    # Second connector (exchange)
    connector2: str
    
    # Minimum price difference percentage to trigger trades
    min_profitability: Decimal
    
    # Order amount in quote currency
    order_amount: Decimal
    
    # Cooldown time between trades (in seconds)
    cooldown_time: int
    
    # Maximum number of concurrent arbitrage positions
    max_concurrent_positions: int
    
    # Trading pair to monitor (now optional as we'll scan for common pairs)
    trading_pair: Optional[str] = None
    
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
        Updates the markets dictionary with the trading pairs needed by this controller.
        """
        # If a specific trading pair is provided, use that
        if self.trading_pair:
            if self.connector1 not in markets:
                markets[self.connector1] = set()
            if self.connector2 not in markets:
                markets[self.connector2] = set()
            
            markets[self.connector1].add(self.trading_pair)
            markets[self.connector2].add(self.trading_pair)
        else:
            # For scanning all pairs, we need to ensure both connectors are in the markets dict
            if self.connector1 not in markets:
                markets[self.connector1] = set()
            if self.connector2 not in markets:
                markets[self.connector2] = set()
        
        return markets


class ArbitrageController(ControllerBase):
    """
    Controller for arbitrage strategies.
    """
    _logger = None
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
    
    def __init__(self, config: ArbitrageControllerConfig, market_data_provider, actions_queue, update_interval: float = 1.0):
        """
        Initialize the arbitrage controller.
        """
        super().__init__(config, market_data_provider, actions_queue, update_interval)
        self.config = config
        self.market_data_provider = market_data_provider
        self.actions_queue = actions_queue
        self.update_interval = update_interval
        
        # For tracking active executors
        self.executors_info = []
        self.executors_update_event = None
        
        # For storing processed data
        self.processed_data = {}
        
        # For tracking last trade timestamp
        self.last_trade_timestamp = 0
        
        # For tracking common trading pairs
        self.common_trading_pairs = []
        self.pair_last_trade_timestamps = {}
        
        self.logger().info("ArbitrageController initialized")
    
    async def start(self):
        """
        Starts the controller.
        """
        self.logger().info("Starting ArbitrageController...")
        self.executors_update_event = asyncio.Event()
        self.processed_data = {}
        self.last_trade_timestamp = 0
        self.common_trading_pairs = self.find_common_trading_pairs()
        self.pair_last_trade_timestamps = {pair: 0 for pair in self.common_trading_pairs}
        
        # Initialize processed data for each pair
        for pair in self.common_trading_pairs:
            self.processed_data[pair] = {
                "price_1": Decimal("0"),
                "price_2": Decimal("0"),
                "price_diff": Decimal("0"),
                "price_diff_pct": Decimal("0"),
                "signal": 0
            }
        
        self.status = RunnableStatus.RUNNING
        
        self.logger().info(f"ArbitrageController started with {len(self.common_trading_pairs)} common trading pairs")
        if self.common_trading_pairs:
            self.logger().info(f"Monitoring pairs: {', '.join(self.common_trading_pairs[:10])}" + 
                              (f" and {len(self.common_trading_pairs) - 10} more..." if len(self.common_trading_pairs) > 10 else ""))
    
    def find_common_trading_pairs(self) -> List[str]:
        """
        Finds trading pairs that are common to both exchanges.
        """
        # If a specific trading pair is provided, use that
        if self.config.trading_pair:
            self.logger().info(f"Using specified trading pair: {self.config.trading_pair}")
            return [self.config.trading_pair]
        
        # Otherwise, find common pairs
        self.logger().info(f"Scanning for common trading pairs between {self.config.connector1} and {self.config.connector2}...")
        connector1_pairs = self.market_data_provider.get_trading_pairs(self.config.connector1)
        connector2_pairs = self.market_data_provider.get_trading_pairs(self.config.connector2)
        
        # Find common pairs
        common_pairs = list(set(connector1_pairs).intersection(set(connector2_pairs)))
        
        # Log the common pairs
        self.logger().info(f"Found {len(common_pairs)} common trading pairs between {self.config.connector1} and {self.config.connector2}")
        if common_pairs:
            self.logger().info(f"Common pairs: {', '.join(common_pairs[:10])}" + 
                              (f" and {len(common_pairs) - 10} more..." if len(common_pairs) > 10 else ""))
        else:
            self.logger().warning(f"No common trading pairs found between {self.config.connector1} and {self.config.connector2}")
        
        return common_pairs
    
    def generate_signal(self, price1: float, price2: float) -> int:
        """
        Generates a trading signal based on price difference.
        
        :param price1: Price on the first exchange
        :param price2: Price on the second exchange
        :return: Signal (1 for buy on exchange 2, sell on exchange 1; -1 for buy on exchange 1, sell on exchange 2; 0 for no trade)
        """
        # Calculate price difference percentage
        price_diff_pct = abs((price1 - price2) / price2) * 100
        
        # If price difference is greater than min_profitability, generate a signal
        if price_diff_pct >= float(self.config.min_profitability):
            if price1 > price2:
                return 1  # Buy on exchange 2, sell on exchange 1
            else:
                return -1  # Buy on exchange 1, sell on exchange 2
        
        return 0  # No trade
    
    async def update(self):
        """
        Updates the controller state and determines actions to take.
        """
        if self.status != RunnableStatus.RUNNING:
            return
        
        # Determine actions to take
        actions = await self.determine_actions()
        
        # If there are actions to take, put them in the queue
        if actions:
            await self.actions_queue.put(actions)
            self.logger().info(f"Added {len(actions)} actions to the queue")
    
    async def determine_actions(self) -> List[ExecutorAction]:
        """
        Determines what actions to take based on the current market conditions.
        """
        # Get active executors
        active_executors = [e for e in self.executors_info if e.is_active]
        
        # Check if we've reached the maximum number of concurrent positions
        if len(active_executors) >= self.config.max_concurrent_positions:
            self.logger().debug(f"Maximum concurrent positions reached ({self.config.max_concurrent_positions})")
            return []
        
        # Determine actions
        actions = await self.determine_executor_actions()
        
        return actions
    
    async def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determines what actions to take based on the current market conditions.
        """
        actions = []
        current_time = self.market_data_provider.time()
        opportunities = []
        
        # Process each common trading pair
        for trading_pair in self.common_trading_pairs:
            try:
                # Check if we're in cooldown period for this pair
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
                price_diff_pct = abs((price1 - price2) / price2) * 100
                
                # Store processed data
                self.processed_data[trading_pair] = {
                    "price_1": price1,
                    "price_2": price2,
                    "price_diff": price1 - price2,
                    "price_diff_pct": price_diff_pct,
                    "signal": signal
                }
                
                # If there's a signal, add to opportunities
                if signal != 0:
                    opportunities.append((trading_pair, price_diff_pct, signal))
                
                # Check for active executors for this pair
                active_executors = [e for e in self.executors_info if e.is_active and trading_pair in e.id]
                
                # Check if we should close any positions (price difference is close to zero)
                for executor_info in active_executors:
                    # If the price difference is close to zero (within 0.1%), close the position
                    if price_diff_pct < 0.1:
                        self.logger().info(f"Closing position for {trading_pair} as price difference is now {price_diff_pct:.2f}%")
                        actions.append(StopExecutorAction(
                            controller_id=self.config.id,
                            executor_id=executor_info.id,
                            keep_position=False
                        ))
            
            except Exception as e:
                self.logger().error(f"Error processing pair {trading_pair}: {e}")
        
        # Sort opportunities by price difference percentage (descending)
        opportunities.sort(key=lambda x: x[1], reverse=True)
        
        # Log top opportunities
        if opportunities:
            self.logger().info(f"Top arbitrage opportunities:")
            for i, (pair, diff_pct, signal) in enumerate(opportunities[:5]):
                direction = "Buy on 2, Sell on 1" if signal == 1 else "Buy on 1, Sell on 2"
                self.logger().info(f"  {i+1}. {pair}: {diff_pct:.2f}% - {direction}")
        
        # Get active executors
        active_executors = [e for e in self.executors_info if e.is_active]
        
        # Check if we can create a new executor
        if opportunities and len(active_executors) < self.config.max_concurrent_positions:
            # Take the best opportunity
            best_pair, _, signal = opportunities[0]
            
            # Check if we already have an executor for this pair
            if not any(best_pair in e.id for e in active_executors):
                self.logger().info(f"Creating new arbitrage executor for {best_pair} with {signal}")
                
                # Create the executor
                if signal == 1:  # Buy on exchange 2, sell on exchange 1
                    buying_market = ConnectorPair(connector_name=self.config.connector2, trading_pair=best_pair)
                    selling_market = ConnectorPair(connector_name=self.config.connector1, trading_pair=best_pair)
                else:  # Buy on exchange 1, sell on exchange 2
                    buying_market = ConnectorPair(connector_name=self.config.connector1, trading_pair=best_pair)
                    selling_market = ConnectorPair(connector_name=self.config.connector2, trading_pair=best_pair)
                
                # Create a unique ID for the executor
                signal_str = "buy_2_sell_1" if signal == 1 else "buy_1_sell_2"
                executor_id = f"arbitrage_{best_pair}_{signal_str}_{int(current_time)}"
                
                # Create the executor config
                executor_config = ArbitrageExecutorConfig(
                    id=executor_id,
                    buying_market=buying_market,
                    selling_market=selling_market,
                    order_amount=self.config.order_amount,
                    min_profitability=self.config.min_profitability,
                    max_retries=3
                )
                
                # Create the executor action
                actions.append(CreateExecutorAction(
                    controller_id=self.config.id,
                    executor_config=executor_config
                ))
                
                # Update the last trade timestamp for this pair
                self.pair_last_trade_timestamps[best_pair] = current_time
                self.logger().info(f"Created arbitrage executor for {best_pair}")
        
        return actions
    
    async def stop(self):
        """
        Stops the controller.
        """
        self.logger().info("Stopping ArbitrageController...")
        self.status = RunnableStatus.TERMINATED
        self.logger().info("ArbitrageController stopped")
    
    def to_format_status(self) -> List[str]:
        """
        Returns the status of the controller as a list of strings.
        """
        lines = []
        lines.append("Arbitrage Strategy")
        lines.append(f"Status: {self.status.name}")
        lines.append(f"Exchanges: {self.config.connector1} and {self.config.connector2}")
        lines.append(f"Min Profitability: {self.config.min_profitability}%")
        lines.append(f"Order Amount: {self.config.order_amount}")
        lines.append(f"Cooldown Time: {self.config.cooldown_time} seconds")
        lines.append(f"Max Concurrent Positions: {self.config.max_concurrent_positions}")
        
        # Add common trading pairs info
        lines.append(f"Monitoring {len(self.common_trading_pairs)} common trading pairs")
        
        # Add top opportunities
        opportunities = []
        for pair, data in self.processed_data.items():
            if data["signal"] != 0:
                opportunities.append((pair, data["price_diff_pct"], data["signal"]))
        
        # Sort opportunities by price difference percentage (descending)
        opportunities.sort(key=lambda x: x[1], reverse=True)
        
        if opportunities:
            lines.append("\nTop Arbitrage Opportunities:")
            for i, (pair, diff_pct, signal) in enumerate(opportunities[:5]):
                direction = "Buy on 2, Sell on 1" if signal == 1 else "Buy on 1, Sell on 2"
                lines.append(f"  {i+1}. {pair}: {diff_pct:.2f}% - {direction}")
        else:
            lines.append("\nNo arbitrage opportunities found")
        
        # Add active executors info
        active_executors = [e for e in self.executors_info if e.is_active]
        if active_executors:
            lines.append("\nActive Executors:")
            for executor in active_executors:
                lines.append(f"  ID: {executor.id}")
                lines.append(f"  Net PnL: {executor.net_pnl_quote}")
                lines.append(f"  Created: {executor.timestamp}")
                lines.append("")
        
        return lines


class ArbitrageStrategy(StrategyV2Base):
    """
    Arbitrage strategy that monitors price differences between the same trading pair on two exchanges.
    """
    _logger = None
    markets: ClassVar[Dict[str, Set[str]]] = {}
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
    
    @classmethod
    def init_markets(cls, config: ArbitrageStrategyConfig):
        """Initialize the markets for the strategy"""
        cls.markets = {}
        if config.connector1 not in cls.markets:
            cls.markets[config.connector1] = set()
        if config.connector2 not in cls.markets:
            cls.markets[config.connector2] = set()
        
        if config.trading_pair:
            cls.markets[config.connector1].add(config.trading_pair)
            cls.markets[config.connector2].add(config.trading_pair)
        return cls.markets

    def __init__(self, connectors: Dict[str, ConnectorBase] = None, config: Optional[ArbitrageStrategyConfig] = None):
        """Initialize the arbitrage strategy."""
        if connectors is None:
            connectors = {}
        
        # Make sure markets are initialized
        if not self.markets and config is not None:
            self.init_markets(config)
        
        super().__init__(connectors=connectors, config=config)
        self.config = config
        self.logger().info("ArbitrageStrategy initialized")
    
    def initialize_controllers(self):
        """Initialize controllers for the strategy"""
        if self.config is None:
            return
        
        controller_config = ArbitrageControllerConfig(
            controller_name="arbitrage_controller",
            trading_pair=self.config.trading_pair,
            connector1=self.config.connector1,
            connector2=self.config.connector2,
            min_profitability=self.config.min_profitability,
            order_amount=self.config.order_amount,
            cooldown_time=self.config.cooldown_time,
            max_concurrent_positions=self.config.max_concurrent_positions
        )
        
        self.controllers["arbitrage_controller"] = ArbitrageController(
            config=controller_config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue,
            update_interval=1.0
        )
        
    async def process_actions(self):
        """Process actions from controllers"""
        while True:
            action = await self.actions_queue.get()
            self.logger().debug(f"Processing action: {action}")
            
            if isinstance(action, CreateExecutorAction):
                await self.executor_orchestrator.create_executor(action.executor_config)
            elif isinstance(action, StopExecutorAction):
                await self.executor_orchestrator.stop_executor(action.executor_id)
            else:
                self.logger().warning(f"Unknown action type: {type(action)}")
                
            self.actions_queue.task_done()


def start():
    """
    Main entry point for the strategy.
    """
    config = ArbitrageStrategyConfig()
    ArbitrageStrategy.init_markets(config)  # Initialize markets before creating strategy
    strategy = ArbitrageStrategy(config=config)
    return strategy