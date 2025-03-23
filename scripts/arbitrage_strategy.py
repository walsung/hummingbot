# scripts/arbitrage_strategy.py
# start --script arbitrage_strategy.py --conf conf_arbitrage_strategy_1.yml

import asyncio
import logging
import os
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, ClassVar

import pandas as pd
from pydantic import Field, validator
import yaml

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
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.client import settings
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.exchange.paper_trade import PaperTradeExchange
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.core.utils.trading_pair_fetcher import TradingPairFetcher
from hummingbot.client.config.security import Security


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
    
    config_update_interval: int = Field(default=60)
    
    @validator('min_profitability', 'order_amount', pre=True, allow_reuse=True)
    def validate_decimal(cls, v):
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
    
    def __init__(self, config: ArbitrageControllerConfig, market_data_provider, actions_queue, update_interval=1.0):
        super().__init__(config, market_data_provider, actions_queue, update_interval)
        self.common_trading_pairs = []
        self.processed_data = {"opportunities": []}
        self.pair_last_trade_timestamps = {}
        self.executors_info = []
    
    async def control_loop(self):
        """
        Main control loop for the arbitrage controller.
        """
        try:
            # Check if market data provider is ready
            if not self.market_data_provider.ready:
                self.logger().info("Market data provider not ready. Waiting...")
                await asyncio.sleep(5.0)
                return
                
            # Check if both connectors exist and are ready
            connector_issues = []
            
            # First check if connectors exist in the market data provider
            if self.config.connector1 not in self.market_data_provider.connectors:
                connector_issues.append(f"Connector '{self.config.connector1}' not found in available connectors")
            
            if self.config.connector2 not in self.market_data_provider.connectors:
                connector_issues.append(f"Connector '{self.config.connector2}' not found in available connectors")
            
            # If any connector is missing, log detailed error and retry later
            if connector_issues:
                self.logger().error(f"Connector initialization issues: {'; '.join(connector_issues)}")
                self.logger().info(f"Available connectors: {list(self.market_data_provider.connectors.keys())}")
                # Use a counter to reduce log spam but still show periodic updates
                if not hasattr(self, '_connector_retry_count'):
                    self._connector_retry_count = 0
                self._connector_retry_count += 1
                
                # Log only every 5 attempts to reduce spam
                if self._connector_retry_count % 5 == 1:
                    self.logger().info(f"Will retry connector initialization (attempt {self._connector_retry_count})")
                await asyncio.sleep(10.0)  # Shorter sleep for faster recovery
                return
            
            # Reset retry counter when connectors are available
            if hasattr(self, '_connector_retry_count'):
                self._connector_retry_count = 0
                
            # Check if connectors are ready for trading
            connector1 = self.market_data_provider.connectors[self.config.connector1]
            connector2 = self.market_data_provider.connectors[self.config.connector2]
            
            if not connector1.ready:
                self.logger().info(f"Connector '{self.config.connector1}' not ready yet. Waiting...")
                await asyncio.sleep(5.0)
                return
                
            if not connector2.ready:
                self.logger().info(f"Connector '{self.config.connector2}' not ready yet. Waiting...")
                await asyncio.sleep(5.0)
                return
                
            # Try to find common trading pairs
            if not self.common_trading_pairs:
                try:
                    self.common_trading_pairs = self.find_common_trading_pairs()
                    if not self.common_trading_pairs:
                        self.logger().warning("No common trading pairs found between exchanges.")
                        # Check if specific trading pair was configured
                        if self.config.trading_pair:
                            self.logger().error(f"Configured trading pair '{self.config.trading_pair}' not available on both exchanges.")
                            # Check if the pair exists on either exchange
                            pair_on_ex1 = self.config.trading_pair in self.market_data_provider.get_trading_pairs(self.config.connector1)
                            pair_on_ex2 = self.config.trading_pair in self.market_data_provider.get_trading_pairs(self.config.connector2)
                            
                            if not pair_on_ex1 and not pair_on_ex2:
                                self.logger().error(f"Trading pair '{self.config.trading_pair}' not found on either exchange.")
                            elif not pair_on_ex1:
                                self.logger().error(f"Trading pair '{self.config.trading_pair}' not found on {self.config.connector1}.")
                            elif not pair_on_ex2:
                                self.logger().error(f"Trading pair '{self.config.trading_pair}' not found on {self.config.connector2}.")
                        
                        # Retry after delay but not too frequently
                        if not hasattr(self, '_pairs_retry_count'):
                            self._pairs_retry_count = 0
                        self._pairs_retry_count += 1
                        
                        # Gradually increase retry interval to avoid hammering the API
                        retry_delay = min(30.0, 5.0 + self._pairs_retry_count)
                        self.logger().info(f"Will retry finding common pairs in {retry_delay:.1f} seconds...")
                        await asyncio.sleep(retry_delay)
                        return
                    else:
                        self.logger().info(f"Found {len(self.common_trading_pairs)} common trading pairs.")
                        # Reset retry counter on success
                        if hasattr(self, '_pairs_retry_count'):
                            self._pairs_retry_count = 0
                except Exception as e:
                    self.logger().error(f"Error finding common pairs: {str(e)}", exc_info=True)
                    await asyncio.sleep(10.0)
                    return
            
            # Rest of controller logic for checking arbitrage opportunities
            # This part would continue with your existing implementation
            
        except Exception as e:
            self.logger().error(f"Unexpected error in control loop: {str(e)}", exc_info=True)
            await asyncio.sleep(5.0)
    
    def find_common_trading_pairs(self) -> List[str]:
        """
        Finds trading pairs that are common to both exchanges.
        """
        connector1_pairs = self.market_data_provider.get_trading_pairs(self.config.connector1)
        connector2_pairs = self.market_data_provider.get_trading_pairs(self.config.connector2)
        
        # Find common pairs
        common_pairs = list(set(connector1_pairs).intersection(set(connector2_pairs)))
        self.logger().info(f"Found {len(common_pairs)} common trading pairs between {self.config.connector1} and {self.config.connector2}")
        return common_pairs
    
    def check_arbitrage_opportunity(self, trading_pair: str) -> Optional[Dict]:
        """
        Checks for an arbitrage opportunity between the two exchanges for a specific trading pair.
        """
        # Get current bid/ask prices from both exchanges
        ex1_ticker = self.market_data_provider.get_ticker(self.config.connector1, trading_pair)
        ex2_ticker = self.market_data_provider.get_ticker(self.config.connector2, trading_pair)
        
        if not ex1_ticker or not ex2_ticker:
            return None
        
        # Calculate price differences and profitability
        profit_pct_1 = (ex2_ticker.bid - ex1_ticker.ask) / ex1_ticker.ask * Decimal("100")
        profit_pct_2 = (ex1_ticker.bid - ex2_ticker.ask) / ex2_ticker.ask * Decimal("100")
        
        # Detect profitable opportunities
        if profit_pct_1 > profit_pct_2 and profit_pct_1 >= self.config.min_profitability:
            return {
                "pair": trading_pair,
                "buy_exchange": self.config.connector1,
                "sell_exchange": self.config.connector2,
                "buy_price": float(ex1_ticker.ask),
                "sell_price": float(ex2_ticker.bid),
                "profit_pct": float(profit_pct_1),
                "timestamp": self.current_timestamp
            }
        elif profit_pct_2 >= self.config.min_profitability:
            return {
                "pair": trading_pair,
                "buy_exchange": self.config.connector2,
                "sell_exchange": self.config.connector1,
                "buy_price": float(ex2_ticker.ask),
                "sell_price": float(ex1_ticker.bid),
                "profit_pct": float(profit_pct_2),
                "timestamp": self.current_timestamp
            }
        
        return None
    
    async def create_arbitrage_executor(self, trading_pair: str, buy_exchange: str, sell_exchange: str, profit_pct: float):
        """
        Creates an arbitrage executor for a specific opportunity.
        """
        executor_config = ArbitrageExecutorConfig(
            type="arbitrage_executor",
            buying_market=ConnectorPair(connector_name=buy_exchange, trading_pair=trading_pair),
            selling_market=ConnectorPair(connector_name=sell_exchange, trading_pair=trading_pair),
            order_amount=self.config.order_amount,
            min_profitability=self.config.min_profitability
        )
        
        executor_action = CreateExecutorAction(executor_config=executor_config)
        await self.actions_queue.put(executor_action)
        
        self.logger().info(f"Created arbitrage executor for {trading_pair}: Buy on {buy_exchange}, Sell on {sell_exchange}, Profit: {profit_pct:.2f}%")
    
    def format_status(self) -> str:
        """
        Format status output for the strategy.
        """
        if not self.processed_data:
            return "No data available yet."
        
        lines = []
        lines.append(f"\n  Arbitrage Controller: {self.config.controller_name}")
        lines.append(f"  Monitoring {len(self.common_trading_pairs)} pairs between {self.config.connector1} and {self.config.connector2}")
        
        # Format opportunities
        if self.processed_data.get("opportunities"):
            df = pd.DataFrame(self.processed_data["opportunities"])
            df = df.sort_values("profit_pct", ascending=False)
            lines.append("\n  Current Arbitrage Opportunities:")
            lines.append(format_df_for_printout(df, table_format="pretty"))
        else:
            lines.append("\n  No arbitrage opportunities found.")
        
        # Format active executors
        if self.executors_info:
            lines.append(f"\n  Active Positions: {len(self.executors_info)}/{self.config.max_concurrent_positions}")
            for executor_info in self.executors_info:
                lines.append(f"    - {executor_info.executor_id}: {executor_info.config.buying_market.trading_pair}")
        else:
            lines.append("\n  No active positions.")
        
        return "\n".join(lines)

    def log_price_comparison(self, trading_pair: str, ex1_ticker, ex2_ticker):
        """Log real-time price comparison between exchanges"""
        if not ex1_ticker or not ex2_ticker:
            return
        
        # Always log focused pairs, or all pairs if no focus set
        focused_pairs = getattr(self.config, 'focused_pairs', [])
        if focused_pairs and trading_pair not in focused_pairs:
            return
        
        # Calculate spread
        spread_1 = (ex1_ticker.bid - ex1_ticker.ask) / ex1_ticker.ask * Decimal("100") 
        spread_2 = (ex2_ticker.bid - ex2_ticker.ask) / ex2_ticker.ask * Decimal("100")
        
        # Calculate cross-exchange price differences
        diff_pct_1 = (ex2_ticker.bid - ex1_ticker.ask) / ex1_ticker.ask * Decimal("100")
        diff_pct_2 = (ex1_ticker.bid - ex2_ticker.ask) / ex2_ticker.ask * Decimal("100")
        
        # Format log message
        log_msg = f"PRICE: {trading_pair} | " \
                  f"{self.config.connector1}: ask={ex1_ticker.ask:.8f} bid={ex1_ticker.bid:.8f} spread={spread_1:.4f}% | " \
                  f"{self.config.connector2}: ask={ex2_ticker.ask:.8f} bid={ex2_ticker.bid:.8f} spread={spread_2:.4f}% | " \
                  f"Opportunity 1→2: {diff_pct_1:.4f}% | 2→1: {diff_pct_2:.4f}%"
        
        # Log to both logger and print to stdout
        self.logger().info(log_msg)
        print(log_msg)


class ArbitrageStrategyConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    markets: Dict[str, Set[str]] = Field(default_factory=dict)
    candles_config: List[CandlesConfig] = Field(default_factory=list)
    
    # First connector (exchange)
    connector1: str = Field(
        default="binance",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the first connector name (e.g., binance_paper_trade): ",
            prompt_on_new=True))
    
    # Second connector (exchange)
    connector2: str = Field(
        default="bybit",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the second connector name (e.g., bybit_paper_trade): ",
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

    config_update_interval: int = Field(default=60)

    @validator('min_profitability', 'order_amount', pre=True, allow_reuse=True)
    def validate_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return v

    def load_controller_configs(self) -> List[ControllerConfigBase]:
        controller_config = ArbitrageControllerConfig(
            controller_name="arbitrage_controller",
            controller_type="arbitrage",
            connector1=self.connector1,
            connector2=self.connector2,
            min_profitability=self.min_profitability,
            order_amount=self.order_amount,
            cooldown_time=self.cooldown_time,
            max_concurrent_positions=self.max_concurrent_positions,
            trading_pair=self.trading_pair,
            config_update_interval=self.config_update_interval
        )
        return [controller_config]


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
        
        # If config is None, create a default one
        if config is None:
            config = ArbitrageStrategyConfig()
            self.init_markets(config)
        
        # Make sure markets are initialized
        if not self.markets and config is not None:
            self.init_markets(config)
        
        # Store BEFORE super().__init__ so it's not overwritten
        self.strategy_config = config
        
        # Important: Call super() without the controller creation
        super().__init__(connectors=connectors, config=config)
    
    def initialize_controllers(self):
        """Override to properly initialize our controller"""
        try:
            controller_config = ArbitrageControllerConfig(
                controller_name="arbitrage_controller",
                controller_type="arbitrage",
                connector1=self.strategy_config.connector1,
                connector2=self.strategy_config.connector2,
                min_profitability=self.strategy_config.min_profitability,
                order_amount=self.strategy_config.order_amount,
                cooldown_time=self.strategy_config.cooldown_time,
                max_concurrent_positions=self.strategy_config.max_concurrent_positions,
                trading_pair=self.strategy_config.trading_pair,
                config_update_interval=self.strategy_config.config_update_interval
            )
            
            controller = controller_config.get_controller_class()(
                config=controller_config,
                market_data_provider=self.market_data_provider,
                actions_queue=self.actions_queue
            )
            controller.start()
            self.controllers[controller_config.controller_name] = controller
            self.logger().info(f"Controller {controller_config.controller_name} initialized successfully")
        except Exception as e:
            self.logger().error(f"Failed to initialize controller: {str(e)}", exc_info=True)
            self.logger().warning("Strategy will run without controller functionality")
    
    def create_actions_proposal(self) -> List[ExecutorAction]:
        """
        Create actions proposal for the strategy.
        This method is required for StrategyV2Base and is called by determine_executor_actions.
        """
        # For arbitrage strategies, typically no actions are created directly
        # Instead, controllers trigger actions when they detect opportunities
        return []
    
    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Create a list of actions to stop executors.
        This method is required for StrategyV2Base and is called by determine_executor_actions.
        """
        # For our arbitrage strategy, we let the controllers handle stopping executors
        # based on their own logic, so we don't add any stop actions here
        return []
    
    def format_status(self) -> str:
        """Format status of the strategy for display."""
        if not hasattr(self, 'strategy_config'):
            return "Strategy not properly initialized"
        
        # Check for connector readiness differently - avoid false warnings
        for exchange_name in [self.strategy_config.connector1, self.strategy_config.connector2]:
            if exchange_name not in self.connectors:
                return f"{exchange_name} is not available. Please connect first."
        
        # Rest of method stays the same but use self.strategy_config instead of self._strategy_config
        lines = []
        lines.append("Arbitrage Strategy")
        lines.append(f"Trading pair: {self.strategy_config.trading_pair or 'Auto-detecting common pairs'}")
        lines.append(f"Exchange 1: {self.strategy_config.connector1}")
        lines.append(f"Exchange 2: {self.strategy_config.connector2}")
        lines.append(f"Min profitability: {self.strategy_config.min_profitability}%")
        lines.append(f"Order amount: {self.strategy_config.order_amount}")
        
        # Add active executors info
        active_executors = len(self.executor_orchestrator.active_executors)
        lines.append(f"\nActive arbitrage executions: {active_executors}")
        
        # Controller status
        for controller_name, controller in self.controllers.items():
            if hasattr(controller, "processed_data") and controller.processed_data:
                lines.append(f"\n{controller_name.upper()} STATUS:")
                
                # Opportunities
                if "opportunities" in controller.processed_data and controller.processed_data["opportunities"]:
                    opps = controller.processed_data["opportunities"]
                    lines.append("\nCurrent Opportunities:")
                    
                    # Convert to DataFrame for display
                    df = pd.DataFrame(opps)
                    if not df.empty:
                        lines.append(format_df_for_printout(df, True))
                else:
                    lines.append("\nNo arbitrage opportunities detected")
        
        return "\n".join(lines)

    def _create_controller(self, config: ControllerConfigBase):
        """Create a controller instance."""
        return config.get_controller_class()(
            config=config,
            market_data_provider=self.market_data_provider,
            actions_queue=self.actions_queue
        )

    def update_controllers_configs(self):
        """Use our stored strategy_config instead of self.config"""
        if not hasattr(self, '_last_config_update_ts'):
            self._last_config_update_ts = 0
        
        if hasattr(self, 'strategy_config') and self._last_config_update_ts + self.strategy_config.config_update_interval < self.current_timestamp:
            self._last_config_update_ts = self.current_timestamp
            
            # Create controller config manually since we know the structure
            controller_config = ArbitrageControllerConfig(
                controller_name="arbitrage_controller",
                controller_type="arbitrage",
                connector1=self.strategy_config.connector1,
                connector2=self.strategy_config.connector2,
                min_profitability=self.strategy_config.min_profitability,
                order_amount=self.strategy_config.order_amount,
                cooldown_time=self.strategy_config.cooldown_time,
                max_concurrent_positions=self.strategy_config.max_concurrent_positions,
                trading_pair=self.strategy_config.trading_pair,
                config_update_interval=self.strategy_config.config_update_interval
            )
            
            # Update existing controller
            if "arbitrage_controller" in self.controllers:
                self.controllers["arbitrage_controller"].update_config(controller_config)


def start(config_file_name=None):
    """
    Main entry point for the strategy.
    """
    import asyncio
    import time
    
    # Initialize logging first
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(settings.LOG_FILE_PATH, "hummingbot_logs.yml")),
            logging.StreamHandler()
        ]
    )
    
    # Get the main Hummingbot application
    hb = HummingbotApplication.main_application()
    
    # Load config from file or create default config
    if config_file_name is not None:
        config_path = os.path.join(settings.STRATEGY_CONF_DIR, config_file_name)
        if not os.path.exists(config_path):
            config_path = os.path.join(settings.CONF_DIR, "scripts", config_file_name)
        
        with open(config_path) as file:
            config_dict = yaml.safe_load(file)
        config = ArbitrageStrategyConfig.parse_obj(config_dict)
    else:
        config = ArbitrageStrategyConfig()
    
    # Patch the Security.decrypt_connector_config method to avoid errors
    original_decrypt_connector_config = Security.decrypt_connector_config
    
    def safe_decrypt_connector_config(cls, file_path):
        try:
            original_decrypt_connector_config(file_path)
        except Exception as e:
            logging.getLogger("hummingbot.client.config.security").warning(
                f"Failed to decrypt connector config for {file_path}: {str(e)}")
    
    # Apply the patch
    Security.decrypt_connector_config = classmethod(safe_decrypt_connector_config)
    
    # Get only connectors that are already connected and initialized
    connectors = {}
    ready_connectors = set()
    max_wait_time = 30  # seconds
    start_time = time.time()
    
    # First check if connectors are available
    for connector_name in [config.connector1, config.connector2]:
        if connector_name not in hb.markets:
            logging.getLogger(__name__).error(
                f"Connector {connector_name} not found. Make sure you've added it with 'connect {connector_name}'")
            return
    
    # Wait for connectors to be ready
    print(f"Waiting for connectors to be ready (max {max_wait_time} seconds)...")
    while time.time() - start_time < max_wait_time:
        pending_connectors = []
        
        for connector_name in [config.connector1, config.connector2]:
            if connector_name in ready_connectors:
                continue
                
            if hb.markets[connector_name].ready:
                connectors[connector_name] = hb.markets[connector_name]
                ready_connectors.add(connector_name)
                print(f"✓ Connector {connector_name} is ready")
            else:
                pending_connectors.append(connector_name)
        
        if not pending_connectors:
            # All connectors are ready
            break
            
        # Print status but don't spam the logs
        if len(pending_connectors) > 0:
            print(f"Waiting for: {', '.join(pending_connectors)}...")
            
        time.sleep(1)
    
    # Check if all connectors are ready
    if len(ready_connectors) < 2:
        not_ready = set([config.connector1, config.connector2]) - ready_connectors
        print(f"Timed out waiting for connectors: {', '.join(not_ready)}")
        print("Please ensure the exchanges are properly configured and try again.")
        return
    
    # Initialize markets for the strategy
    ArbitrageStrategy.init_markets(config)
    
    # Create the strategy with only successfully connected connectors
    strategy = ArbitrageStrategy(connectors=connectors, config=config)
    
    # Restore original method
    Security.decrypt_connector_config = original_decrypt_connector_config
    
    return strategy