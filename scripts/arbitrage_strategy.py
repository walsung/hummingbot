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
import binascii
import time
from tabulate import tabulate

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


class RateLimiter:
    """Improved rate limiter with better throttling and debugging"""
    
    def __init__(self, max_calls_per_second=10, buffer_factor=0.7):
        self.max_calls = max_calls_per_second
        # Use buffer factor to stay well under the limit (70% of max by default)
        self.effective_max_calls = int(max_calls_per_second * buffer_factor)
        self.call_timestamps = []
        self.lock = asyncio.Lock()
        self.last_log_time = 0
        self.call_count = 0
        
    async def acquire(self):
        """Wait until a call can be made without exceeding the rate limit"""
        async with self.lock:
            now = time.time()
            
            # Remove timestamps older than 1 second
            self.call_timestamps = [ts for ts in self.call_timestamps if now - ts < 1.0]
            current_calls = len(self.call_timestamps)
            
            # If we've reached the effective limit, wait until we can make another call
            if current_calls >= self.effective_max_calls:
                # Calculate wait time based on oldest timestamp
                if self.call_timestamps:
                    oldest_timestamp = min(self.call_timestamps)
                    wait_time = 1.0 - (now - oldest_timestamp)
                    if wait_time > 0:
                        # Log rate limiting but not too frequently
                        if now - self.last_log_time > 5.0:
                            logging.info(f"Rate limiting: waiting {wait_time:.3f}s ({current_calls}/{self.max_calls} calls in last second)")
                            self.last_log_time = now
                        
                        # Wait until we can make another call
                        await asyncio.sleep(wait_time)
                        
                        # Update current time and clean timestamps again
                        now = time.time()
                        self.call_timestamps = [ts for ts in self.call_timestamps if now - ts < 1.0]
            
            # Add current timestamp and proceed
            self.call_timestamps.append(now)
            self.call_count += 1


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
    
    # Add trading_pairs field to the controller config
    trading_pairs: List[str] = Field(default_factory=list)
    
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
        
        # Add single trading pair if specified
        if self.trading_pair:
            markets[self.connector1].add(self.trading_pair)
            markets[self.connector2].add(self.trading_pair)
        
        # Add multiple trading pairs if specified
        if hasattr(self, 'trading_pairs') and self.trading_pairs:
            for pair in self.trading_pairs:
                markets[self.connector1].add(pair)
                markets[self.connector2].add(pair)
        
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
        self.unavailable_pairs = {}  # Track unavailable pairs by exchange
        # Add rate limiters for each exchange
        self.rate_limiters = {
            self.config.connector1: RateLimiter(max_calls_per_second=10),
            self.config.connector2: RateLimiter(max_calls_per_second=10)
        }
    
    async def get_ticker_with_rate_limit(self, connector_name, trading_pair):
        """Get ticker data with rate limiting"""
        try:
            await self.rate_limiters[connector_name].acquire()
            return self.market_data_provider.get_ticker(connector_name, trading_pair)
        except KeyError as e:
            # Handle case where trading pair doesn't exist on this exchange
            pair_str = str(e).strip("'")
            if connector_name not in self.unavailable_pairs:
                self.unavailable_pairs[connector_name] = set()
            
            if pair_str not in self.unavailable_pairs[connector_name]:
                self.unavailable_pairs[connector_name].add(pair_str)
                self.logger().error(f"Trading pair {pair_str} not available on {connector_name}")
                # Also print to stdout for immediate visibility
                print(f"\n⚠️ ERROR: Trading pair {pair_str} not available on {connector_name}")
            
            return None
    
    async def control_loop(self):
        """Main control loop for the arbitrage controller."""
        try:
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
                
            # Process trading pairs and show real-time price comparisons
            if not self.common_trading_pairs:
                try:
                    self.common_trading_pairs = await self.find_common_trading_pairs()
                    if not self.common_trading_pairs:
                        self.logger().warning("No common trading pairs found between exchanges.")
                        await asyncio.sleep(30.0)
                        return
                    else:
                        self.logger().info(f"Found {len(self.common_trading_pairs)} common trading pairs")
                        # Print the pairs we found
                        self.logger().info(f"Common pairs: {self.common_trading_pairs[:20]}")
                        print(f"Found {len(self.common_trading_pairs)} common trading pairs: {self.common_trading_pairs[:10]}")
                except Exception as e:
                    self.logger().error(f"Error finding common pairs: {str(e)}", exc_info=True)
                    await asyncio.sleep(30.0)
                    return
            
            opportunities = []
            price_data = []
            
            # Process each trading pair (limiting to 10 for display purposes)
            display_pairs = self.common_trading_pairs[:10] if len(self.common_trading_pairs) > 10 else self.common_trading_pairs
            
            # Show a specific trading pair if configured
            if self.config.trading_pair:
                display_pairs = [self.config.trading_pair]
            
            # Force print price comparisons at least once per minute
            current_time = time.time()
            if not hasattr(self, '_last_forced_print_time') or current_time - self._last_forced_print_time > 60:
                self._last_forced_print_time = current_time
                self._last_price_print_time = 0  # Reset to force print
                print(f"\nFetching latest prices for {len(display_pairs)} trading pairs...")
            
            for trading_pair in display_pairs:
                try:
                    # Get ticker data for both exchanges with rate limiting
                    ex1_ticker = await self.get_ticker_with_rate_limit(self.config.connector1, trading_pair)
                    ex2_ticker = await self.get_ticker_with_rate_limit(self.config.connector2, trading_pair)
                    
                    # Log prices regardless of opportunity
                    if ex1_ticker and ex2_ticker:
                        self.log_price_comparison(trading_pair, ex1_ticker, ex2_ticker)
                        
                        # Check for arbitrage opportunity
                        opportunity = self.check_arbitrage_opportunity(trading_pair)
                        if opportunity:
                            opportunities.append(opportunity)
                            
                    # Add the price data for later processing 
                    price_data.append({
                        "pair": trading_pair,
                        "ex1_ask": float(ex1_ticker.ask) if ex1_ticker else None,
                        "ex1_bid": float(ex1_ticker.bid) if ex1_ticker else None,
                        "ex2_ask": float(ex2_ticker.ask) if ex2_ticker else None,
                        "ex2_bid": float(ex2_ticker.bid) if ex2_ticker else None,
                        "time": self.current_timestamp
                    })
                except Exception as e:
                    self.logger().error(f"Error processing pair {trading_pair}: {e}")
            
            # Update opportunities
            if opportunities:
                # Sort by profitability (descending)
                opportunities.sort(key=lambda x: x["profit_pct"], reverse=True)
                self.processed_data["opportunities"] = opportunities
                
            # Store latest price data
            self.processed_data["price_data"] = price_data
            
            # Force print price comparisons if we have data but haven't printed recently
            if price_data and (not hasattr(self, '_last_price_print_time') or current_time - self._last_price_print_time > 10):
                self.print_price_comparisons()
            
        except Exception as e:
            self.logger().error(f"Error in control loop: {str(e)}", exc_info=True)
    
    async def find_common_trading_pairs(self) -> List[str]:
        """
        Finds trading pairs that are common to both exchanges, filtering out unavailable pairs.
        """
        try:
            # Get actual trading pairs from each exchange with rate limiting
            await self.rate_limiters[self.config.connector1].acquire()
            connector1_pairs = set(self.market_data_provider.get_trading_pairs(self.config.connector1))
            
            await self.rate_limiters[self.config.connector2].acquire()
            connector2_pairs = set(self.market_data_provider.get_trading_pairs(self.config.connector2))
            
            # Print available pairs for debugging
            self.logger().info(f"Available pairs on {self.config.connector1}: {list(connector1_pairs)[:10]}...")
            self.logger().info(f"Available pairs on {self.config.connector2}: {list(connector2_pairs)[:10]}...")
            
            # If trading_pairs is specified in config, validate each pair
            if hasattr(self.config, 'trading_pairs') and self.config.trading_pairs:
                valid_pairs = []
                for pair in self.config.trading_pairs:
                    # Check if pair exists on first exchange
                    if pair not in connector1_pairs:
                        # Try alternative quote currencies (USDT instead of USDC, etc.)
                        base, quote = pair.split('-')
                        alternatives = []
                        if quote == "USDC":
                            alternatives = [f"{base}-USDT", f"{base}-BUSD"]
                        
                        found_alt = False
                        for alt_pair in alternatives:
                            if alt_pair in connector1_pairs:
                                self.logger().warning(f"Trading pair {pair} not found on {self.config.connector1}, using {alt_pair} instead")
                                print(f"\n⚠️ WARNING: Trading pair {pair} not found on {self.config.connector1}, using {alt_pair} instead")
                                pair = alt_pair
                                found_alt = True
                                break
                        
                        if not found_alt:
                            self.logger().error(f"Trading pair {pair} not available on {self.config.connector1}")
                            print(f"\n⚠️ ERROR: Trading pair {pair} not available on {self.config.connector1}")
                            continue
                    
                    # Now check second exchange with possibly updated pair
                    if pair not in connector2_pairs:
                        self.logger().error(f"Trading pair {pair} not available on {self.config.connector2}")
                        print(f"\n⚠️ ERROR: Trading pair {pair} not available on {self.config.connector2}")
                        continue
                    
                    valid_pairs.append(pair)
                
                return valid_pairs
            
            # Find common pairs
            common_pairs = list(connector1_pairs.intersection(connector2_pairs))
            return common_pairs
        except Exception as e:
            self.logger().error(f"Error finding common trading pairs: {str(e)}", exc_info=True)
            return []
    
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
        """Log price comparison between exchanges"""
        if not ex1_ticker or not ex2_ticker:
            return
            
        # Calculate price differences
        bid_diff = float(ex1_ticker.bid) - float(ex2_ticker.bid)
        ask_diff = float(ex1_ticker.ask) - float(ex2_ticker.ask)
        bid_diff_pct = (bid_diff / float(ex2_ticker.bid)) * 100 if float(ex2_ticker.bid) != 0 else 0
        ask_diff_pct = (ask_diff / float(ex2_ticker.ask)) * 100 if float(ex2_ticker.ask) != 0 else 0
        
        # Format the price data for display
        price_data = {
            "Pair": trading_pair,
            f"{self.config.connector1} Bid": f"{float(ex1_ticker.bid):.8f}",
            f"{self.config.connector1} Ask": f"{float(ex1_ticker.ask):.8f}",
            f"{self.config.connector2} Bid": f"{float(ex2_ticker.bid):.8f}",
            f"{self.config.connector2} Ask": f"{float(ex2_ticker.ask):.8f}",
            "Bid Diff %": f"{bid_diff_pct:.2f}%",
            "Ask Diff %": f"{ask_diff_pct:.2f}%"
        }
        
        # Log to file
        self.logger().info(f"Price comparison for {trading_pair}: "
                          f"{self.config.connector1} Bid/Ask: {float(ex1_ticker.bid):.8f}/{float(ex1_ticker.ask):.8f}, "
                          f"{self.config.connector2} Bid/Ask: {float(ex2_ticker.bid):.8f}/{float(ex2_ticker.ask):.8f}, "
                          f"Diff: Bid {bid_diff_pct:.2f}%, Ask {ask_diff_pct:.2f}%")
        
        # Store in processed data for display
        if "price_comparisons" not in self.processed_data:
            self.processed_data["price_comparisons"] = []
            
        # Update or add price comparison
        found = False
        for i, comp in enumerate(self.processed_data["price_comparisons"]):
            if comp["Pair"] == trading_pair:
                self.processed_data["price_comparisons"][i] = price_data
                found = True
                break
                
        if not found:
            self.processed_data["price_comparisons"].append(price_data)
            
        # Print to stdout every 10 seconds to avoid flooding
        current_time = time.time()
        if not hasattr(self, '_last_price_print_time') or current_time - self._last_price_print_time > 10:
            self._last_price_print_time = current_time
            self.print_price_comparisons()
    
    def print_price_comparisons(self):
        """Print price comparisons to stdout"""
        if "price_comparisons" not in self.processed_data or not self.processed_data["price_comparisons"]:
            return
            
        # Format as table
        table_data = []
        headers = ["Pair", f"{self.config.connector1} Bid/Ask", f"{self.config.connector2} Bid/Ask", "Diff %"]
        
        for comp in self.processed_data["price_comparisons"]:
            table_data.append([
                comp["Pair"],
                f"{comp[f'{self.config.connector1} Bid']}/{comp[f'{self.config.connector1} Ask']}",
                f"{comp[f'{self.config.connector2} Bid']}/{comp[f'{self.config.connector2} Ask']}",
                f"Bid: {comp['Bid Diff %']}, Ask: {comp['Ask Diff %']}"
            ])
        
        # Print table to stdout
        table = tabulate(table_data, headers=headers, tablefmt="grid")
        print("\n=== REAL-TIME PRICE COMPARISON ===")
        print(table)
        print("=================================\n")

    async def start(self):
        """
        Starts the controller and initializes trading pairs.
        """
        await super().start()
        
        # Initialize trading pairs
        if hasattr(self.config, 'trading_pairs') and self.config.trading_pairs:
            self.common_trading_pairs = self.config.trading_pairs
        elif self.config.trading_pair:
            self.common_trading_pairs = [self.config.trading_pair]
        else:
            self.common_trading_pairs = await self.find_common_trading_pairs()
        
        self.pair_last_trade_timestamps = {pair: 0 for pair in self.common_trading_pairs}
        self.pair_processed_data = {pair: {} for pair in self.common_trading_pairs}


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

    trading_pairs: List[str] = Field(default_factory=list)

    @validator('min_profitability', 'order_amount', pre=True, allow_reuse=True)
    def validate_decimal(cls, v):
        if isinstance(v, str):
            return Decimal(v)
        return v

    @validator('trading_pair', allow_reuse=True)
    def parse_trading_pair(cls, v, values):
        if v and ',' in v:
            # This is a comma-separated list - store it in trading_pairs
            values['trading_pairs'] = [pair.strip() for pair in v.split(',')]
            return None
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
        
        # Pass trading_pairs to the controller if available
        if hasattr(self, 'trading_pairs') and self.trading_pairs:
            setattr(controller_config, 'trading_pairs', self.trading_pairs)
        
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
        
        # Add single trading pair if specified
        if config.trading_pair:
            cls.markets[config.connector1].add(config.trading_pair)
            cls.markets[config.connector2].add(config.trading_pair)
        
        # Add multiple trading pairs if specified
        if hasattr(config, 'trading_pairs') and config.trading_pairs:
            for pair in config.trading_pairs:
                cls.markets[config.connector1].add(pair)
                cls.markets[config.connector2].add(pair)
            
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
                
                # Price comparisons
                if "price_comparisons" in controller.processed_data and controller.processed_data["price_comparisons"]:
                    lines.append("\nPrice Comparisons:")
                    price_data = controller.processed_data["price_comparisons"]
                    price_df = pd.DataFrame(price_data)
                    if not price_df.empty:
                        lines.append(format_df_for_printout(price_df, True))
                
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
                
                # Unavailable pairs
                if hasattr(controller, "unavailable_pairs") and controller.unavailable_pairs:
                    lines.append("\nUnavailable Trading Pairs:")
                    for exchange, pairs in controller.unavailable_pairs.items():
                        if pairs:
                            lines.append(f"  {exchange}: {', '.join(sorted(pairs))}")
        
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
    import binascii
    
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
    
    # Patch security module to handle non-hex values
    original_decrypt_secret_value = Security.secrets_manager.decrypt_secret_value
    original_decrypt_connector_config = Security.decrypt_connector_config

    # Create a monkey patch for the decrypt_secret_value method
    def safe_decrypt_secret_value(attr, value):
        try:
            if value is None:
                return None
            
            # Skip decryption for non-string values
            if not isinstance(value, str):
                return value
            
            # Skip decryption for empty strings
            if not value.strip():
                return value
            
            # Check if value is a valid hex string (must be even length and only hex chars)
            if len(value) % 2 != 0 or not all(c in '0123456789abcdefABCDEF' for c in value):
                return value
            
            # Try to decrypt
            return original_decrypt_secret_value(attr, value)
        except Exception as e:
            # If it fails for any reason, return as-is
            logging.getLogger(__name__).debug(f"Decryption error for {attr}: {str(e)}")
            return value

    # Create a monkey patch for the decrypt_connector_config method
    def safe_decrypt_connector_config(file_path):
        try:
            connector_name = connector_name_from_file(file_path)
            config_map = load_connector_config_map_from_file(file_path)
            Security._secure_configs[connector_name] = config_map
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to decrypt connector config for {file_path}: {str(e)}")

    # Apply the patches
    if hasattr(Security.secrets_manager, 'decrypt_secret_value'):
        Security.secrets_manager.decrypt_secret_value = safe_decrypt_secret_value

    # Replace the class method with our safe version
    Security.decrypt_connector_config = classmethod(lambda cls, file_path: safe_decrypt_connector_config(file_path))

    # Also patch the decrypt_all method to catch any errors
    original_decrypt_all = Security.decrypt_all

    def safe_decrypt_all():
        try:
            Security._secure_configs.clear()
            Security._decryption_done.clear()
            encrypted_files = list_connector_configs()
            for file in encrypted_files:
                try:
                    safe_decrypt_connector_config(file)
                except Exception as e:
                    logging.getLogger(__name__).warning(f"Error decrypting {file}: {str(e)}")
            Security._decryption_done.set()
        except Exception as e:
            logging.getLogger(__name__).error(f"Error in decrypt_all: {str(e)}")
            Security._decryption_done.set()  # Make sure we set this even if there's an error

    # Replace the class method
    Security.decrypt_all = classmethod(lambda cls: safe_decrypt_all())
    
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
    Security.secrets_manager.decrypt_secret_value = original_decrypt_secret_value
    Security.decrypt_connector_config = original_decrypt_connector_config
    Security.decrypt_all = original_decrypt_all
    
    return strategy