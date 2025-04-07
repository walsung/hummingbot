"""
Spot Arbitrage Strategy V2

This strategy monitors price differences between the same trading pair on two exchanges
and executes arbitrage trades when profitable opportunities arise.

# Spot Arbitrage Strategy Configuration

## New Balance Handling Options

- `arbitrage_mode`: Controls how the strategy handles arbitrage when balances are insufficient
  - `both`: Execute both buy and sell sides of arbitrage (default)
  - `buy_only`: Only execute the buy side of arbitrage opportunities
  - `sell_only`: Only execute the sell side of arbitrage opportunities

- `auto_adjust_order_size`: When set to true, automatically adjusts order sizes to meet exchange minimums
  - `true`: Auto-adjust order sizes to minimum requirements (default)
  - `false`: Skip opportunities where calculated order size is below minimum
"""

import logging
import time
import asyncio
import random
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Dict, List, Optional, Set, Union, Any
from pydantic import Field, validator
import pandas as pd

from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent, BuyOrderCompletedEvent, SellOrderCompletedEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future


class SpotArbitrageConfig(StrategyV2ConfigBase):
    """Configuration for the Spot Arbitrage strategy"""
    
    # Add these two fields to match the YAML template
    strategy: Optional[str] = Field(
        default="spot_arbitrage",
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    template_version: Optional[int] = Field(
        default=1,
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    # Override the inherited fields with empty defaults
    candles_config: List[CandlesConfig] = Field(
        default_factory=list,
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    markets: Dict[str, Set[str]] = Field(
        default_factory=dict,
        client_data=ClientFieldData(
            prompt=None,
            prompt_on_new=False,
        )
    )
    
    # First exchange configuration
    exchange_1: str = Field(
        default="binance",
        client_data=ClientFieldData(
            prompt="Enter the first exchange name",
            prompt_on_new=True,
        )
    )
    
    # Second exchange configuration
    exchange_2: str = Field(
        default="bybit",
        client_data=ClientFieldData(
            prompt="Enter the second exchange name",
            prompt_on_new=True,
        )
    )
    
    # Trading pairs
    trading_pairs: List[str] = Field(
        default=["ETH-USDT", "BTC-USDT"],
        client_data=ClientFieldData(
            prompt="Enter trading pairs (comma-separated)",
            prompt_on_new=True,
        )
    )
    
    # Minimum profitability threshold
    min_profitability: float = Field(
        default=0.5,
        client_data=ClientFieldData(
            prompt="Enter minimum profitability threshold (%)",
            prompt_on_new=True,
        )
    )
    
    # Order amount in USD
    order_amount_usd: float = Field(
        default=5.0,
        client_data=ClientFieldData(
            prompt="Enter order amount in USD",
            prompt_on_new=True,
        )
    )
    
    # Maximum order age in seconds
    max_order_age: int = Field(
        default=60,
        client_data=ClientFieldData(
            prompt="Enter maximum order age in seconds",
            prompt_on_new=True,
        )
    )
    
    # Check interval in seconds
    check_interval: int = Field(
        default=5,
        client_data=ClientFieldData(
            prompt="Enter check interval in seconds",
            prompt_on_new=True,
        )
    )
    
    # Minimum order amounts for trading pairs
    min_order_amounts: Dict[str, float] = Field(
        default={},
        client_data=ClientFieldData(
            prompt=None,  # Too complex for direct prompting
            prompt_on_new=False,
        )
    )
    
    # New options for better balance handling
    arbitrage_mode: str = "both"  # Options: "both", "buy_only", "sell_only"
    auto_adjust_order_size: bool = True  # Auto-adjust to minimum order size if needed
    
    @validator("trading_pairs", pre=True, allow_reuse=True)
    def validate_trading_pairs(cls, v):
        """Validate and format trading pairs"""
        if isinstance(v, str):
            return [pair.strip() for pair in v.split(",")]
        return v


class SpotArbitrage(StrategyV2Base):
    """
    This strategy monitors price differences between the same trading pair on two exchanges
    and executes arbitrage trades when profitable opportunities arise.
    """
    _logger = None
    markets: ClassVar[Dict[str, Set[str]]] = {}
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger
    
    @classmethod
    def init_markets(cls, config: SpotArbitrageConfig):
        """Initialize the markets for the strategy"""
        cls.markets = {
            config.exchange_1: set(config.trading_pairs),
            config.exchange_2: set(config.trading_pairs)
        }
        return cls.markets
    
    def __init__(self, connectors: Dict[str, ConnectorBase], config: Optional[SpotArbitrageConfig] = None):
        if config is None:
            config = SpotArbitrageConfig()
        
        # Call super().__init__ first
        super().__init__(connectors, config)
        self.config = config
        
        # Initialize essential attributes
        self.exchange_1 = config.exchange_1
        self.exchange_2 = config.exchange_2
        self.trading_pairs = config.trading_pairs
        self.min_profitability = Decimal(str(config.min_profitability))
        self.order_amount_usd = Decimal(str(config.order_amount_usd))
        self.max_order_age = config.max_order_age
        self.check_interval = config.check_interval
        
        # Initialize strategy variables
        self.last_checked_ts = 0
        self.prices = {}
        self.active_orders = {}
        self.arbitrage_opportunities = {}
        
        # Check if connectors dictionary is provided
        if not connectors:
            self.logger().warning("No connectors dictionary provided. Strategy cannot start.")
            self.ready_to_trade = False
            return
        
        # Check that the specified exchanges are in the connectors
        for exchange in [self.exchange_1, self.exchange_2]:
            if exchange not in connectors:
                self.logger().error(f"Exchange {exchange} not in the list of connectors.")
                self.ready_to_trade = False
                return
        
        # Initialize markets and validate trading pairs
        self._initialize_markets()
        
        # Initialize min_amount dictionary with defaults and user config
        self.min_amount = self._initialize_min_amounts()
        
        # All checks passed, set ready to trade
        self.ready_to_trade = True
        self.logger().info("Strategy initialized successfully.")
    
    def _initialize_min_amounts(self) -> Dict[str, Decimal]:
        """Initialize minimum order amounts from config and defaults"""
        # Start with an empty dictionary
        min_amounts = {}
        
        # Add only the trading pairs from the config
        for trading_pair in self.trading_pairs:
            # Set reasonable defaults based on common pairs
            if trading_pair == "BTC-USDT":
                min_amounts[trading_pair] = Decimal("0.0001")
            elif trading_pair == "ETH-USDT":
                min_amounts[trading_pair] = Decimal("0.01")
            elif trading_pair == "SOL-USDT":
                min_amounts[trading_pair] = Decimal("0.1")
            elif trading_pair == "BNB-USDT":
                min_amounts[trading_pair] = Decimal("0.01")
            elif trading_pair == "ADA-USDT":
                min_amounts[trading_pair] = Decimal("10")
            elif trading_pair == "XRP-USDT":
                min_amounts[trading_pair] = Decimal("10")
            elif trading_pair == "DOGE-USDT":
                min_amounts[trading_pair] = Decimal("100")
            else:
                # Default minimum for unknown pairs
                min_amounts[trading_pair] = Decimal("0.01")
        
        # Override with user-configured values if provided
        if hasattr(self.config, 'min_order_amounts') and self.config.min_order_amounts:
            for pair, amount in self.config.min_order_amounts.items():
                if pair in self.trading_pairs:  # Only apply to pairs we're actually trading
                    min_amounts[pair] = Decimal(str(amount))
        
        return min_amounts
    
    def tick(self, timestamp: float):
        """
        Main loop for the strategy, called at the specified interval
        """
        if not self.ready_to_trade:
            return
        
        # Check if enough time has passed since the last check
        if timestamp - self.last_checked_ts < self.check_interval:
            return
        
        self.last_checked_ts = timestamp
        
        # Update prices
        self._update_prices()
        
        # Find arbitrage opportunities
        self._find_arbitrage_opportunities()
        
        # Execute arbitrage trades
        self._execute_arbitrage_trades()
        
        # Clean up old orders
        self._cleanup_old_orders(timestamp)
    
    def _update_prices(self):
        """Update prices for all trading pairs on both exchanges"""
        for exchange in [self.exchange_1, self.exchange_2]:
            for trading_pair in self.common_trading_pairs:  # Only use common pairs
                try:
                    connector = self.connectors[exchange]
                    mid_price = connector.get_mid_price(trading_pair)
                    
                    if exchange not in self.prices:
                        self.prices[exchange] = {}
                    
                    self.prices[exchange][trading_pair] = mid_price
                    
                    # Also store bid/ask prices for more detailed analysis
                    orderbook = connector.get_order_book(trading_pair)
                    best_bid = Decimal(str(orderbook.get_price(False)))
                    best_ask = Decimal(str(orderbook.get_price(True)))
                    
                    # Store bid/ask prices with unique keys
                    self.prices[f"{exchange}_bid_{trading_pair}"] = best_bid
                    self.prices[f"{exchange}_ask_{trading_pair}"] = best_ask
                    
                except Exception as e:
                    self.logger().debug(f"Error updating prices for {exchange} {trading_pair}: {e}")
                    # Using debug level instead of error for missing pairs
    
    def _find_arbitrage_opportunities(self):
        """Find arbitrage opportunities between the two exchanges"""
        self.arbitrage_opportunities = {}
        
        for trading_pair in self.trading_pairs:
            # Ensure we have prices for both exchanges
            if (trading_pair not in self.prices[self.exchange_1] or 
                trading_pair not in self.prices[self.exchange_2]):
                continue
            
            price_1 = self.prices[self.exchange_1][trading_pair]
            price_2 = self.prices[self.exchange_2][trading_pair]
            
            # Calculate the price difference as a percentage
            if price_1 > price_2:
                diff_pct = (price_1 / price_2 - Decimal("1")) * Decimal("100")
                if diff_pct > self.min_profitability:
                    self.arbitrage_opportunities[trading_pair] = {
                        "buy_exchange": self.exchange_2,
                        "sell_exchange": self.exchange_1,
                        "buy_price": price_2,
                        "sell_price": price_1,
                        "difference_pct": diff_pct
                    }
                    self.logger().info(
                        f"Arbitrage opportunity found: Buy {trading_pair} on {self.exchange_2} at {price_2}, "
                        f"Sell on {self.exchange_1} at {price_1}, Difference: {diff_pct:.2f}%"
                    )
            elif price_2 > price_1:
                diff_pct = (price_2 / price_1 - Decimal("1")) * Decimal("100")
                if diff_pct > self.min_profitability:
                    self.arbitrage_opportunities[trading_pair] = {
                        "buy_exchange": self.exchange_1,
                        "sell_exchange": self.exchange_2,
                        "buy_price": price_1,
                        "sell_price": price_2,
                        "difference_pct": diff_pct
                    }
                    self.logger().info(
                        f"Arbitrage opportunity found: Buy {trading_pair} on {self.exchange_1} at {price_1}, "
                        f"Sell on {self.exchange_2} at {price_2}, Difference: {diff_pct:.2f}%"
                    )
    
    def _check_sufficient_balance(self, trading_pair, opportunity):
        """Check if both exchanges have sufficient balance for the arbitrage trade"""
        buy_exchange = opportunity["buy_exchange"]
        sell_exchange = opportunity["sell_exchange"]
        
        # Split trading pair into base and quote assets
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        
        # Calculate required amounts
        order_amount_quote = self.order_amount_usd
        order_amount_base = order_amount_quote / opportunity["buy_price"]
        
        # Get exchange minimum order sizes
        buy_connector = self.connectors[buy_exchange]
        sell_connector = self.connectors[sell_exchange]
        
        # Check minimum order size requirements
        min_order_size = self.min_amount.get(trading_pair, Decimal("0"))
        
        # Check if our calculated order size is below minimum
        if order_amount_base < min_order_size:
            self.logger().warning(
                f"Calculated order size {order_amount_base} is below minimum {min_order_size} for {trading_pair}"
            )
            # Adjust to minimum order size if possible
            if self.config.auto_adjust_order_size:
                order_amount_base = min_order_size
                self.logger().info(f"Auto-adjusted order size to minimum: {order_amount_base}")
            else:
                return False
        
        # Check buy exchange has enough quote currency (e.g., USDT)
        buy_quote_balance = buy_connector.get_available_balance(quote_asset)
        required_quote_amount = order_amount_base * opportunity["buy_price"]
        
        # Check sell exchange has enough base currency (e.g., BTC)
        sell_base_balance = sell_connector.get_available_balance(base_asset)
        
        # Log balances for debugging
        self.logger().info(
            f"Balance check for {trading_pair}: "
            f"{buy_exchange} {quote_asset} balance: {buy_quote_balance}, required: {required_quote_amount}. "
            f"{sell_exchange} {base_asset} balance: {sell_base_balance}, required: {order_amount_base}."
        )
        
        # Check if we can execute one-sided arbitrage if configured
        if self.config.arbitrage_mode == "both":
            return buy_quote_balance >= required_quote_amount and sell_base_balance >= order_amount_base
        elif self.config.arbitrage_mode == "buy_only":
            return buy_quote_balance >= required_quote_amount
        elif self.config.arbitrage_mode == "sell_only":
            return sell_base_balance >= order_amount_base
        else:
            return buy_quote_balance >= required_quote_amount and sell_base_balance >= order_amount_base
    
    def _execute_arbitrage_trades(self):
        """Execute arbitrage trades for identified opportunities"""
        for trading_pair, opportunity in self.arbitrage_opportunities.items():
            # Check if we already have active orders for this pair
            if trading_pair in self.active_orders:
                continue
            
            # Check if both exchanges have sufficient balance
            if not self._check_sufficient_balance(trading_pair, opportunity):
                self.logger().warning(
                    f"Skipping arbitrage opportunity for {trading_pair} due to insufficient balance"
                )
                continue
            
            buy_exchange = opportunity["buy_exchange"]
            sell_exchange = opportunity["sell_exchange"]
            
            # Calculate order amount in base currency
            base_asset, quote_asset = split_hb_trading_pair(trading_pair)
            order_amount_quote = self.order_amount_usd
            order_amount_base = order_amount_quote / opportunity["buy_price"]
            
            # Ensure minimum order size
            if order_amount_base < self.min_amount[trading_pair]:
                order_amount_base = self.min_amount[trading_pair]
            
            try:
                # Place buy order
                buy_order_id = self.buy(
                    connector_name=buy_exchange,
                    trading_pair=trading_pair,
                    amount=order_amount_base,
                    order_type=OrderType.LIMIT,
                    price=opportunity["buy_price"] * Decimal("1.001"),  # Small buffer
                )
                
                # Place sell order
                sell_order_id = self.sell(
                    connector_name=sell_exchange,
                    trading_pair=trading_pair,
                    amount=order_amount_base,
                    order_type=OrderType.LIMIT,
                    price=opportunity["sell_price"] * Decimal("0.999"),  # Small buffer
                )
                
                # Record active orders
                self.active_orders[trading_pair] = {
                    "buy": {
                        "order_id": buy_order_id,
                        "exchange": buy_exchange,
                        "timestamp": time.time()
                    },
                    "sell": {
                        "order_id": sell_order_id,
                        "exchange": sell_exchange,
                        "timestamp": time.time()
                    }
                }
                
                self.logger().info(
                    f"Executed arbitrage: Buy {order_amount_base} {trading_pair} on {buy_exchange}, "
                    f"Sell on {sell_exchange}"
                )
            except Exception as e:
                self.logger().error(f"Error executing arbitrage for {trading_pair}: {e}")
    
    def _cleanup_old_orders(self, current_timestamp: float):
        """Cancel orders that have been active for too long"""
        orders_to_remove = []
        
        for trading_pair, orders in self.active_orders.items():
            buy_order = orders["buy"]
            sell_order = orders["sell"]
            
            # Check if buy order is old enough to cancel
            if current_timestamp - buy_order["timestamp"] > self.max_order_age:
                try:
                    self.cancel(buy_order["exchange"], trading_pair, buy_order["order_id"])
                    self.logger().info(f"Canceled old buy order for {trading_pair}")
                except Exception as e:
                    self.logger().error(f"Error canceling buy order: {e}")
            
            # Check if sell order is old enough to cancel
            if current_timestamp - sell_order["timestamp"] > self.max_order_age:
                try:
                    self.cancel(sell_order["exchange"], trading_pair, sell_order["order_id"])
                    self.logger().info(f"Canceled old sell order for {trading_pair}")
                except Exception as e:
                    self.logger().error(f"Error canceling sell order: {e}")
            
            # If both orders are old, remove them from active orders
            if (current_timestamp - buy_order["timestamp"] > self.max_order_age and
                current_timestamp - sell_order["timestamp"] > self.max_order_age):
                orders_to_remove.append(trading_pair)
        
        # Remove completed/canceled orders
        for trading_pair in orders_to_remove:
            del self.active_orders[trading_pair]
    
    def did_fill_order(self, event: OrderFilledEvent):
        """Handle order filled events"""
        self.logger().info(f"Order filled: {event}")
        # You could add more sophisticated handling here
    
    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """Handle buy order completed events"""
        self.logger().info(f"Buy order completed: {event}")
        # You could add more sophisticated handling here
    
    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """Handle sell order completed events"""
        self.logger().info(f"Sell order completed: {event}")
        # You could add more sophisticated handling here
    
    def format_status(self) -> str:
        """Format status display"""
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        # Format timestamps in UTC
        current_time = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S UTC')
        last_checked_time = datetime.utcfromtimestamp(self.last_checked_ts).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        lines = []
        lines.append("Spot Arbitrage Strategy")
        lines.append("---------------------")
        lines.append(f"Current timestamp: {current_time}")
        lines.append(f"Last checked timestamp: {last_checked_time}")
        lines.append(f"Check interval: {self.check_interval} seconds")
        lines.append(f"Ready: {self.ready_to_trade}")
        lines.append(f"Active orders: {len(self.active_orders)}")
        lines.append(f"Arbitrage opportunities: {len(self.arbitrage_opportunities)}")
        lines.append("--------------------------------")
        lines.append(f"Min profitability threshold: {float(self.min_profitability):.2f}%")
        lines.append("--------------------------------")
        
        # Show current prices
        lines.append("\nCurrent Prices:")
        for trading_pair in self.trading_pairs:
            if (self.exchange_1 in self.prices and trading_pair in self.prices[self.exchange_1] and
                self.exchange_2 in self.prices and trading_pair in self.prices[self.exchange_2]):
                
                price_1 = self.prices[self.exchange_1][trading_pair]
                price_2 = self.prices[self.exchange_2][trading_pair]
                
                if price_1 > 0 and price_2 > 0:
                    price_diff_pct = abs(price_1 - price_2) / min(price_1, price_2)
                    lines.append(f"{trading_pair}: {self.exchange_1}={float(price_1):.8f}, "
                                 f"{self.exchange_2}={float(price_2):.8f}, Diff={price_diff_pct:.2%}")
        
        # Show active arbitrage opportunities
        lines.append("\nArbitrage Opportunities:")
        for trading_pair, opportunity in self.arbitrage_opportunities.items():
            lines.append(
                f"{trading_pair}: Buy on {opportunity['buy_exchange']} at {float(opportunity['buy_price']):.8f}, "
                f"Sell on {opportunity['sell_exchange']} at {float(opportunity['sell_price']):.8f}, "
                f"Diff: {float(opportunity['difference_pct']):.2%}"
            )
        
        # Show active orders
        lines.append("\nActive Orders:")
        current_timestamp = time.time()
        for trading_pair, order_info in self.active_orders.items():
            buy_order = order_info["buy"]
            sell_order = order_info["sell"]
            buy_order_age = current_timestamp - buy_order["timestamp"]
            sell_order_age = current_timestamp - sell_order["timestamp"]
            
            lines.append(
                f"{trading_pair}: Buy on {buy_order['exchange']}, Sell on {sell_order['exchange']}, "
                f"Buy Age: {buy_order_age:.1f}s, Sell Age: {sell_order_age:.1f}s"
            )
        
        # Display balances with explicit inclusion of USDC
        lines.append("\nBalances:")
        
        # Get all assets from trading pairs plus USDC
        assets = set()
        for pair in self.trading_pairs:
            base, quote = split_hb_trading_pair(pair)
            assets.add(base)
            assets.add(quote)
        
        # Explicitly add USDC
        assets.add("USDC")
        
        # Create balance data
        balance_data = []
        for exchange in [self.exchange_1, self.exchange_2]:
            connector = self.connectors[exchange]
            for asset in assets:
                total_balance = connector.get_balance(asset)
                available_balance = connector.get_available_balance(asset)
                balance_data.append([
                    exchange,
                    asset,
                    float(total_balance),
                    float(available_balance)
                ])
        
        # Create and display balance DataFrame
        balance_df = pd.DataFrame(
            data=balance_data,
            columns=["Exchange", "Asset", "Total Balance", "Available Balance"]
        )
        
        lines.extend(["    " + line for line in balance_df.to_string(index=False).split("\n")])
        
        return "\n".join(lines)

    def _initialize_markets(self):
        """Initialize markets and validate trading pairs"""
        self.logger().info("Initializing markets...")
        
        # Store valid trading pairs for each exchange
        self.valid_trading_pairs = {
            self.exchange_1: set(),
            self.exchange_2: set()
        }
        
        # Check which trading pairs are valid on each exchange
        for exchange in [self.exchange_1, self.exchange_2]:
            try:
                connector = self.connectors[exchange]
                self.logger().info(f"Fetching trading pairs for {exchange}...")
                
                # Check if connector is ready
                if not connector.ready:
                    self.logger().warning(f"Connector for {exchange} is not ready yet. Using configured pairs.")
                    # Use all configured pairs for now, validation will happen during trading
                    self.valid_trading_pairs[exchange] = set(self.trading_pairs)
                    continue
                    
                # Get trading pairs with a timeout protection
                exchange_trading_pairs = connector.get_trading_pairs()
                self.logger().info(f"Found {len(exchange_trading_pairs)} pairs on {exchange}")
                
                # Validate our trading pairs against available pairs
                for trading_pair in self.trading_pairs:
                    if trading_pair in exchange_trading_pairs:
                        self.valid_trading_pairs[exchange].add(trading_pair)
                    else:
                        self.logger().warning(f"Trading pair {trading_pair} not available on {exchange}")
            
            except Exception as e:
                self.logger().error(f"Error fetching trading pairs for {exchange}: {e}")
                # Use all configured pairs as fallback
                self.valid_trading_pairs[exchange] = set(self.trading_pairs)
        
        # Find common trading pairs available on both exchanges
        self.common_trading_pairs = list(
            self.valid_trading_pairs[self.exchange_1].intersection(
                self.valid_trading_pairs[self.exchange_2]
            )
        )
        
        if not self.common_trading_pairs:
            self.logger().warning("No common trading pairs found! Using all configured pairs for now.")
            self.common_trading_pairs = self.trading_pairs
        else:
            self.logger().info(f"Found {len(self.common_trading_pairs)} common trading pairs")
        
        # Initialize minimum order amounts only for common pairs
        self.min_amount = self._initialize_min_amounts() 