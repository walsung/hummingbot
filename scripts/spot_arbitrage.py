"""
Spot Arbitrage Strategy V2

This strategy monitors price differences between the same trading pair on two exchanges
and executes arbitrage trades when profitable opportunities arise.
"""

import logging
import time
import asyncio
import random
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Dict, List, Optional, Set, Union, Any
from pydantic import Field, validator

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
        
        # Initialize min_amount dictionary with defaults and user config
        self.min_amount = self._initialize_min_amounts()
        
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
        
        # All checks passed, set ready to trade
        self.ready_to_trade = True
        self.logger().info("Strategy initialized successfully.")
    
    def _initialize_min_amounts(self) -> Dict[str, Decimal]:
        """Initialize minimum order amounts from config and defaults"""
        # Set some reasonable defaults for common pairs
        defaults = {
            "BTC-USDT": Decimal("0.0001"),
            "ETH-USDT": Decimal("0.01"),
            "SOL-USDT": Decimal("0.1"),
            "BNB-USDT": Decimal("0.01"),
            "ADA-USDT": Decimal("10"),
            "XRP-USDT": Decimal("10"),
            "DOGE-USDT": Decimal("10"),
        }
        
        # Override with user configuration
        min_amounts = {}
        for pair in self.trading_pairs:
            if pair in self.config.min_order_amounts:
                min_amounts[pair] = Decimal(str(self.config.min_order_amounts[pair]))
            elif pair in defaults:
                min_amounts[pair] = defaults[pair]
            else:
                min_amounts[pair] = Decimal("0.01")  # Default fallback
                
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
            if exchange not in self.prices:
                self.prices[exchange] = {}
            
            for trading_pair in self.trading_pairs:
                try:
                    # Get mid price from the order book
                    connector = self.connectors[exchange]
                    order_book = connector.get_order_book(trading_pair)
                    best_ask = Decimal(str(order_book.ask_price()))
                    best_bid = Decimal(str(order_book.bid_price()))
                    
                    # Using mid price for comparison
                    mid_price = (best_ask + best_bid) / Decimal("2")
                    self.prices[exchange][trading_pair] = mid_price
                    
                    # Store bid and ask separately for placing actual orders
                    self.prices[f"{exchange}_bid"] = best_bid
                    self.prices[f"{exchange}_ask"] = best_ask
                except Exception as e:
                    self.logger().error(f"Error updating prices for {exchange} {trading_pair}: {e}")
    
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
    
    def _execute_arbitrage_trades(self):
        """Execute arbitrage trades for identified opportunities"""
        for trading_pair, opportunity in self.arbitrage_opportunities.items():
            # Check if we already have active orders for this pair
            if trading_pair in self.active_orders:
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