"""
Spot Arbitrage Strategy

This strategy monitors price differences between the same trading pair on two exchanges
(Binance and Bybit) and executes arbitrage trades when profitable opportunities arise.
"""

import logging
import pandas as pd
import numpy as np
import time
import asyncio
import random
from datetime import datetime

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.event.events import (
  BuyOrderCompletedEvent,
  BuyOrderCreatedEvent,
  MarketOrderFailureEvent,
  OrderCancelledEvent,
  OrderFilledEvent,
  SellOrderCompletedEvent,
  SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import Decimal, OrderType, ScriptStrategyBase
from typing import Dict, Any, List, Set
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.core.data_type import common
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit

class SpotArbitrage(ScriptStrategyBase):
    """
    This strategy monitors price differences between the same trading pair on two exchanges
    and executes arbitrage trades when profitable opportunities arise.
    """
    # Define common trading pairs for both exchanges
    common_trading_pairs = [
        # "BERA-USDT",
        # "DOGE-USDT",
        # "GALA-USDT",
        # "MEME-USDT",
        # "OP-USDT",
        # "INJ-USDT",
        # "ADA-USDT",
        # "ORDI-USDT",
        # "NEAR-USDT",
        # "SEI-USDT",
        # "ICP-USDT",
        # "WLD-USDT",
        # "BNB-USDT",
        # "AVAX-USDT",
        # "ETH-USDT",
        # "ALGO-USDT",
        # "TIA-USDT",
        # "BTC-USDT",
        # "SOL-USDT",
        # "TON-USDT",
        # "SUN-USDT",
        # # "ALPACA-USDT",
        # # "RARE-USDT",
        # # "REZ-USDT",
        # "TRX-USDT",
        # "XRP-USDT",
        # "UNI-USDT",
        # "PNUT-USDT",
        # "SUI-USDT",
        # "ENA-USDT",
        # "JUP-USDT",
        # "WIF-USDT",
        # "BANANAS31-USDT",
      #  "WAL-USDT",
        "BNB-USDC",
        "BCH-USDC",
        "NEAR-USDC",
        "WIF-USDC",
        "BONK-USDC",
        "SOL-USDC",
        "XRP-USDC",
        "ADA-USDC",
        "AVAX-USDC",
        "TON-USDC"
        
    ]
    
    # Define exchanges and trading pairs
    exchange_configs = {
        "binance": {
            "min_profitability": Decimal("0.005"),  # 0.5% minimum profitability
            "order_amount_usd": Decimal("20"),      # Order size in USD
            "max_order_age": 60,                    # Max order age in seconds
        },
        "bybit": {
            "min_profitability": Decimal("0.005"),  # 0.5% minimum profitability
            "order_amount_usd": Decimal("20"),      # Order size in USD
            "max_order_age": 60,                    # Max order age in seconds
        }
    }
    
    # Arbitrage threshold (percentage)
    THRESHOLD = Decimal("0.1")  # value 1 = 1% price difference threshold,  0.1 = 0.1% price difference threshold
    
    # Define markets as a class attribute
    markets = {}
    for exchange in exchange_configs:
        markets[exchange] = common_trading_pairs
    
    # Common variables
    last_checked_ts = 0
    check_interval = 5  # check interval in seconds
    
    # Store data for each exchange
    prices = {}
    active_orders = {}
    arbitrage_opportunities = {}
    
    # Minimum order sizes for various trading pairs
    
    min_amount = {
      "BERA-USDT": Decimal("10"),
      "DOGE-USDT": Decimal("12"),
      "LDOM-USDT": Decimal("10"),
      "GALA-USDT": Decimal("10"),
      "1000PEPE-USDT": Decimal("309"),
      "MEME-USDT": Decimal("1"),
      "OP-USDT": Decimal("1.3"),
      "INJ-USDT": Decimal("0.3"),
      "ADA-USDT": Decimal("10"),
      "ORDI-USDT": Decimal("0.1"),
      "MATIC-USDT": Decimal("10"),
      "NEAR-USDT": Decimal("1"),
      "SEI-USDT": Decimal("10"),
      "ICP-USDT": Decimal("0.1"),
      "WLD-USDT": Decimal("3"),
      "BSV-USDT": Decimal("0.01"),
      "BNB-USDT": Decimal("0.01"),
      "AVAX-USDT": Decimal("1"),
      "ETH-USDT": Decimal("0.01"),
      "ALGO-USDT": Decimal("10"),
      "TIA-USDT": Decimal("1"),
      "BTC-USDT": Decimal("0.0001"),
      "SOL-USDT": Decimal("1"),
      "TON-USDT": Decimal("1"),
      "SUN-USDT": Decimal("130"),
      "BOND-USDT": Decimal("2.6"),
      "ALPACA-USDT": Decimal("23"),
      "REEF-USDT": Decimal("5189"),
      "RARE-USDT": Decimal("21"),
      "REZ-USDT": Decimal("97"),
      "DDGS-USDT": Decimal("3976"),
      "TRX-USDT": Decimal("32"),
      "XRP-USDT": Decimal("9.4"),
      "UNI-USDT": Decimal("1"),
      "PNUT-USDT": Decimal("4"),
      "SUI-USDT": Decimal("1.4"),
      "MNT-USDT": Decimal("741"),
      "ENA-USDT": Decimal("1.1"),
      "JUP-USDT": Decimal("0.8"),
      "WIF-USDT": Decimal("5"),
      "BONK-USDT": Decimal("24331"),
      "FLOKI-USDT": Decimal("4367"),
      "SHIB-USDT": Decimal("24330"),
      "AMI-USDT": Decimal("10"),      # AMI: Min Transaction Limit = 10
    # 2025 March -- Newer pairs (not in Convert list, inferred from Spot Trading Rules)
      "VVV-USDT": Decimal("1"),       # Default to 1 USDT notional value (Source 5)
      "BANANAS31-USDT": Decimal("1"), # Unlisted; use 1 USDT equivalent
      "KILO-USDT": Decimal("1"),      # Unlisted; use 1 USDT equivalent
      "WAL-USDT": Decimal("1"),       # Unlisted; use 1 USDT equivalent
      "B3TR-USDT": Decimal("7")       # From Convert list (B3TR: Min = 7)
    }
    
    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        
        # Initialize exchange-specific configurations
        self.exchange_data = {}
        self.instance_markets = {}
        
        # Setup each exchange
        for exchange_name in self.exchange_configs:
            if exchange_name in connectors:
                # Initialize exchange data
                self.exchange_data[exchange_name] = {
                    "status": "ACTIVE",
                    "trading_pairs": [],
                    "min_profitability": self.exchange_configs[exchange_name]["min_profitability"],
                    "order_amount_usd": self.exchange_configs[exchange_name]["order_amount_usd"],
                    "max_order_age": self.exchange_configs[exchange_name]["max_order_age"],
                }
                
                # Validate trading pairs for this exchange
                self.validate_trading_pairs(exchange_name)
                
                # Add to instance markets
                self.instance_markets[exchange_name] = self.exchange_data[exchange_name]["trading_pairs"]
        
        # Create throttlers for each exchange
        self.throttlers = {
            "binance": AsyncThrottler(
                rate_limits=[
                    RateLimit(limit_id="GET", limit=40, time_interval=1.0),
                    RateLimit(limit_id="POST", limit=15, time_interval=1.0),
                ]
            ),
            "bybit": AsyncThrottler(
                rate_limits=[
                    RateLimit(limit_id="GET", limit=40, time_interval=1.0),
                    RateLimit(limit_id="POST", limit=15, time_interval=1.0),
                ]
            )
        }
        
        self.logger().info(f"Initialized SpotArbitrage strategy with {len(self.common_trading_pairs)} common trading pairs")
        self.logger().info(f"Common trading pairs: {self.common_trading_pairs}")
    
    def validate_trading_pairs(self, exchange_name):
        """Validate trading pairs for a specific exchange"""
        if exchange_name not in self.connectors:
            self.logger().error(f"Exchange {exchange_name} not found in connectors")
            return
            
        connector = self.connectors[exchange_name]
        valid_trading_pairs = []
        
        # Filter to only include common trading pairs
        for trading_pair in self.common_trading_pairs:
            try:
                # Try to get the exchange symbol - this will fail if the pair doesn't exist
                exchange_symbol = connector.exchange_symbol_associated_to_pair(trading_pair)
                valid_trading_pairs.append(trading_pair)
                self.logger().info(f"Validated trading pair on {exchange_name}: {trading_pair}")
            except Exception as e:
                self.logger().warning(f"Trading pair {trading_pair} not available on {exchange_name}: {str(e)}. Skipping.")
        
        # Update trading_pair list with only valid pairs
        self.exchange_data[exchange_name]["trading_pairs"] = valid_trading_pairs
    
    def on_tick(self):
        """
        Main strategy logic, executed at regular intervals
        1. Check if it's time to run the strategy
        2. Get current prices from both exchanges
        3. Identify arbitrage opportunities
        4. Execute trades for profitable opportunities
        """
        # Check if it's time to run the strategy
        if self.last_checked_ts < (self.current_timestamp - self.check_interval):
            # Update prices and look for arbitrage opportunities
            safe_ensure_future(self.update_prices_and_find_opportunities())
            self.last_checked_ts = self.current_timestamp
    
    async def update_prices_and_find_opportunities(self):
        """Update prices and find arbitrage opportunities"""
        try:
            # Get current prices for all trading pairs on both exchanges
            await self.update_prices()
            
            # Find arbitrage opportunities
            await self.find_arbitrage_opportunities()
            
            # Execute trades for profitable opportunities
            await self.execute_arbitrage_trades()
        except Exception as e:
            self.logger().error(f"Error in update_prices_and_find_opportunities: {str(e)}")
    
    async def update_prices(self):
        """Update prices for all trading pairs on both exchanges"""
        for exchange_name in self.exchange_data:
            if exchange_name not in self.connectors:
                continue
                
            connector = self.connectors[exchange_name]
            
            for trading_pair in self.exchange_data[exchange_name]["trading_pairs"]:
                try:
                    # Use throttler to respect rate limits
                    async with self.throttlers[exchange_name].execute_task(limit_id="GET"):
                        # Get mid price
                        price = connector.get_mid_price(trading_pair)
                        
                        # Store price
                        if trading_pair not in self.prices:
                            self.prices[trading_pair] = {}
                        self.prices[trading_pair][exchange_name] = price
                except Exception as e:
                    self.logger().error(f"Error getting price for {trading_pair} on {exchange_name}: {str(e)}")
    
    async def find_arbitrage_opportunities(self):
        """Find arbitrage opportunities between exchanges"""
        self.arbitrage_opportunities = {}
        
        for trading_pair in self.common_trading_pairs:
            # Check if we have prices for this pair on both exchanges
            if trading_pair not in self.prices:
                continue
                
            if "binance" not in self.prices[trading_pair] or "bybit" not in self.prices[trading_pair]:
                continue
            
            binance_price = self.prices[trading_pair]["binance"]
            bybit_price = self.prices[trading_pair]["bybit"]
            
            # Calculate price difference as a percentage
            price_diff_pct = abs(binance_price - bybit_price) / min(binance_price, bybit_price)
            
            # Determine which exchange has higher price
            if binance_price > bybit_price:
                higher_exchange = "binance"
                lower_exchange = "bybit"
            else:
                higher_exchange = "bybit"
                lower_exchange = "binance"
            
            # Check if the price difference exceeds the threshold
            min_profitability = self.THRESHOLD / Decimal("100")  # Convert percentage to decimal
            
            if price_diff_pct >= min_profitability:
                self.arbitrage_opportunities[trading_pair] = {
                    "higher_exchange": higher_exchange,
                    "lower_exchange": lower_exchange,
                    "higher_price": max(binance_price, bybit_price),
                    "lower_price": min(binance_price, bybit_price),
                    "price_diff_pct": price_diff_pct,
                    "profitable": True
                }
                
                self.logger().info(
                    f"Arbitrage opportunity found for {trading_pair}: "
                    f"Buy on {lower_exchange} at {min(binance_price, bybit_price)}, "
                    f"Sell on {higher_exchange} at {max(binance_price, bybit_price)}, "
                    f"Difference: {price_diff_pct:.2%}"
                )
            else:
                # Close any existing orders if price difference is minimal
                if price_diff_pct < min_profitability * Decimal("0.5"):
                    self.arbitrage_opportunities[trading_pair] = {
                        "higher_exchange": higher_exchange,
                        "lower_exchange": lower_exchange,
                        "higher_price": max(binance_price, bybit_price),
                        "lower_price": min(binance_price, bybit_price),
                        "price_diff_pct": price_diff_pct,
                        "profitable": False
                    }
    
    async def execute_arbitrage_trades(self):
        """Execute trades for profitable arbitrage opportunities"""
        for trading_pair, opportunity in self.arbitrage_opportunities.items():
            if opportunity["profitable"]:
                # Calculate order amount in base currency
                higher_exchange = opportunity["higher_exchange"]
                lower_exchange = opportunity["lower_exchange"]
                
                # Get order amount in USD
                order_amount_usd = min(
                    self.exchange_data[higher_exchange]["order_amount_usd"],
                    self.exchange_data[lower_exchange]["order_amount_usd"]
                )
                
                # Convert to base currency amount
                base_amount = order_amount_usd / opportunity["higher_price"]
                
                # Ensure minimum order size
                min_amount = self.min_amount.get(trading_pair, Decimal("0.001"))
                base_amount = max(base_amount, min_amount)
                
                # Execute trades
                try:
                    # Sell on higher exchange
                    async with self.throttlers[higher_exchange].execute_task(limit_id="POST"):
                        self.sell(
                            connector_name=higher_exchange,
                            trading_pair=trading_pair,
                            amount=base_amount,
                            order_type=OrderType.MARKET,
                            price=opportunity["higher_price"]
                        )
                        self.logger().info(f"Placed SELL order on {higher_exchange} for {trading_pair}: {base_amount} @ {opportunity['higher_price']}")
                    
                    # Buy on lower exchange
                    async with self.throttlers[lower_exchange].execute_task(limit_id="POST"):
                        self.buy(
                            connector_name=lower_exchange,
                            trading_pair=trading_pair,
                            amount=base_amount,
                            order_type=OrderType.MARKET,
                            price=opportunity["lower_price"]
                        )
                        self.logger().info(f"Placed BUY order on {lower_exchange} for {trading_pair}: {base_amount} @ {opportunity['lower_price']}")
                    
                    # Track active orders
                    if trading_pair not in self.active_orders:
                        self.active_orders[trading_pair] = {}
                    
                    self.active_orders[trading_pair] = {
                        "timestamp": self.current_timestamp,
                        "higher_exchange": higher_exchange,
                        "lower_exchange": lower_exchange,
                        "amount": base_amount
                    }
                    
                except Exception as e:
                    self.logger().error(f"Error executing arbitrage trades for {trading_pair}: {str(e)}")
            
            elif trading_pair in self.active_orders:
                # Check if we need to close positions due to price convergence
                order_age = self.current_timestamp - self.active_orders[trading_pair]["timestamp"]
                max_order_age = max(
                    self.exchange_data["binance"]["max_order_age"],
                    self.exchange_data["bybit"]["max_order_age"]
                )
                
                if order_age > max_order_age:
                    self.logger().info(f"Closing arbitrage position for {trading_pair} due to price convergence or timeout")
                    # Remove from active orders
                    del self.active_orders[trading_pair]
    
    def format_status(self) -> str:
        """Format status display"""
        if not self.ready:
            return "Market connectors are not ready."
        
        # Format timestamps in UTC
        current_time = datetime.utcfromtimestamp(self.current_timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')
        last_checked_time = datetime.utcfromtimestamp(self.last_checked_ts).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        lines = []
        lines.append("Spot Arbitrage Strategy")
        lines.append("---------------------")
        lines.append(f"Current timestamp: {current_time}")
        lines.append(f"Last checked timestamp: {last_checked_time}")
        lines.append(f"Check interval: {self.check_interval} seconds")
        lines.append(f"Ready: {self.ready}")
        lines.append(f"Active orders: {len(self.active_orders)}")
        lines.append(f"Arbitrage opportunities: {len(self.arbitrage_opportunities)}")
        lines.append("--------------------------------")
        lines.append(f"threshold is set to {self.THRESHOLD} %")
        lines.append("--------------------------------")
        
        # Show current prices
        lines.append("\nCurrent Prices:")
        for trading_pair in self.common_trading_pairs:
            if trading_pair in self.prices:
                binance_price = self.prices[trading_pair].get("binance", Decimal("0"))
                bybit_price = self.prices[trading_pair].get("bybit", Decimal("0"))
                
                if binance_price > 0 and bybit_price > 0:
                    price_diff_pct = abs(binance_price - bybit_price) / min(binance_price, bybit_price)
                    lines.append(f"{trading_pair}: Binance={binance_price:.8f}, Bybit={bybit_price:.8f}, Diff={price_diff_pct:.2%}")
        
        # Show active arbitrage opportunities
        lines.append("\nArbitrage Opportunities:")
        for trading_pair, opportunity in self.arbitrage_opportunities.items():
            if opportunity["profitable"]:
                lines.append(
                    f"{trading_pair}: Buy on {opportunity['lower_exchange']} at {opportunity['lower_price']:.8f}, "
                    f"Sell on {opportunity['higher_exchange']} at {opportunity['higher_price']:.8f}, "
                    f"Diff: {opportunity['price_diff_pct']:.2%}"
                )
        
        # Show active orders
        lines.append("\nActive Orders:")
        for trading_pair, order_info in self.active_orders.items():
            order_age = self.current_timestamp - order_info["timestamp"]
            lines.append(
                f"{trading_pair}: Buy on {order_info['lower_exchange']}, Sell on {order_info['higher_exchange']}, "
                f"Amount: {order_info['amount']}, Age: {order_age}s"
            )
        
        return "\n".join(lines)
    
    def did_fill_order(self, event: OrderFilledEvent):
        """Called when an order is filled"""
        self.logger().info(f"Order filled: {event}")
    
    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """Called when a buy order is completed"""
        self.logger().info(f"Buy order completed: {event}")
    
    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """Called when a sell order is completed"""
        self.logger().info(f"Sell order completed: {event}")

    @property
    def ready(self):
        """Check if all connectors are ready"""
        return all(connector.ready for connector in self.connectors.values()) 