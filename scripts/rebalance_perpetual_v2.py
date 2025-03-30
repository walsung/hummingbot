import logging
import pandas as pd
import numpy as np
import time
import asyncio
import random

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
from typing import Dict, Any, List
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.connector.derivative.position import Position 
from hummingbot.connector.derivative_base import DerivativeBase

from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.core.data_type import common
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.connector_base import ConnectorBase
from enum import Enum
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit

class PositionMode(Enum):
    HEDGE = "Hedge"
    ONEWAY = "OneWay"

class Rebalance_perpetual(ScriptStrategyBase):
    """
    This strategy is used to rebalance a perpetual position across multiple exchanges.
    """
    # Define connectors and their trading pairs
    exchange_configs = {
        "bybit_perpetual": {
            "trading_pairs": [
                "BERA-USDT",
                "DOGE-USDT",
                "GALA-USDT",
                "1000PEPE-USDT",
                "MEME-USDT",
                "OP-USDT",
                "INJ-USDT",
                "ADA-USDT",
                "ORDI-USDT",
                "NEAR-USDT",
                "SEI-USDT",
                "ICP-USDT",
                "WLD-USDT",
                "BSV-USDT",
                "BNB-USDT",
                "AVAX-USDT",
                "ETH-USDT",
                "ALGO-USDT",
                "TIA-USDT",
                "BTC-USDT",
                "SOL-USDT",
                "TON-USDT",
                "SUN-USDT",
                "ALPACA-USDT",
                "RARE-USDT",
                "REZ-USDT",
                "TRX-USDT",
                "XRP-USDT",
                "UNI-USDT",
                "PNUT-USDT",
                "SUI-USDT",
                "MNT-USDT",
                "ENA-USDT",
                "JUP-USDT",
                "WIF-USDT",
                "VVV-USDT",
                "BANANAS31-USDT",
                "WAL-USDT"
            ],
            "leverage": 4,
            "max_leverage": 4,
            "min_leverage": 2,
            "threshold": Decimal("0.05"),
            "target_value": Decimal("200"),
        },
        "binance": {
            "trading_pairs": [
                "BTC-USDT",
                "ETH-USDT",
                "SOL-USDT",
                "BNB-USDT",
                "XRP-USDT",
                "DOGE-USDT",
                "ADA-USDT",
                "AVAX-USDT",
                "MATIC-USDT",
                "DOT-USDT"
            ],
            "leverage": 3,
            "max_leverage": 3,
            "min_leverage": 1,
            "threshold": Decimal("0.05"),
            "target_value": Decimal("200"),
        }
    }
    
    # Define markets as a class attribute
    markets = {}
    for exchange, config in exchange_configs.items():
        markets[exchange] = config["trading_pairs"]
    
    # Common variables
    last_ordered_ts = 0
    buy_interval = 60  # check interval in seconds
    
    # Store data for each exchange
    price = {}
    activate_order_id = {}
    asset_value = {}
    
    # minimal quantity
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
      "AVAX-USDT": Decimal("0.1"),
      "ETH-USDT": Decimal("0.001"),
      "ALGO-USDT": Decimal("10"),
      "TIA-USDT": Decimal("0.1"),
      "BTC-USDT": Decimal("0.0001"),
      "SOL-USDT": Decimal("0.1"),
      "TON-USDT": Decimal("0.1"),
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
      "BONK-USDT": Decimal("24330"),
      "FLOKI-USDT": Decimal("4367"),
      "SHIB-USDT": Decimal("24330"),
      "DOT-USDT": Decimal("0.1"),
      "VVV-USDT": Decimal("1"),
      "BANANAS31-USDT": Decimal("1"),
      "WAL-USDT": Decimal("1")
    }
    
    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        
        # Initialize exchange-specific configurations
        self.exchange_data = {}
        self.instance_markets = {}  # Store instance-specific market data
        
        # Setup each exchange
        for exchange_name in self.exchange_configs:
            if exchange_name in connectors:
                # Initialize exchange data
                self.exchange_data[exchange_name] = {
                    "status": "",
                    "trading_pairs": [],
                    "leverage": self.exchange_configs[exchange_name]["leverage"],
                    "threshold": self.exchange_configs[exchange_name]["threshold"],
                    "target_value": self.exchange_configs[exchange_name]["target_value"],
                }
                
                # Validate trading pairs for this exchange
                self.validate_trading_pairs(exchange_name)
                
                # Set leverage for this exchange
                self.check_and_set_leverage(exchange_name)
                
                # Add to instance markets
                self.instance_markets[exchange_name] = self.exchange_data[exchange_name]["trading_pairs"]
        
        # Create throttlers for each exchange
        self.throttlers = {
            "bybit_perpetual": AsyncThrottler(
                rate_limits=[
                    RateLimit(limit_id="GET", limit=20, time_interval=1.0),
                    RateLimit(limit_id="POST", limit=5, time_interval=1.0),
                ]
            ),
            "binance_perpetual": AsyncThrottler(
                rate_limits=[
                    RateLimit(limit_id="GET", limit=20, time_interval=1.0),
                    RateLimit(limit_id="POST", limit=5, time_interval=1.0),
                ]
            )
        }
    
    def validate_trading_pairs(self, exchange_name):
        """Validate trading pairs for a specific exchange"""
        if exchange_name not in self.connectors:
            self.logger().warning(f"Exchange {exchange_name} not available. Skipping.")
            return
            
        connector = self.connectors[exchange_name]
        valid_trading_pairs = []
        
        # Get trading pairs for this exchange
        trading_pairs = self.exchange_configs[exchange_name]["trading_pairs"]
        
        # Validate each trading pair
        for trading_pair in trading_pairs:
            try:
                # Try to get the exchange symbol
                exchange_symbol = connector.exchange_symbol_associated_to_pair(trading_pair)
                valid_trading_pairs.append(trading_pair)
                self.logger().info(f"Validated trading pair: {trading_pair} on {exchange_name}")
            except Exception as e:
                self.logger().warning(f"Trading pair {trading_pair} not available on {exchange_name}: {str(e)}. Skipping.")
        
        # Update exchange data with valid pairs
        self.exchange_data[exchange_name]["trading_pairs"] = valid_trading_pairs
        
        self.logger().info(f"Using {len(valid_trading_pairs)} valid trading pairs on {exchange_name}: {valid_trading_pairs}")
    
    def check_and_set_leverage(self, exchange_name):
        """Set leverage for a specific exchange"""
        if exchange_name not in self.connectors:
            return
            
        connector = self.connectors[exchange_name]
        leverage = self.exchange_configs[exchange_name]["leverage"]
        
        try:
            # Set position mode if supported
            if hasattr(connector, "set_position_mode"):
                connector.set_position_mode(PositionMode.HEDGE)
            
            # Set leverage for each trading pair
            for trading_pair in self.exchange_data[exchange_name]["trading_pairs"]:
                try:
                    connector.set_leverage(
                        trading_pair=trading_pair, leverage=leverage
                    )
                    self.logger().info(f"Set leverage to {leverage} for {trading_pair} on {exchange_name}")
                except Exception as e:
                    self.logger().warning(f"Error setting leverage for {trading_pair} on {exchange_name}: {str(e)}")
            
            self.logger().info(f"Leverage setting completed for {exchange_name}")
        except Exception as e:
            self.logger().error(f"Error setting position mode on {exchange_name}: {str(e)}")
    
    def on_tick(self):
        """
        Main strategy logic, execute at an interval
        """
        # Check if it reaches the next checkpoint interval
        if self.last_ordered_ts < (self.current_timestamp - self.buy_interval):
            # Process each exchange
            for exchange_name in self.exchange_data:
                if exchange_name not in self.connectors:
                    continue
                    
                # Initialize if needed
                if self.exchange_data[exchange_name]["status"] == "":
                    self.init_exchange(exchange_name)
                elif self.exchange_data[exchange_name]["status"] == "ACTIVATE":
                    try:
                        # Use safe_ensure_future to run operations with rate limiting
                        safe_ensure_future(self.rate_limited_operations(exchange_name))
                    except Exception as e:
                        self.logger().error(f"Error in on_tick for {exchange_name}: {str(e)}")
            
            self.last_ordered_ts = self.current_timestamp
    
    def init_exchange(self, exchange_name):
        """Initialize an exchange"""
        self.logger().info(f"Initializing {exchange_name}...")
        self.exchange_data[exchange_name]["status"] = "ACTIVATE"
    
    def cancel_all_orders(self, exchange_name=None):
        """Cancel all orders for a specific exchange or all exchanges"""
        if exchange_name:
            if exchange_name in self.connectors:
                safe_ensure_future(self.connectors[exchange_name].cancel_all(timeout_seconds=6))
        else:
            for exchange in self.connectors.values():
                safe_ensure_future(exchange.cancel_all(timeout_seconds=6))
    
    async def rate_limited_operations(self, exchange_name):
        """Execute rate-limited operations for a specific exchange"""
        # Get the throttler for this exchange
        throttler = self.throttlers.get(exchange_name)
        
        # Use the throttler to limit API calls
        async with throttler.execute_task(limit_id="GET"):
            # Get prices and positions
            await self.get_price(exchange_name)
            await self.get_position(exchange_name)
        
        # Create orders with rate limiting
        await self.create_order(exchange_name)
    
    async def get_price(self, exchange_name):
        """Get prices for a specific exchange"""
        connector = self.connectors[exchange_name]
        for tp in self.exchange_data[exchange_name]["trading_pairs"]:
            try:
                price = connector.get_mid_price(tp)
                self.price[tp] = price
            except Exception as e:
                self.logger().error(f"Error getting price for {tp} on {exchange_name}: {str(e)}")
    
    async def get_position(self, exchange_name):
        """Get positions for a specific exchange"""
        connector = self.connectors[exchange_name]
        
        # Get all positions
        positions = connector.account_positions
        
        # Process positions for this exchange
        for tp in self.exchange_data[exchange_name]["trading_pairs"]:
            position_pair = tp + "LONG"
            self.asset_value[tp] = Decimal("0")
            
            if position_pair in positions:
                amount = Decimal(positions[position_pair].amount)
                self.asset_value[tp] = amount * self.price.get(tp, Decimal("0"))
    
    async def create_order(self, exchange_name):
        """Create orders for a specific exchange"""
        connector = self.connectors[exchange_name]
        threshold = self.exchange_data[exchange_name]["threshold"]
        target_value = self.exchange_data[exchange_name]["target_value"]
        
        # Process only a few trading pairs at a time
        processed_pairs = 0
        max_pairs_per_cycle = 3
        
        # Get available balance
        balance_df = self.get_balance_df(exchange_name)
        usdt_balance = balance_df.loc[balance_df['Asset'] == "USDT", 'Available Balance']
        available_balance = Decimal("0")
        if not usdt_balance.empty:
            available_balance = Decimal(float(usdt_balance.iloc[0]))
        
        self.logger().info(f"Available USDT balance on {exchange_name}: {available_balance}")
        
        # Calculate max order value
        leverage = self.exchange_configs[exchange_name]["leverage"]
        max_order_value = available_balance * leverage * Decimal("0.9")
        
        for tp in self.exchange_data[exchange_name]["trading_pairs"]:
            if processed_pairs >= max_pairs_per_cycle:
                break
                
            if tp not in self.asset_value or tp not in self.price:
                continue
                
            # Calculate order size
            target_order_size = max(
                Decimal(target_value * threshold) / self.price[tp], 
                self.min_amount.get(tp, Decimal("1"))
            )
            
            # Check if we need to rebalance
            if self.asset_value[tp] >= target_value * (1 + threshold):
                # Sell order
                self.sell(
                    exchange_name,
                    tp,
                    target_order_size,
                    OrderType.LIMIT,
                    self.price[tp] * Decimal("1.001"),
                    common.PositionAction.CLOSE
                )
                processed_pairs += 1
                await asyncio.sleep(1.0)
                
            elif self.asset_value[tp] < target_value * (1 - threshold):
                # Buy order
                self.buy(
                    exchange_name,
                    tp,
                    target_order_size,
                    OrderType.LIMIT,
                    self.price[tp] * Decimal("0.9999"),
                    common.PositionAction.OPEN
                )
                processed_pairs += 1
                await asyncio.sleep(1.0)
                
            else:
                # Within threshold - place one random order
                if random.choice([True, False]):
                    self.sell(
                        exchange_name,
                        tp,
                        target_order_size,
                        OrderType.LIMIT,
                        self.price[tp] * Decimal("1.005"),
                        common.PositionAction.CLOSE
                    )
                else:
                    self.buy(
                        exchange_name,
                        tp,
                        target_order_size,
                        OrderType.LIMIT,
                        self.price[tp] * Decimal("0.9949"),
                        common.PositionAction.OPEN
                    )
                processed_pairs += 1
                await asyncio.sleep(1.0)
    
    def get_balance_df(self, exchange_name=None):
        """Get balance dataframe for a specific exchange"""
        if exchange_name is None:
            # For backward compatibility
            exchange_name = list(self.exchange_data.keys())[0]
            
        connector = self.connectors[exchange_name]
        all_balances = connector.get_all_balances()
        
        df = pd.DataFrame(columns=["Exchange", "Asset", "Total Balance", "Available Balance"])
        for asset, balance in all_balances.items():
            df = pd.concat([df, pd.DataFrame([{
                "Exchange": exchange_name,
                "Asset": asset,
                "Total Balance": balance.total,
                "Available Balance": balance.available
            }])], ignore_index=True)
            
        return df 