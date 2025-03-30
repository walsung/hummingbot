"""
bybit_perpetual only has OrderType.LIMIT and MAKER, doesn't have LIMIT_MAKER

connector_name = bybit  doesn't support many crypto pairs. Has to be bybit_perpetual
"""

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
    This strategy is used to rebalance a perpetual position.
    """
    #connector_name = "binance_perpetual"    # "binance_paper_trade"
    connector_name = "bybit_perpetual"
    last_ordered_ts = 0

    trading_pair = [
      "BERA-USDT",
      "DOGE-USDT",
      # "LDOM-USDT",
      "GALA-USDT",
      "1000PEPE-USDT",
      "MEME-USDT",
      "OP-USDT",
      "INJ-USDT",
      "ADA-USDT",
      "ORDI-USDT",
      # "MATIC-USDT",
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
      # "BOND-USDT",
      "ALPACA-USDT",
      # "REEF-USDT",
      "RARE-USDT",
      "REZ-USDT",
      # "DDGS-USDT",
      "TRX-USDT",
      "XRP-USDT",
      "UNI-USDT",
      "PNUT-USDT",
      "SUI-USDT",
      "MNT-USDT",
      "ENA-USDT",
      "JUP-USDT",
      "WIF-USDT",
      # "BONK-USDT",
      # "FLOKI-USDT",
      #"SHIB-USDT",
     # "AMI-USDT",
      "VVV-USDT",
      "BANANAS31-USDT",
      # "KILO-USDT",
      "WAL-USDT"
      # "B3TR-USDT"
    ]
    # strategy specific variables
    rb: Dict = {
        "connector_name": connector_name,
        "trading_pair": trading_pair,
        "is_buy": True,
        "threshold": Decimal("0.05"),
        "target_value": Decimal("200"),
        "status": "",
    }

    # exchange and trading pair matching up
    markets = {rb["connector_name"]: trading_pair}

    buy_interval = 60             # check interval in seconds

    # store the current price of the asset and the dict of order
    price = {}
    activate_order_id = {}
    asset_value = {}

    # hedge fund variables
    position_mode: PositionMode = PositionMode.HEDGE
    set_leverage_flag = False

    # leverage parameters 
    leverage = 4
    max_leverage = 4
    min_leverage = 2
    
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

    @property
    def connector(self) -> ExchangeBase:
        return self.connectors[self.connector_name]

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        
        # Validate trading pairs before anything else
        self.validate_trading_pairs()
        
        # Set leverage after validation
        self.check_and_set_leverage()
        
        # Create throttler with Bybit's specific rate limits - even more conservative
        self.throttler = AsyncThrottler(
            rate_limits=[
                # Global rate limits - reduced to be more conservative
                RateLimit(limit_id="GET", limit=20, time_interval=1.0),  # 20 GET requests per second
                RateLimit(limit_id="POST", limit=5, time_interval=1.0),  # 5 POST requests per second
            ]
        )

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
        self.markets = {self.rb["connector_name"]: valid_trading_pairs}
        
        # Also update min_amount dictionary to only include valid pairs
        valid_min_amounts = {}
        for pair in valid_trading_pairs:
            if pair in self.min_amount:
                valid_min_amounts[pair] = self.min_amount[pair]
        self.min_amount = valid_min_amounts
        
        self.logger().info(f"Using {len(valid_trading_pairs)} valid trading pairs: {valid_trading_pairs}")

    def check_and_set_leverage(self):
        if not self.set_leverage_flag:
            perp_connector = self.connector
            try:
                perp_connector.set_position_mode(PositionMode.HEDGE)
                
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
        every tick triggers a logic
        check if there's need for rebalance

        main strategy logic, execute at an interval
        1. check if it reaches the interval for order
        2. if it's first time to run, initialize the strategy
        3. if the strategy has been activated, execute the rebalance logic
        """
        # check if it reaches the next checkpoint interval
        if self.last_ordered_ts < (self.current_timestamp - self.buy_interval):
            # calculate the value of the position and compare with the target value
            # if asset price more than target value * (1+threshold), sell
            # if asset price lower than target value * (1+threshold), buy
            # within the threshold, stay put
            if self.rb.get("status") == "":
                # initialize the position
                self.init_rebalance()
            elif self.rb["status"] == "ACTIVATE":
                try:
                    # Use safe_ensure_future to run these operations with rate limiting
                    safe_ensure_future(self.rate_limited_operations())
                except Exception as e:
                    self.logger().error(f"Error in on_tick: {str(e)}")
            self.last_ordered_ts = self.current_timestamp                
    
    # cancel all order
    def cancel_all_order(self):
        for exchange in self.connectors.values():
            safe_ensure_future(exchange.cancel_all(timeout_seconds=6))

    # initialization
    def init_rebalance(self):
        print("Start Initialization.....")
        self.rb["status"] = "ACTIVATE"
        self.market = self.rb["connector_name"]

    def get_balance(self):
        """
        get the current balance status:
        1. retrieve balance
        2. get all trading pair status
        3. calculate every trading pair value
        4. calculate the gain/loss
        """
        print("retrieve order amount")
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
        create order based on the difference between base asset value and target value
        1. if position value more than target value +threshold, sell 
        2. if position value less than target value -threshold, buy
        3. if within the threshold, then create both buy and sell orders
        """
        rb = self.rb.copy()
        # Process only 5 trading pairs at a time to avoid rate limits
        processed_pairs = 0
        max_pairs_per_cycle = 5
        
        for tp in self.asset_value:
            if processed_pairs >= max_pairs_per_cycle:
                break
            
            if self.asset_value[tp] >= rb["target_value"] * (1 + rb["threshold"]):
                # sell order: when position value is high
                self.sell(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT,
                    self.price[tp] * Decimal("1.001"),
                    common.PositionAction.CLOSE
                )
                processed_pairs += 1
                time.sleep(0.5)  # Add delay between orders
            
            elif self.asset_value[tp] < rb["target_value"] * (1 - rb["threshold"]):
                # open order: when position value is low
                self.buy(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT,
                    self.price[tp] * Decimal("0.9999"),
                    common.PositionAction.OPEN
                )
                processed_pairs += 1
                time.sleep(0.5)  # Add delay between orders
            
            else:
                # Only place one order (not both) to reduce API calls
                if random.choice([True, False]):
                    self.sell(
                        self.rb["connector_name"], 
                        tp,
                        max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                        OrderType.LIMIT,
                        self.price[tp] * Decimal("1.005"),
                        common.PositionAction.CLOSE
                    )
                else:
                    self.buy(
                        self.rb["connector_name"], 
                        tp,
                        max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                        OrderType.LIMIT,
                        self.price[tp] * Decimal("0.9949"),
                        common.PositionAction.OPEN
                    )
                processed_pairs += 1
                time.sleep(0.5)  # Add delay between orders
                
    # format output
    def format_status(self) -> str:
        """
        returns status of the current strategy on user balances and current active orders. 
        This function is called when status command is issued. Override this function to create
        custom status display output
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

    # retrieve the position stats
    def get_positions_df(self) -> pd.DataFrame:
        """
        Returns a data frame for all asset positions for displaying purpose.
        Implements position tracking based on Shannon's Demon rebalancing approach.
        """          
        columns: List[str] = ["Exchange", 
                              "Trading Pair", 
                              "Amount", 
                              "Entry Price", "Current Price",
                              "Unrealized pnl", 
                              "Percentage" 
                            #  "Position Value", "Target Value", "Drift %", "Unrealized PnL", "Action"
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