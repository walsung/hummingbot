import logging
import pandas as pd
import numpy as np
import time

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
from hummingbot.strategy_v2.script_strategy_base import ScriptStrategyBase

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.connector_base import ConnectorBase
from enum import Enum

class PositionMode(Enum):
    HEDGE = "Hedge"
    ONEWAY = "OneWay"

class Rebalance_example(ScriptStrategyBase):
    """
    This strategy is used to rebalance a perpetual position.
    """
    #connector_name = "binance_perpetual"
    connector_name = "binance_paper_trade"
    last_ordered_ts = 0

    trading_pair = [
      "BERA-USDT", 
      "TON-USDT", 
      "XRP-USDT"
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
    leverage = 20
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
      "BTC-USDT": Decimal("0.001"),
      "SOL-USDT": Decimal("1"),
      "TON-USDT": Decimal("1"),
      "SUN-USDT": Decimal("130"),
      "BOND-USDT": Decimal("2.6"),
      "ALPACA-USOT": Decimal("23"),
      "REEF-USDT": Decimal("5189"),
      "RARE-USDT": Decimal("21"),
      "REZ-USDT": Decimal("97"),
      "DDGS-USDT": Decimal("3976"),
      "TRX-USDT": Decimal("32"),
      "XRP-USDT": Decimal("9.4"),
      "ADA-USDT": Decimal("15"),
      "UNI-USDT": Decimal("1"),
      "PNUT-USDT": Decimal("4"),
      "SUI-USDT": Decimal("1.4"),
    }

    @property
    def connector(self) -> ExchangeBase:
        return self.connectors[self.connector_name]

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        # is necessary to start the candle feed
        self.check_and_set_leverage()

    def check_and_set_leverage(self):
        if not self.set_leverage_flag:
            perp_connector = self.connector
            perp_connector.set_position_mode(PositionMode.HEDGE)
            for trading_pair in self.trading_pair:
                perp_connector.set_leverage(
                    trading_pair=trading_pair, leverage=self.leverage
                )
            self.logger().info(
                f"Setting leverage to {self.leverage} for {perp_connector} on {self.trading_pair}"
            )
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
                    self.cancel_all_order()
                    time.sleep(1)
                    self.get_balance()
                    time.sleep(1)
                    self.create_order()
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
        1. retrieve usdt balance
        2. get all trading pair status
        3. calculate every trading pair value
        4. calculate the gain/loss
        """
        print("retrieve order amount")
        balance = self.get_balance_df()
        self.get_balance = Decimal(float(balance.loc[balance['Asset'] == "USDT", 'Total Balance']))
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
        for tp in self.asset_value:
            if self.asset_value[tp] >= rb["target_value"] * (1 + rb["threshold"]):
                # sell order: when position value is high
                self.sell(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT_MAKER,
                    self.price[tp] * Decimal("1.001"),
                    common.PositionAction.CLOSE
                )
            elif self.asset_value[tp] < rb["target_value"] * (1 - rb["threshold"]):
                # open order: when position value is low
                self.buy(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT_MAKER,
                    self.price[tp] * Decimal("0.9999"),
                    common.PositionAction.OPEN
                )
            else:
                # within the threshold, sell and buy
                self.sell(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT_MAKER,
                    self.price[tp] * Decimal("1.005"),
                    common.PositionAction.CLOSE
                )
                self.buy(
                    self.rb["connector_name"], 
                    tp,
                    max(Decimal(rb["target_value"] * rb["threshold"]) / self.price[tp], self.min_amount[tp]),
                    OrderType.LIMIT_MAKER,
                    self.price[tp] * Decimal("0.9949"),
                    common.PositionAction.OPEN
                )
                
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

         
        # Process each trading pair in our strategy
        for tp in self.trading_pair:
            position_pair = tp + "LONG"
            
            # Default values
            amount = Decimal("0")
            entry_price = Decimal("0")
            unrealized_pnl = Decimal("0")
# """ 
#     """ 
#     def did_create_buy_order(self, event: BuyOrderCreatedEvent):
#         """
#         Handle buy order created event
#         """
#         self.logger().info(f"Buy order {event.order_id} created for {event.trading_pair} at {event.price}")
#         # Update the active order tracking
#         self.activate_order_id[event.order_id] = {
#             "trading_pair": event.trading_pair,
#             "price": event.price,
#             "amount": event.amount,
#             "type": "BUY",
#             "timestamp": self.current_timestamp
#         }

#     def did_create_sell_order(self, event: SellOrderCreatedEvent):
#         """
#         Handle sell order created event
#         """
#         self.logger().info(f"Sell order {event.order_id} created for {event.trading_pair} at {event.price}")
#         # Update the active order tracking
#         self.activate_order_id[event.order_id] = {
#             "trading_pair": event.trading_pair,
#             "price": event.price,
#             "amount": event.amount,
#             "type": "SELL",
#             "timestamp": self.current_timestamp
#         }
    
#     def did_fill_order(self, event: OrderFilledEvent):
#         """
#         Handle order filled events to track position changes
#         """
#         order_id = event.order_id
#         if order_id in self.activate_order_id:
#             order_data = self.activate_order_id[order_id]
#             fill_price = event.price
#             fill_amount = event.amount
            
#             # Log the fill
#             self.logger().info(
#                 f"Order {order_id} filled: {order_data['type']} {fill_amount} {order_data['trading_pair']} @ {fill_price}"
#             )
            
#             # Update position tracking
#             trading_pair = order_data['trading_pair']
            
#             # Force refresh of position data on next tick
#             self.last_ordered_ts = 0
            
#             # Remove from active orders
#             if event.trade_type == TradeType.BUY:
#                 self.logger().info(f"Increased position in {trading_pair}")
#             else:
#                 self.logger().info(f"Decreased position in {trading_pair}")
#  """
# # '''     """