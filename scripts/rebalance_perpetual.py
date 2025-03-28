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
from typing import Dict
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.connector.derivative.position import Position 
from hummingbot.connector.derivative_base import DerivativeBase

from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderTypeFilledEvent, OrderType, TradeType
from hummingbot.core.data_type import common
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy_v2.script_strategy_base import ScriptStrategyBase

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.connector_base import ConnectorBase

from typing import Any, List


class Rebalance_example(ScriptStrategyBase):
    """
    This strategy is used to rebalance a perpetual position.
    """
    connector_name = "binance_perpetual"
    last_ordered_ts = 0

    trading_pair = [
      "BERA-USDT", "TON-USDT", "XRP-USDT"
    ]
    # strategy specific variables
    rb: Dict = {
        "connector_name": connector_name,
        "trading_pair": trading_pair,
        "is_buy": True,
        "threshold": Decimal("0.02"),
        "target_value": Decimal("200"),
        "status": "",
        "buy_interval": 10             # check interval in seconds
    }
    markets: {setting["connector_name"]: {setting["trading_pair"]}}
    price = 0


    def on_tick(self):
          """
          every tick triggers a logic
          check if there's need for rebalance
          """
        # check if it reaches the next checkpoint interval
        if self.last_ordered_ts < (self.current_timestamp - self.setting["buy_interval"]):
          # calculate the value of the position and compare with the target value
          # if asset price more than target value * (1+threshold), sell
          # if asset price lower than target value * (1+threshold), buy
          # within the threshold , stay put
            if self.setting.get("status") == "":
                # initialize the position
                self.setting["status"] = "ACTIVATE"
                base, quote = split_hb_trading_pair(self.setting["trading_pair"])
                self.setting["base_asset"] = base
                self.setting["quote_asset"] = quote
                self.setting["start_price"] = self.connectors[self.setting["connector_name"]].get_mid_price(self.setting["trading_pair"])
            elif self.setting["status"] == "ACTIVATE":
                self.cancel_all_order()
                self.get_balance()
                self.create_order

            self.last_ordered_ts = self.current_timestamp

    def cancel_all_order(self):
        active_orders = self.get_active_orders(self.stting["connector_name"])
        for order in active_orders:
            self.cancel(self.setting["connector_name"], self.setting["trading_pair"], order.client_order_id)

    def get_balance(self):
        df = self.get_balance_df()
        self.setting["base_asset"] = float(df.loc[df["Asset"] == self.setting["base"]], 'Total Balance'])
        self.price = float(self.connectors[self.setting["connector_name"]].get_mid_price(self.setting["trading_pair"]))
        self.setting["base_value"] = self.setting["base_asset"] * self.price
        self.setting["quote_value"] = float(df.loc[df["Asset"] == self.setting["quote"]], 'Total Balance'])

    def create_order(self):
      """
      create order based on the difference between base asset value and target value
      """
        setting = self.setting.copy()
        if setting["base_value"] >= setting["target_value"] * (1+setting["threshold"]):
          # sell when base asset value more than market price
            logging.info(f"base asset value too high, current: {setting['base_value']: .2f} target: {setting['target_value']: .2f} difference: {((setting['base_value']/setting['target_value'])-1)*100: .2f}%")
            self.sell(setting['connector_name'], setting['trading_pair'], Decimal(setting['target_value']/self.price * setting['threshold']), OrderType.LIMIT, Decimal(self.price * 1.0001))
        elif setting["base_value"] < setting["target_value"] * (1-setting["threshold"]):
          # buy when base asset value less than market price
            logging.info(f"base asset value too low, current: {setting['base_value']: .2f} target: {setting['target_value']: .2f} difference: {((setting['base_value']/setting['target_value'])-1)*100: .2f}%")
            self.buy(setting['connector_name'], setting['trading_pair'], Decimal(setting['target_value']/self.price * setting['threshold']), OrderType.LIMIT, Decimal(self.price * 0.9999))


    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """
        handle buy order created event
        """
        self.logger().info(logging.INFO, f"The buy order {event.order_id} has been created ")


    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """
        handle sell order created event
        """
        self.logger().info(logging.INFO, f"The sell order {event.order_id} has been created ")

                  
              
    
    


class RebalancePerpetual(ScriptStrategyBase):
        """
    This strategy is used to rebalance a perpetual position.
    """