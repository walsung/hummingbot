# hummingbot/strategy_v2/backtesting/arbitrage_backtesting_engine.py

import pandas as pd
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.strategy_v2.backtesting.backtesting_engine_base import BacktestingEngineBase
from hummingbot.strategy_v2.controllers.arbitrage_controller import ArbitrageController, ArbitrageControllerConfig
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.core.data_type.common import TradeType


class ArbitrageBacktestingEngine(BacktestingEngineBase):
    """
    Backtesting engine for arbitrage strategies.
    """
    
    def prepare_market_data(self) -> pd.DataFrame:
        """
        Prepares market data for backtesting by combining data from both exchanges.
        """
        # Get the trading pair and connectors from the controller config
        trading_pair = self.controller.config.trading_pair
        connector1 = self.controller.config.connector1
        connector2 = self.controller.config.connector2
        
        # Get candles data for both connectors
        candles1 = self.controller.market_data_provider.get_candles_df(
            connector_name=connector1,
            trading_pair=trading_pair,
            interval=self.backtesting_resolution
        )
        
        candles2 = self.controller.market_data_provider.get_candles_df(
            connector_name=connector2,
            trading_pair=trading_pair,
            interval=self.backtesting_resolution
        )
        
        # Ensure both dataframes have the same timestamps
        merged_df = pd.merge(
            candles1, candles2,
            on='timestamp',
            suffixes=('_1', '_2')
        )
        
        # Calculate price difference and percentage
        merged_df['price_diff'] = merged_df['close_1'] - merged_df['close_2']
        merged_df['price_diff_pct'] = (merged_df['price_diff'] / merged_df['close_2']) * 100
        
        # Add columns needed for backtesting
        merged_df['close_bt'] = merged_df['close_1']  # Use first exchange as reference
        merged_df['open_bt'] = merged_df['open_1']
        merged_df['high_bt'] = merged_df['high_1']
        merged_df['low_bt'] = merged_df['low_1']
        merged_df['volume_bt'] = merged_df['volume_1']
        
        return merged_df
    
    async def update_state(self, row):
        """
        Updates the state of the controller with the current market data.
        """
        # Update the time in the market data provider
        self.controller.market_data_provider._time = row['timestamp']
        
        # Update processed data in the controller
        await self.update_processed_data(row)
        
        # Simulate execution
        await self.simulate_execution(self.trade_cost)
    
    async def update_processed_data(self, row: pd.Series):
        """
        Updates processed data in the controller with the current price data.
        """
        # Update processed data with current prices from both exchanges
        self.controller.processed_data.update({
            "timestamp": row['timestamp'],
            "price_1": row['close_1'],
            "price_2": row['close_2'],
            "price_diff": row['price_diff'],
            "price_diff_pct": row['price_diff_pct'],
            "signal": self.controller.generate_signal(row['close_1'], row['close_2'])
        })
        
        # Update the features dataframe
        if "features" not in self.controller.processed_data:
            self.controller.processed_data["features"] = pd.DataFrame()
        
        new_row = pd.DataFrame([{
            "timestamp": row['timestamp'],
            "price_1": row['close_1'],
            "price_2": row['close_2'],
            "price_diff": row['price_diff'],
            "price_diff_pct": row['price_diff_pct'],
            "signal": self.controller.processed_data["signal"]
        }])
        
        self.controller.processed_data["features"] = pd.concat([
            self.controller.processed_data["features"], 
            new_row
        ]).reset_index(drop=True)