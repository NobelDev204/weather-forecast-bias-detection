#!/usr/bin/env python3
"""
Bias Analysis Framework
Calculate and analyze forecast errors using spatial consensus
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BiasAnalyzer:
    """Framework for analyzing forecast bias across spatial ensembles"""
    
    def __init__(self, db_path='weather_forecasts.db'):
        self.db_path = db_path
    
    def calculate_spatial_bias(self, city, start_date, end_date, horizon=1):
        """
        Calculate bias using spatial consensus approach
        
        Args:
            city: City identifier
            start_date: Start date for analysis (YYYY-MM-DD)
            end_date: End date for analysis (YYYY-MM-DD)
            horizon: Forecast horizon in days
            
        Returns:
            DataFrame with bias metrics
        """
        conn = sqlite3.connect(self.db_path)
        
        # Query spatial consensus vs actuals
        query = '''
        SELECT 
            sc.city,
            sc.target_date,
            sc.forecast_horizon,
            sc.grid_count,
            sc.consensus_high,
            sc.consensus_low,
            a.high_temp as actual_high,
            a.low_temp as actual_low,
            (sc.consensus_high - a.high_temp) as high_bias,
            (sc.consensus_low - a.low_temp) as low_bias
        FROM spatial_consensus sc
        INNER JOIN actuals a 
            ON sc.city = a.city 
            AND sc.target_date = a.date
        WHERE sc.city = ?
            AND sc.target_date BETWEEN ? AND ?
            AND sc.forecast_horizon = ?
            AND sc.grid_count >= 5
        ORDER BY sc.target_date
        '''
        
        df = pd.read_sql_query(query, conn, params=(city, start_date, end_date, horizon))
        conn.close()
        
        return df
    
    def aggregate_bias_metrics(self, bias_df):
        """
        Calculate aggregate bias statistics
        
        Args:
            bias_df: DataFrame from calculate_spatial_bias
            
        Returns:
            Dictionary of metrics
        """
        if len(bias_df) == 0:
            logger.warning("No data available for bias calculation")
            return {}
        
        metrics = {
            'n_days': len(bias_df),
            'mean_high_bias': bias_df['high_bias'].mean(),
            'std_high_bias': bias_df['high_bias'].std(),
            'mean_low_bias': bias_df['low_bias'].mean(),
            'std_low_bias': bias_df['low_bias'].std(),
            'mae_high': bias_df['high_bias'].abs().mean(),
            'mae_low': bias_df['low_bias'].abs().mean(),
            'rmse_high': np.sqrt((bias_df['high_bias'] ** 2).mean()),
            'rmse_low': np.sqrt((bias_df['low_bias'] ** 2).mean()),
        }
        
        return metrics
    
    def detect_persistent_bias(self, bias_df, threshold=0.5, min_days=30):
        """
        Detect persistent directional bias
        
        Args:
            bias_df: DataFrame from calculate_spatial_bias
            threshold: Minimum mean bias (degrees) to be considered significant
            min_days: Minimum number of days required for analysis
            
        Returns:
            Dictionary indicating if persistent bias detected
        """
        if len(bias_df) < min_days:
            return {
                'sufficient_data': False,
                'high_bias_detected': False,
                'low_bias_detected': False
            }
        
        mean_high_bias = bias_df['high_bias'].mean()
        mean_low_bias = bias_df['low_bias'].mean()
        
        # Simple statistical test: is mean bias > threshold?
        # More sophisticated tests could be added (t-test, etc.)
        
        return {
            'sufficient_data': True,
            'high_bias_detected': abs(mean_high_bias) > threshold,
            'high_bias_direction': 'warm' if mean_high_bias > 0 else 'cold',
            'high_bias_magnitude': abs(mean_high_bias),
            'low_bias_detected': abs(mean_low_bias) > threshold,
            'low_bias_direction': 'warm' if mean_low_bias > 0 else 'cold',
            'low_bias_magnitude': abs(mean_low_bias),
        }
    
    def compare_gridpoint_variance(self, city, target_date):
        """
        Analyze variance across gridpoints for a specific forecast
        
        Args:
            city: City identifier
            target_date: Target date to analyze
            
        Returns:
            DataFrame with per-gridpoint forecasts
        """
        conn = sqlite3.connect(self.db_path)
        
        query = '''
        SELECT 
            grid_id,
            grid_x,
            grid_y,
            high_temp,
            low_temp,
            forecast_horizon
        FROM forecasts
        WHERE city = ?
            AND target_date = ?
        ORDER BY forecast_horizon, grid_id, grid_x, grid_y
        '''
        
        df = pd.read_sql_query(query, conn, params=(city, target_date))
        conn.close()
        
        return df
    
    def export_summary_report(self, city, start_date, end_date, output_path):
        """
        Generate summary report for a city and date range
        
        Args:
            city: City identifier
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            output_path: Path to save CSV report
        """
        results = []
        
        # Analyze each forecast horizon
        for horizon in range(0, 10):
            bias_df = self.calculate_spatial_bias(city, start_date, end_date, horizon)
            
            if len(bias_df) == 0:
                continue
            
            metrics = self.aggregate_bias_metrics(bias_df)
            bias_detection = self.detect_persistent_bias(bias_df)
            
            results.append({
                'city': city,
                'horizon_days': horizon,
                'n_days': metrics.get('n_days', 0),
                'mean_high_bias': metrics.get('mean_high_bias', None),
                'mean_low_bias': metrics.get('mean_low_bias', None),
                'mae_high': metrics.get('mae_high', None),
                'mae_low': metrics.get('mae_low', None),
                'high_bias_detected': bias_detection.get('high_bias_detected', False),
                'low_bias_detected': bias_detection.get('low_bias_detected', False),
            })
        
        # Save results
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_path, index=False)
        logger.info(f"Report saved to {output_path}")
        
        return results_df


def main():
    """Example usage"""
    analyzer = BiasAnalyzer()
    
    # Example: Calculate bias for a city
    city = 'EXAMPLE_CITY'
    start_date = '2025-01-01'
    end_date = '2025-12-31'
    
    # 1-day ahead forecast analysis
    bias_df = analyzer.calculate_spatial_bias(city, start_date, end_date, horizon=1)
    
    if len(bias_df) > 0:
        # Calculate metrics
        metrics = analyzer.aggregate_bias_metrics(bias_df)
        print(f"\nBias Metrics for {city} (1-day ahead):")
        for key, value in metrics.items():
            print(f"  {key}: {value:.3f}" if isinstance(value, float) else f"  {key}: {value}")
        
        # Detect persistent bias
        bias_detection = analyzer.detect_persistent_bias(bias_df)
        print(f"\nBias Detection:")
        for key, value in bias_detection.items():
            print(f"  {key}: {value}")
    else:
        print(f"No data available for {city}")


if __name__ == '__main__':
    main()

