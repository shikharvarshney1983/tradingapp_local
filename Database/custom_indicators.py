import pandas as pd

def calculate_vstop(prices, atr_period=14, multiplier=2.0, lookback_period=1):
    """
    Calculate the VStop trailing stop levels using ATR for a given DataFrame of price data.

    Parameters:
        - prices (pd.DataFrame): A DataFrame containing historical price data with columns 'High', 'Low', and 'Close'.
        - atr_period (int): The period for calculating Average True Range (ATR). Default is 14.
        - multiplier (float): The multiplier for calculating the trailing stop. Default is 2.0.
        - lookback_period (int): The number of periods to look back for uptrend and downtrend determination. Default is 1.

    Returns:
        - pd.Series: A pandas Series representing the VStop trailing stop levels.
    """
    if not isinstance(prices, pd.DataFrame):
        raise ValueError("prices should be a pandas DataFrame")
    
    if atr_period <= 0 or lookback_period <= 0 or multiplier <= 0:
        raise ValueError("Parameters should be positive values")

    if 'High' not in prices.columns or 'Low' not in prices.columns or 'Close' not in prices.columns:
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns")

    # Calculate ATR
    atr = prices["ATR"]
    fHigh = 0
    fLow = 0

    # Calculate VStop
    vstop = pd.Series(index=prices.index)

    for i in range(len(prices)):
        if i < atr_period:
            vstop.iloc[i] = prices['Close'].iloc[i] + (multiplier * atr.iloc[i])
        else:
            high_period = prices['Close'].iloc[i - lookback_period:i + 1].max()
            low_period = prices['Close'].iloc[i - lookback_period:i + 1].min()

            if prices['High'].iloc[i - 1] > vstop.iloc[i - 1]:
                if fHigh < 1:
                    vstop.iloc[i] = high_period - multiplier * atr.iloc[i]
                else:
                    vstop.iloc[i] = max(vstop.iloc[i - 1], high_period - multiplier * atr.iloc[i])
                fHigh += 1
                fLow = 0
            else:
                if fLow < 1:
                    vstop.iloc[i] = low_period + multiplier * atr.iloc[i]
                else:
                    vstop.iloc[i] = min(vstop.iloc[i - 1], low_period + multiplier * atr.iloc[i])
                fLow += 1
                fHigh = 0
    # print(vstop)
    return vstop

def calculate_relative_strength(prices, comparative_df, period=50 ):
    """
    Calculate the Relative Strength against NIFTY 50.

    Returns:
        - pd.Series: A pandas Series representing the RS levels.
    """
    if not isinstance(prices, pd.DataFrame):
        raise ValueError("prices should be a pandas DataFrame")
    
    if period <= 0:
        raise ValueError("Parameters should be positive values")

    if 'Close' not in prices.columns:
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns")

    # Merge Dataframes
    merged_df =  pd.merge(comparative_df ,prices, how = 'inner', on = ['Date'])
    merged_df.set_index('Date', inplace=True)
    # Create RS Dataframe
    rs = pd.Series(index=merged_df.index)

    for i in range(len(merged_df)):
        # print(merged_df[i]['Date'].values[0])
        if i < period:
            rs.iloc[i] = None
        else:
            rs.iloc[i] = round((merged_df['Close_y'].iloc[i] /merged_df['Close_y'].iloc[i - period])/(merged_df['Close_x'].iloc[i] /merged_df['Close_x'].iloc[i - period]) - 1, 2) 

    return rs    