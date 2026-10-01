
import numpy as np
import pandas as pd

# تنظیم سید برای تکرارپذیری
np.random.seed(42)

def generate_crypto_data(symbol, days=300):
    dates = pd.date_range(end=pd.Timestamp.now(), periods=days, freq='D')
    # تولید مسیر تصادفی قیمت با روند و نوسان
    returns = np.random.normal(0.001, 0.03, days)
    price_0 = 50000 if symbol == 'BTC' else (3000 if symbol == 'ETH' else 150)
    prices = price_0 * np.exp(np.cumsum(returns))
    
    high = prices * (1 + np.abs(np.random.normal(0, 0.015, days)))
    low = prices * (1 - np.abs(np.random.normal(0, 0.015, days)))
    close = prices
    volume = np.random.lognormal(10, 1, days)
    
    df = pd.DataFrame({'date': dates, 'open': prices * (1 + np.random.normal(0, 0.005, days)), 
                       'high': high, 'low': low, 'close': close, 'volume': volume})
    df.set_index('date', inplace=True)
    return df

# اندیکاتورها
def calculate_indicators(df):
    df = df.copy()
    # 1. RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # 2. MACD
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    
    # 3. SMA Crossover (20 vs 50)
    df['SMA20'] = df['close'].rolling(20).mean()
    df['SMA50'] = df['close'].rolling(50).mean()
    
    # 4. Bollinger Bands
    df['BB_mid'] = df['close'].rolling(20).mean()
    df['BB_std'] = df['close'].rolling(20).std()
    df['BB_upper'] = df['BB_mid'] + 2 * df['BB_std']
    df['BB_lower'] = df['BB_mid'] - 2 * df['BB_std']
    
    # 5. Stochastic Oscillator
    low14 = df['low'].rolling(14).min()
    high14 = df['high'].rolling(14).max()
    df['Stoch_K'] = 100 * ((df['close'] - low14) / (high14 - low14 + 1e-9))
    df['Stoch_D'] = df['Stoch_K'].rolling(3).mean()
    
    return df

# بک‌تست هر اندیکاتور برای استخراج ضریب صحت و سودآوری
def evaluate_indicators(df, horizon_days=7):
    # محاسبه بازده آتی N روزه
    future_return = df['close'].shift(-horizon_days) / df['close'] - 1.0
    
    # سیگنال‌های هر اندیکاتور (+1: خرید، -1: فروش، 0: خنثی)
    sig_rsi = np.where(df['RSI'] < 30, 1, np.where(df['RSI'] > 70, -1, 0))
    sig_macd = np.where(df['MACD'] > df['MACD_signal'], 1, -1)
    sig_sma = np.where(df['SMA20'] > df['SMA50'], 1, -1)
    sig_bb = np.where(df['close'] < df['BB_lower'], 1, np.where(df['close'] > df['BB_upper'], -1, 0))
    sig_stoch = np.where((df['Stoch_K'] < 20) & (df['Stoch_K'] > df['Stoch_D']), 1, 
                         np.where((df['Stoch_K'] > 80) & (df['Stoch_K'] < df['Stoch_D']), -1, 0))
    
    indicators = {
        'RSI (14)': sig_rsi,
        'MACD (12,26,9)': sig_macd,
        'SMA Crossover (20/50)': sig_sma,
        'Bollinger Bands': sig_bb,
        'Stochastic (14,3)': sig_stoch
    }
    
    metrics = {}
    valid_idx = ~np.isnan(future_return)
    
    for name, sig in indicators.items():
        trade_mask = (sig != 0) & valid_idx
        if trade_mask.sum() > 10:
            trades_ret = future_return[trade_mask] * sig[trade_mask]
            win_rate = (trades_ret > 0).mean() * 100
            avg_return = trades_ret.mean() * 100
            profit_factor = trades_ret[trades_ret > 0].sum() / (np.abs(trades_ret[trades_ret < 0].sum()) + 1e-9)
            
            # فرمول ضریب صحت (Reliability Weight): ترکیبی از نرخ برد و سود میانگین
            weight = max(0.1, (win_rate / 50.0) * (1.0 + max(0, avg_return / 10.0)))
            current_signal = sig[-1]
            
            metrics[name] = {
                'current_signal': int(current_signal),
                'signal_desc': 'خرید (BUY)' if current_signal == 1 else ('فروش (SELL)' if current_signal == -1 else 'خنثی (HOLD)'),
                'win_rate_pct': round(float(win_rate), 1),
                'avg_profit_pct': round(float(avg_return), 2),
                'profit_factor': round(float(profit_factor), 2),
                'weight': round(float(weight), 2),
                'trades_count': int(trade_mask.sum())
            }
        else:
            metrics[name] = {
                'current_signal': int(sig[-1]),
                'signal_desc': 'خنثی (HOLD)',
                'win_rate_pct': 50.0,
                'avg_profit_pct': 0.0,
                'profit_factor': 1.0,
                'weight': 1.0,
                'trades_count': int(trade_mask.sum())
            }
            
    # محاسبه میانگین موزون سیگنال کلی
    total_weight = sum(m['weight'] for m in metrics.values())
    weighted_score = sum(m['current_signal'] * m['weight'] for m in metrics.values()) / total_weight
    expected_profit = sum(m['avg_profit_pct'] * m['weight'] for m in metrics.values() if m['current_signal'] != 0) / (sum(m['weight'] for m in metrics.values() if m['current_signal'] != 0) + 1e-9)
    
    consensus = 'خرید قوی (STRONG BUY)' if weighted_score > 0.4 else ('خرید (BUY)' if weighted_score > 0.1 else ('فروش قوی (STRONG SELL)' if weighted_score < -0.4 else ('فروش (SELL)' if weighted_score < -0.1 else 'خنثی (NEUTRAL)')))
    
    return {
        'indicators': metrics,
        'overall_score': round(float(weighted_score), 3),
        'consensus': consensus,
        'confidence_pct': round(abs(float(weighted_score)) * 100, 1),
        'expected_profit_horizon_7d': round(float(expected_profit), 2)
    }

results = {}
for sym in ['BTC', 'ETH', 'SOL']:
    df = generate_crypto_data(sym)
    df = calculate_indicators(df)
    results[sym] = evaluate_indicators(df, horizon_days=7)

print(json.dumps(results, indent=2, ensure_ascii=False))
