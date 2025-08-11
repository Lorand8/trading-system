# Sistema di Raccolta Dati per Trading System

# Questo modulo gestisce l’acquisizione dati da diverse fonti

import pandas as pd
import numpy as np
import yfinance as yf
import requests
import websocket
import json
from datetime import datetime, timedelta
import sqlite3
import threading
import time
from typing import Dict, List, Optional
import logging

# Configurazione logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

class DataCollector:
“””
Classe principale per la raccolta dati da multiple fonti
“””

```
def __init__(self, db_path: str = "trading_data.db"):
    self.db_path = db_path
    self.init_database()
    self.active_connections = {}
    
def init_database(self):
    """Inizializza il database SQLite per memorizzare i dati"""
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()
    
    # Tabella per dati OHLCV
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            timeframe TEXT,
            source TEXT,
            UNIQUE(symbol, timestamp, timeframe)
        )
    ''')
    
    # Tabella per news e sentiment
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            title TEXT,
            content TEXT,
            source TEXT,
            sentiment_score REAL,
            symbols TEXT
        )
    ''')
    
    # Tabella per indicatori economici
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS economic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            indicator_name TEXT,
            value REAL,
            country TEXT,
            importance INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database inizializzato correttamente")
```

class HistoricalDataCollector(DataCollector):
“””
Raccoglie dati storici da varie fonti
“””

```
def get_yahoo_data(self, symbol: str, period: str = "1y", interval: str = "1d"):
    """
    Scarica dati storici da Yahoo Finance
    
    Args:
        symbol: Simbolo del titolo (es. "AAPL", "^GSPC")
        period: Periodo (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        interval: Intervallo (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
    """
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval=interval)
        
        if not data.empty:
            self._save_market_data(symbol, data, interval, "yahoo")
            logger.info(f"Scaricati {len(data)} record per {symbol}")
            return data
        else:
            logger.warning(f"Nessun dato trovato per {symbol}")
            return None
            
    except Exception as e:
        logger.error(f"Errore nel download dati {symbol}: {e}")
        return None

def get_multiple_symbols(self, symbols: List[str], **kwargs):
    """Scarica dati per multipli simboli"""
    results = {}
    for symbol in symbols:
        logger.info(f"Scaricando dati per {symbol}...")
        data = self.get_yahoo_data(symbol, **kwargs)
        if data is not None:
            results[symbol] = data
        time.sleep(0.5)  # Pausa per evitare rate limiting
    return results

def _save_market_data(self, symbol: str, data: pd.DataFrame, timeframe: str, source: str):
    """Salva i dati di mercato nel database"""
    conn = sqlite3.connect(self.db_path)
    
    for timestamp, row in data.iterrows():
        try:
            conn.execute('''
                INSERT OR REPLACE INTO market_data 
                (symbol, timestamp, open, high, low, close, volume, timeframe, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, timestamp, row['Open'], row['High'], 
                 row['Low'], row['Close'], row['Volume'], timeframe, source))
        except sqlite3.Error as e:
            logger.error(f"Errore nel salvataggio dati: {e}")
    
    conn.commit()
    conn.close()
```

class RealTimeDataCollector(DataCollector):
“””
Raccoglie dati in tempo reale
“””

```
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.ws_connections = {}
    self.is_running = False

def start_finnhub_stream(self, api_key: str, symbols: List[str]):
    """
    Avvia stream WebSocket con Finnhub (gratuito con limitazioni)
    """
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'data' in data:
                for trade in data['data']:
                    self._process_real_time_trade(trade)
        except Exception as e:
            logger.error(f"Errore processamento messaggio: {e}")
    
    def on_error(ws, error):
        logger.error(f"WebSocket errore: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        logger.info("WebSocket connessione chiusa")
    
    def on_open(ws):
        logger.info("WebSocket connessione aperta")
        # Subscribe ai simboli
        for symbol in symbols:
            ws.send(json.dumps({'type': 'subscribe', 'symbol': symbol}))
    
    websocket_url = f"wss://ws.finnhub.io?token={api_key}"
    ws = websocket.WebSocketApp(websocket_url,
                               on_open=on_open,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close)
    
    self.ws_connections['finnhub'] = ws
    ws.run_forever()

def _process_real_time_trade(self, trade_data):
    """Processa i dati di trade in tempo reale"""
    # Implementa la logica per processare i trade in tempo reale
    logger.info(f"Trade ricevuto: {trade_data}")
```

class NewsCollector(DataCollector):
“””
Raccoglie news e calcola sentiment
“””

```
def get_financial_news(self, api_key: str, query: str = "stock market"):
    """
    Raccoglie news finanziarie (esempio con News API)
    """
    url = f"https://newsapi.org/v2/everything"
    params = {
        'q': query,
        'apiKey': api_key,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 50
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            news_data = response.json()
            self._process_news(news_data['articles'])
            return news_data
        else:
            logger.error(f"Errore API News: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Errore raccolta news: {e}")
        return None

def _process_news(self, articles):
    """Processa gli articoli di news e calcola sentiment"""
    conn = sqlite3.connect(self.db_path)
    
    for article in articles:
        # Sentiment analysis semplice (da migliorare con NLP avanzato)
        sentiment_score = self._calculate_basic_sentiment(article['title'] + " " + (article['description'] or ""))
        
        try:
            conn.execute('''
                INSERT INTO news_data 
                (timestamp, title, content, source, sentiment_score)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')),
                article['title'],
                article['description'],
                article['source']['name'],
                sentiment_score
            ))
        except sqlite3.Error as e:
            logger.error(f"Errore salvataggio news: {e}")
    
    conn.commit()
    conn.close()

def _calculate_basic_sentiment(self, text: str) -> float:
    """
    Calcolo sentiment basilare (da sostituire con modelli NLP)
    Restituisce un valore tra -1 (negativo) e 1 (positivo)
    """
    positive_words = ['up', 'rise', 'gain', 'bull', 'growth', 'profit', 'increase']
    negative_words = ['down', 'fall', 'loss', 'bear', 'decline', 'drop', 'decrease']
    
    text_lower = text.lower()
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    total = pos_count + neg_count
    if total == 0:
        return 0.0
    
    return (pos_count - neg_count) / total
```

class DataValidator:
“””
Valida la qualità dei dati raccolti
“””

```
@staticmethod
def validate_ohlcv(data: pd.DataFrame) -> Dict[str, bool]:
    """Valida dati OHLCV"""
    checks = {
        'no_nulls': not data.isnull().any().any(),
        'high_gte_low': (data['High'] >= data['Low']).all(),
        'high_gte_open': (data['High'] >= data['Open']).all(),
        'high_gte_close': (data['High'] >= data['Close']).all(),
        'low_lte_open': (data['Low'] <= data['Open']).all(),
        'low_lte_close': (data['Low'] <= data['Close']).all(),
        'positive_volume': (data['Volume'] >= 0).all(),
        'no_extreme_moves': (abs(data['Close'].pct_change()) < 0.5).all()  # No moves > 50%
    }
    return checks

@staticmethod
def detect_outliers(series: pd.Series, method: str = 'iqr') -> pd.Series:
    """Rileva outliers in una serie"""
    if method == 'iqr':
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (series < lower_bound) | (series > upper_bound)
    elif method == 'zscore':
        z_scores = abs((series - series.mean()) / series.std())
        return z_scores > 3
```

# Esempio di utilizzo

if **name** == “**main**”:
# Inizializza il collector
collector = HistoricalDataCollector()

```
# Lista di simboli da tracciare
symbols = ["^GSPC", "^DJI", "^IXIC", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

print("Inizio raccolta dati storici...")

# Scarica dati giornalieri ultimo anno
daily_data = collector.get_multiple_symbols(symbols, period="1y", interval="1d")

# Scarica dati orari ultima settimana
hourly_data = collector.get_multiple_symbols(symbols, period="5d", interval="1h")

print("Raccolta dati completata!")

# Valida i dati
validator = DataValidator()
for symbol, data in daily_data.items():
    if data is not None:
        checks = validator.validate_ohlcv(data)
        print(f"{symbol}: {all(checks.values())} - {checks}")

print("Sistema di raccolta dati ready!")
```
