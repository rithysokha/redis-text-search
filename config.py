import redis
import os
from dotenv import load_dotenv
import logging

load_dotenv()

class RedisConfig:
    """Redis configuration and connection management"""
    
    def __init__(self):
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', 6379))
        self.password = os.getenv('REDIS_PASSWORD')
        self.db = int(os.getenv('REDIS_DB', 0))
        self._connection = None
        
    def get_connection(self):
        """Get Redis connection instance"""
        if self._connection is None:
            try:
                self._connection = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    retry_on_timeout=True
                )
                self._connection.ping()
                logging.info(f"Successfully connected to Redis at {self.host}:{self.port}")
            except redis.ConnectionError as e:
                logging.error(f"Failed to connect to Redis: {e}")
                raise
            except Exception as e:
                logging.error(f"Error connecting to Redis: {e}")
                raise
                
        return self._connection
    
    def test_connection(self):
        """Test Redis connection"""
        try:
            conn = self.get_connection()
            conn.ping()
            return True
        except Exception as e:
            logging.error(f"Redis connection test failed: {e}")
            return False

redis_config = RedisConfig()
