import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
import logging
from typing import List, Dict

# Load environment variables
load_dotenv()

class PostgreSQLService:
    """PostgreSQL database service for fetching product data"""
    
    def __init__(self):
        self.host = os.getenv('PSQL_HOST', 'localhost')
        self.port = int(os.getenv('PSQL_PORT', 5432))
        self.username = os.getenv('PSQL_USERNAME')
        self.password = os.getenv('PSQL_PASSWORD')
        self.database = os.getenv('PSQL_DATABASE')
        self.table = os.getenv('PSQL_TABLE')
        self._connection = None
        
    def get_connection(self):
        """Get PostgreSQL connection instance"""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    database=self.database,
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
                logging.info(f"Successfully connected to PostgreSQL at {self.host}:{self.port}")
            except psycopg2.Error as e:
                logging.error(f"Failed to connect to PostgreSQL: {e}")
                raise
            except Exception as e:
                logging.error(f"Error connecting to PostgreSQL: {e}")
                raise
                
        return self._connection
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logging.error(f"PostgreSQL connection test failed: {e}")
            return False
    
    def get_table_info(self) -> Dict:
        """Get information about the products table"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get column information
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (self.table,))
                columns = cursor.fetchall()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) as count FROM {self.table}")
                count_result = cursor.fetchone()
                row_count = count_result['count'] if count_result else 0
                
                return {
                    'table_name': self.table,
                    'total_rows': row_count,
                    'columns': [dict(col) for col in columns]
                }
                
        except Exception as e:
            logging.error(f"Error getting table info: {e}")
            return {'error': str(e)}
    
    def fetch_products(self, limit: int = None, offset: int = 0) -> List[Dict]:
        """
        Fetch images from zando_images table: image, sku, names
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                query = f"SELECT image, sku, names FROM {self.table} ORDER BY sku"
                if limit:
                    query += f" LIMIT {limit}"
                if offset:
                    query += f" OFFSET {offset}"
                cursor.execute(query)
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    d = dict(row)
                    d['sku'] = str(d.get('sku', ''))
                    d['image'] = d.get('image', '')
                    d['names'] = d.get('names', '')
                    result.append(d)
                logging.info(f"Fetched {len(result)} images from PostgreSQL")
                return result
        except Exception as e:
            logging.error(f"Error fetching images: {e}")
            return []
    
    def get_products_count(self) -> int:
        """Get total number of products in the table"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as count FROM {self.table}")
                result = cursor.fetchone()
                return result['count'] if result else 0
        except Exception as e:
            logging.error(f"Error getting products count: {e}")
            return 0
    
    def fetch_products_batch(self, batch_size: int = 100):
        """
        Fetch images in batches for large datasets
        """
        try:
            total_count = self.get_products_count()
            offset = 0
            while offset < total_count:
                batch = self.fetch_products(limit=batch_size, offset=offset)
                if not batch:
                    break
                yield batch
                offset += batch_size
        except Exception as e:
            logging.error(f"Error in batch fetching: {e}")
            return []
    
    def close_connection(self):
        """Close PostgreSQL connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logging.info("PostgreSQL connection closed")

postgres_service = PostgreSQLService()