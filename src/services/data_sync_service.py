import logging
from typing import Dict
from src.services import postgres_service
import time

class DataSyncService:
    """Service to sync data from PostgreSQL to Redis search index"""
    
    def __init__(self):
        self.postgres = postgres_service
   
    
    def sync_all_products(self, batch_size: int = 100, clear_existing: bool = True) -> Dict:
        """
        Sync all products from PostgreSQL to Redis search index
        
        Args:
            batch_size: Number of products to process in each batch
            clear_existing: Whether to clear existing Redis data first
            
        Returns:
            Dictionary with sync results
        """
        start_time = time.time()
        results = {
            'success': False,
            'total_products': 0,
            'indexed_products': 0,
            'failed_products': 0,
            'errors': [],
            'duration_seconds': 0
        }
        
        try:
            if not self.postgres.test_connection():
                results['errors'].append('PostgreSQL connection failed')
                return results
            
            if clear_existing:
                logging.info("Clearing existing Redis search data...")
                self.redis_search.clear_all_data()
            
            total_count = self.postgres.get_products_count()
            results['total_products'] = total_count
            if total_count == 0:
                results['errors'].append('No images found in PostgreSQL table')
                return results
            logging.info(f"Starting sync of {total_count} images...")
            indexed_count = 0
            failed_count = 0
            for batch_num, batch in enumerate(self.postgres.fetch_products_batch(batch_size), 1):
                logging.info(f"Processing batch {batch_num} ({len(batch)} images)")
                for product in batch:
                    try:
                        search_doc = self._convert_product_to_search_doc(product)
                        success = self.redis_search.index_document(
                            doc_id=search_doc['id'],
                            name=search_doc['name'],
                            price=search_doc['price'],
                            image=search_doc['image'],
                            metadata=search_doc['metadata']
                        )
                        if success:
                            indexed_count += 1
                        else:
                            failed_count += 1
                            logging.warning(f"Failed to index image {product.get('name')}")
                    except Exception as e:
                        failed_count += 1
                        error_msg = f"Error processing image {product.get('name', 'unknown')}: {str(e)}"
                        logging.error(error_msg)
                        results['errors'].append(error_msg)
                if batch_num % 10 == 0:
                    logging.info(f"Progress: {indexed_count}/{total_count} images indexed")
            results['indexed_products'] = indexed_count
            results['failed_products'] = failed_count
            results['success'] = indexed_count > 0
            end_time = time.time()
            results['duration_seconds'] = round(end_time - start_time, 2)
            logging.info(f"Sync completed: {indexed_count} indexed, {failed_count} failed, {results['duration_seconds']}s")
            
        except Exception as e:
            error_msg = f"Sync failed with error: {str(e)}"
            logging.error(error_msg)
            results['errors'].append(error_msg)
        
        return results
    
    def _convert_product_to_search_doc(self, product: Dict) -> Dict:
        """
        Convert a product record to a search document format for image search.
        """
        id = product.get('id','')
        image = product.get('image', '')
        name = product.get('name', '')
        price = product.get('price', '')
        metadata = product.get('metadata', '')
        return {
            'id': id,
            'name': name,
            'price': price,
            'image':image,
            'metadata': metadata
        }
    
    def sync_single_product(self, product_id: str) -> Dict:
        """
        Sync a single product by ID
        
        Args:
            product_id: Product ID to sync
            
        Returns:
            Sync result dictionary
        """
        try:
            products = self.postgres.fetch_products(limit=1, offset=0)
            
            for product in products:
                if str(product['id']) == str(product_id):
                    search_doc = self._convert_product_to_search_doc(product)
                    
                    success = self.redis_search.index_document(
                        doc_id=search_doc['id'],
                        title=search_doc['title'],
                        content=search_doc['content'],
                        tags=search_doc['tags'],
                        metadata=search_doc['metadata']
                    )
                    
                    return {
                        'success': success,
                        'product_id': product_id,
                        'message': f"Product {product_id} {'indexed successfully' if success else 'failed to index'}"
                    }
            
            return {
                'success': False,
                'product_id': product_id,
                'message': f"Product {product_id} not found in database"
            }
            
        except Exception as e:
            return {
                'success': False,
                'product_id': product_id,
                'message': f"Error syncing product {product_id}: {str(e)}"
            }
    
    def get_sync_status(self) -> Dict:
        """Get current sync status and statistics"""
        try:
            postgres_count = self.postgres.get_products_count()
            postgres_info = self.postgres.get_table_info()
            
            redis_stats = self.redis_search.get_stats()
            
            return {
                'postgresql': {
                    'connected': self.postgres.test_connection(),
                    'total_products': postgres_count,
                    'table_info': postgres_info
                },
                'redis': redis_stats,
                'sync_health': {
                    'redis_vs_postgres': {
                        'postgres_count': postgres_count,
                        'redis_count': redis_stats.get('documents_count', 0),
                        'difference': postgres_count - redis_stats.get('documents_count', 0)
                    }
                }
            }
            
        except Exception as e:
            return {
                'error': f"Error getting sync status: {str(e)}"
            }

data_sync_service = DataSyncService()
