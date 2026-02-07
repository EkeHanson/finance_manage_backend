import logging
import time
from .storage import get_storage_service

logger = logging.getLogger(__name__)

def upload_file_dynamic(file_obj, file_name, content_type="application/octet-stream", storage_type=None):
    """
    Upload files using the selected storage backend.
    storage_type: 'supabase', 's3', 'azure', or 'local'
    """
    # logger.info("[upload_file_dynamic] Using bucket: %s", storage_service.bucket)
    # logger.info("[upload_file_dynamic] Uploading to path: %s", file_name)

    storage_service = get_storage_service(storage_type)
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Reset file pointer before each attempt
            if hasattr(file_obj, 'seek'):
                file_obj.seek(0)
                
            success = storage_service.upload_file(file_obj, file_name, content_type)
            if success:
                return storage_service.get_public_url(file_name)
            else:
                raise Exception(f"Failed to upload {file_name}")
        except Exception as e:
            error_msg = str(e)
            # Retry on SSL/connection errors
            if "SSL" in error_msg or "EOF occurred" in error_msg or "ConnectError" in error_msg:
                if attempt < max_retries - 1:
                    logger.warning(f"Transient error uploading {file_name} (attempt {attempt + 1}/{max_retries}): {error_msg}. Retrying...")
                    time.sleep(1 * (attempt + 1))  # Exponential backoff: 1s, 2s
                    continue
            # If not a retryable error or max retries reached, raise
            logger.error(f"Error uploading file {file_name}: {error_msg}")
            raise





