# examples/basic_usage.py
from logger import Logger, LoggerConfig
import uuid
from contextlib import contextmanager

@contextmanager
def get_logger():
    """Context manager for logger initialization and cleanup."""
    config = LoggerConfig(
        service_name="example-python-service",
        rabbitmq_url="amqp://localhost:5672",
        exchange="logs",
        routing_key="service.logs"
    )
    
    logger = Logger(config)
    try:
        yield logger
    finally:
        logger.close()

def process_user_request(user_id: str):
    """Example function showing logger usage in a request context."""
    # Generate a trace ID for request tracking
    trace_id = str(uuid.uuid4())
    
    with get_logger() as logger:
        # Log the start of request processing
        logger.info(
            message=f"Processing request for user {user_id}",
            trace_id=trace_id,
            data={"user_id": user_id}
        )
        
        try:
            # Simulate some processing
            result = {"status": "success", "user_id": user_id}
            
            # Log successful processing
            logger.info(
                message="Request processed successfully",
                trace_id=trace_id,
                data=result
            )
            
            return result
            
        except Exception as e:
            # Log any errors with full context
            logger.error(
                message=f"Error processing request: {str(e)}",
                trace_id=trace_id,
                data={
                    "user_id": user_id,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
            )
            raise

if __name__ == "__main__":
    # Example usage
    try:
        result = process_user_request("user123")
        print(f"Request processed: {result}")
    except Exception as e:
        print(f"Error: {e}")