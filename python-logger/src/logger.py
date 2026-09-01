# src/logger/logger.py
from typing import Any, Optional, Dict, Union
import json
import inspect
from datetime import datetime
import threading
from contextlib import contextmanager
import pika
from enum import Enum
import logging
import traceback
from dataclasses import dataclass

class LogLevel(str, Enum):
    """
    Log levels that match our Go implementation for consistency
    across the logging system.
    """
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

@dataclass
class LoggerConfig:
    """
    Configuration for the logger, matching the structure of our Go implementation
    while remaining Pythonic.
    """
    service_name: str
    rabbitmq_url: str
    exchange: str
    routing_key: str
    # Optional configurations
    connection_timeout: int = 5
    retry_attempts: int = 3
    retry_delay: int = 1

class Logger:
    """
    A distributed logger that sends messages to RabbitMQ.
    Maintains compatibility with the Go logger implementation while
    providing a Pythonic interface.
    """
    def __init__(self, config: LoggerConfig):
        """
        Initialize the logger with the given configuration.
        
        Args:
            config: LoggerConfig instance containing all necessary settings
        """
        self.config = config
        self._thread_local = threading.local()
        self._connection = None
        self._channel = None
        
        # Initialize RabbitMQ connection
        self._setup_rabbitmq()

    def _setup_rabbitmq(self) -> None:
        """
        Set up the RabbitMQ connection and channel.
        Implements retry logic for resilience.
        """
        for attempt in range(self.config.retry_attempts):
            try:
                # Create connection
                parameters = pika.URLParameters(self.config.rabbitmq_url)
                parameters.connection_attempts = 3
                parameters.retry_delay = 1
                self._connection = pika.BlockingConnection(parameters)
                
                # Create channel
                self._channel = self._connection.channel()
                
                # Declare exchange
                self._channel.exchange_declare(
                    exchange=self.config.exchange,
                    exchange_type='topic',
                    durable=True
                )
                
                # Setup successful, return
                return
                
            except Exception as e:
                if attempt == self.config.retry_attempts - 1:
                    raise Exception(f"Failed to setup RabbitMQ after {self.config.retry_attempts} attempts: {str(e)}")
                logging.warning(f"RabbitMQ setup attempt {attempt + 1} failed, retrying...")

    def _ensure_connection(self) -> None:
        """
        Ensure the RabbitMQ connection is active and healthy.
        Reconnects if necessary.
        """
        try:
            if not self._connection or self._connection.is_closed:
                self._setup_rabbitmq()
        except Exception as e:
            logging.error(f"Failed to ensure RabbitMQ connection: {str(e)}")
            raise

    def _get_caller_info(self) -> tuple[str, int]:
        """
        Get information about the calling function.
        Matches the file and line information provided by the Go logger.
        """
        stack = inspect.stack()
        # Go up 2 frames to get the actual caller
        caller_frame = stack[2]
        return caller_frame.filename, caller_frame.lineno

    def _log(self, level: LogLevel, message: str, trace_id: Optional[str] = None, 
             data: Optional[Any] = None) -> None:
        """
        Core logging function that formats and sends messages to RabbitMQ.
        
        Args:
            level: LogLevel indicating severity
            message: Main log message
            trace_id: Optional trace ID for request tracking
            data: Optional structured data to include
        """
        try:
            self._ensure_connection()
            
            # Get caller information
            file_name, line_number = self._get_caller_info()
            
            # Construct log message matching Go structure
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level.value,
                "message": message,
                "service_name": self.config.service_name,
                "file": file_name,
                "line": line_number
            }
            
            # Add optional fields
            if trace_id:
                log_entry["trace_id"] = trace_id
            if data is not None:
                # Ensure data is JSON serializable
                log_entry["data"] = json.loads(json.dumps(data))
            
            # Publish to RabbitMQ
            self._channel.basic_publish(
                exchange=self.config.exchange,
                routing_key=self.config.routing_key,
                body=json.dumps(log_entry),
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2  # make message persistent
                )
            )
            
        except Exception as e:
            # Fallback to standard logging if RabbitMQ publishing fails
            logging.error(f"Failed to publish log message: {str(e)}")
            logging.log(
                getattr(logging, level.value),
                message,
                extra={"data": data} if data else None
            )

    # Public logging methods
    def debug(self, message: str, trace_id: Optional[str] = None, data: Optional[Any] = None) -> None:
        """Log a debug message."""
        self._log(LogLevel.DEBUG, message, trace_id, data)

    def info(self, message: str, trace_id: Optional[str] = None, data: Optional[Any] = None) -> None:
        """Log an info message."""
        self._log(LogLevel.INFO, message, trace_id, data)

    def warning(self, message: str, trace_id: Optional[str] = None, data: Optional[Any] = None) -> None:
        """Log a warning message."""
        self._log(LogLevel.WARNING, message, trace_id, data)

    def error(self, message: str, trace_id: Optional[str] = None, data: Optional[Any] = None) -> None:
        """Log an error message."""
        self._log(LogLevel.ERROR, message, trace_id, data)

    def close(self) -> None:
        """
        Cleanly shut down the logger and its connections.
        """
        try:
            if self._channel and not self._channel.is_closed:
                self._channel.close()
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception as e:
            logging.error(f"Error closing logger connections: {str(e)}")

    def __enter__(self):
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup when used as context manager."""
        self.close()