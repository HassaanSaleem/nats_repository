# NATS Repository

A Python library for implementing robust message publishing and consuming using NATS JetStream. This library provides high-level abstractions for working with NATS messaging patterns, including retry logic, dead-letter handling, and concurrent message processing.

## Features

- 🚀 **High-performance messaging** with NATS JetStream
- 🔄 **Automatic retry logic** with configurable backoff intervals
- 💀 **Dead-letter queue support** for failed messages
- ⚡ **Concurrent message processing** with configurable concurrency limits
- 🔌 **Flexible connection management** with authentication support
- 📊 **Stream management** with automatic creation and configuration
- 🛡️ **Robust error handling** and graceful shutdown
- 🔧 **Multiprocessing support** for scalable applications

## Installation

### Prerequisites

- Python 3.11 or higher
- NATS server with JetStream enabled

### Install from source

```bash
git clone <repository-url>
cd nats_repository
pip install -e .
```

### Install dependencies

```bash
pip install .
```

## Quick Start


### Basic Publisher Example

```python
import asyncio
import json
from nats_repository import ConnectionParameters
from nats_repository import AsyncIOPublisher

async def main():
    # Configure connection
    connection_params = ConnectionParameters(
        servers=['nats://localhost:4222'],
        username='YOUR_USERNAME',  # Optional
        password='YOUR_PASSWORD'   # Optional
    )
    
    # Create publisher
    publisher = AsyncIOPublisher(
        connection_parameters=connection_params,
        stream_name='my_stream',
        subjects=['my.subject']
    )
    
    # Start publisher
    await publisher.start()
    
    # Publish messages
    for i in range(10):
        message = {'id': i, 'data': f'message_{i}'}
        await publisher.publish(
            payload=json.dumps(message),
            subject='my.subject'
        )
    
    # Stop publisher
    await publisher.stop()

if __name__ == '__main__':
    asyncio.run(main())
```

### Basic Consumer Example

```python
import asyncio
import json
from nats_repository import ConnectionParameters
from nats_repository import Consumer

async def process_message(event, subject, retries):
    """Custom message processing function"""
    print(f"Processing: {event} from {subject} (attempt: {retries})")
    # Add your business logic here
    
async def main():
    # Configure connection
    connection_params = ConnectionParameters(
        servers=['nats://localhost:4222'],
        username='YOUR_USERNAME',
        password='YOUR_PASSWORD'
    )
    
    # Create consumer
    consumer = Consumer(
        connection_parameters=connection_params,
        stream_name='my_stream',
        subjects=['my.subject'],
        max_retries=3,
        backoff_intervals=[1, 5, 10],  # Retry after 1s, 5s, 10s
        dead_letter_subject='my.subject.dlq',
        concurrency=10
    )
    
    # Set custom message handler
    consumer.callback = process_message
    
    # Start consuming
    await consumer.start_task()

if __name__ == '__main__':
    asyncio.run(main())
```

## API Reference

### ConnectionParameters

Configuration class for NATS connection settings.

```python
ConnectionParameters(
    servers: List[str] = ['nats://localhost:4222'],
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    max_reconnect_attempts: int = 60,
    reconnect_time_wait: int = 2
)
```

**Parameters:**
- `servers`: List of NATS server URLs
- `username`: Optional username for authentication
- `password`: Optional password for authentication
- `token`: Optional token for authentication
- `max_reconnect_attempts`: Maximum reconnection attempts
- `reconnect_time_wait`: Wait time between reconnection attempts (seconds)

### AsyncIOPublisher

High-performance asynchronous message publisher.

```python
AsyncIOPublisher(
    connection_parameters: ConnectionParameters,
    stream_name: str,
    subjects: List[str],
    max_retries: int = 5,
    backoff_intervals: List[float] = None,
    concurrency: int = 100,
    msg_retention: str = 'workqueue',
    msg_max_count: int = 1_000_000_000,
    msg_max_size: int = 1
)
```

**Parameters:**
- `connection_parameters`: Connection configuration
- `stream_name`: JetStream name
- `subjects`: List of subjects to publish to
- `max_retries`: Maximum retry attempts for failed publishes
- `backoff_intervals`: Custom backoff intervals for retries
- `concurrency`: Maximum concurrent publishing operations
- `msg_retention`: Message retention policy ('workqueue', 'limits', 'interest')
- `msg_max_count`: Maximum messages in stream
- `msg_max_size`: Maximum stream size in GB

**Methods:**
- `async start()`: Initialize and connect to NATS
- `async publish(payload: str, subject: str)`: Publish a message
- `async stop()`: Gracefully shutdown the publisher

### Consumer

Robust message consumer with retry and dead-letter support.

```python
Consumer(
    connection_parameters: ConnectionParameters,
    stream_name: str,
    subjects: List[str],
    max_retries: int = 5,
    backoff_intervals: List[float] = None,
    dead_letter_subject: str = None,
    concurrency: int = 100,
    fetch_timeout: int = 1,
    msg_retention: str = 'workqueue',
    msg_max_count: int = 1_000_000_000,
    msg_max_size: int = 1
)
```

**Parameters:**
- `connection_parameters`: Connection configuration
- `stream_name`: JetStream name to consume from
- `subjects`: List of subjects to subscribe to
- `max_retries`: Maximum retry attempts before dead-lettering
- `backoff_intervals`: Retry backoff intervals in seconds
- `dead_letter_subject`: Subject for failed messages
- `concurrency`: Maximum concurrent message processing
- `fetch_timeout`: Message fetch timeout in seconds
- `msg_retention`: Message retention policy
- `msg_max_count`: Maximum messages in stream
- `msg_max_size`: Maximum stream size in GB

**Methods:**
- `async start_task()`: Start consuming messages
- `async stop()`: Gracefully shutdown the consumer
- `callback`: Set custom message processing function

### Publisher (Multiprocessing)

Process-based publisher for high-throughput scenarios.

```python
Publisher(
    connection_parameters: ConnectionParameters,
    stream_name: str,
    subjects: List[str],
    max_retries: int = 5,
    backoff_intervals: List[float] = None,
    concurrency: int = 100,
    msg_retention: str = 'workqueue',
    msg_max_count: int = 1_000_000_000,
    msg_max_size: int = 1
)
```

**Methods:**
- `start()`: Start the publisher process
- `publish(payload: str, subject: str)`: Add message to publish queue
- `stop()`: Stop the publisher process
- `join()`: Wait for process to complete

## Configuration Options

### Message Retention Policies

- **workqueue**: Messages are removed after acknowledgment
- **limits**: Messages are retained based on limits (count/size/age)
- **interest**: Messages are retained while there are active subscriptions

### Authentication

The library supports multiple authentication methods:

```python
# Username/Password
connection_params = ConnectionParameters(
    servers=['nats://localhost:4222'],
    username='myuser',
    password='mypassword'
)

# Token-based
connection_params = ConnectionParameters(
    servers=['nats://localhost:4222'],
    token='mytoken'
)

# No authentication
connection_params = ConnectionParameters(
    servers=['nats://localhost:4222']
)
```

## Examples

Check the `examples/` directory for complete working examples:

- `examples/publisher.py`: Benchmark publisher with performance metrics
- `examples/consumer.py`: Benchmark consumer with processing statistics

### Running Examples

```bash
# Terminal 1 - Start consumer
python examples/consumer.py

# Terminal 2 - Start publisher
python examples/publisher.py
```

## Performance Considerations

### Publisher Performance

- Use `AsyncIOPublisher` for most use cases
- Use `Publisher` (multiprocessing) for CPU-intensive message generation
- Adjust `concurrency` based on your system resources
- Monitor `_acked_count` and `_reject_count` for performance metrics

### Consumer Performance

- Set appropriate `concurrency` levels (typically 10-100)
- Use `fetch_timeout` to balance latency and resource usage
- Configure `backoff_intervals` based on your retry strategy
- Monitor message processing rates and adjust accordingly

### Stream Configuration

- Choose appropriate `msg_retention` policy for your use case
- Set realistic `msg_max_count` and `msg_max_size` limits
- Consider storage type (file vs memory) based on durability needs

## Error Handling

The library provides comprehensive error handling:

- **Connection errors**: Automatic reconnection with exponential backoff
- **Publishing errors**: Retry logic with configurable intervals
- **Processing errors**: Message redelivery and dead-letter handling
- **Graceful shutdown**: Proper cleanup of resources and connections

## Monitoring and Observability

### Built-in Metrics

Publishers track:
- `_acked_count`: Successfully published messages
- `_reject_count`: Failed publishing attempts

Consumers provide:
- Message processing rates
- Retry attempts and failures
- Dead-letter queue statistics

### Logging

The library uses Python's standard logging module. Configure logging levels:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('nats_repository')
```

## Best Practices

1. **Connection Management**
   - Reuse connections when possible
   - Configure appropriate reconnection settings
   - Handle connection failures gracefully

2. **Message Design**
   - Keep messages reasonably sized
   - Use JSON for structured data
   - Include correlation IDs for tracing

3. **Error Handling**
   - Implement idempotent message processing
   - Use dead-letter queues for poison messages
   - Log errors with sufficient context

4. **Performance**
   - Tune concurrency based on workload
   - Monitor queue depths and processing rates
   - Use appropriate retention policies

5. **Testing**
   - Test with various failure scenarios
   - Validate retry and dead-letter behavior
   - Load test with realistic message volumes

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure NATS server is running
   - Check server address and port
   - Verify authentication credentials

2. **Stream Not Found**
   - Streams are created automatically
   - Check subject patterns and permissions
   - Verify JetStream is enabled

3. **Messages Not Consuming**
   - Check consumer configuration
   - Verify subject subscriptions
   - Monitor consumer logs for errors

4. **High Memory Usage**
   - Adjust queue sizes and concurrency
   - Check message retention settings
   - Monitor stream storage usage

### Debug Mode

Enable debug logging for detailed information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE.txt file for details.

## Support

For questions, issues, or contributions, please contact:
- **Author**: Syed Hassaan Saleem
- **Email**: saleemhassaan94@gmail.com
