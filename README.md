## aiostasd
an asyncio-based client for send metric to `StatsD`, `Graphite.carbon` and `DogStatsD`.

## Installation
```Bash
pip install aiostatsd
```
## Usage
### Usage Client
Create connection and send gauge metric.
aiostatsd client will automatically send messages in the background when the loop is running
```Python
import asyncio
from aio_statsd import StatsdClient


loop = asyncio.get_event_loop()
client = StatsdClient()
loop.run_until_complete(client.connect())
client.gauge('test.key', 1)
loop.run_forever()
```
Create Connection Pool and use
`max_size=5:` The maximum number of connections in the connection pool
```Python
import asyncio
from aio_statsd import StatsdClient


loop = asyncio.get_event_loop()
client = StatsdClient()
loop.run_until_complete(client.create_pool(max_size=5))
client.gauge('test.key', 1)
loop.run_forever()
```
Use context manager(Now, not support connection pool)
```Python
import asyncio
from aio_statsd import StatsdClient


async def main():
    async with StatsdClient() as client:
        client.gauge('test.key', 1)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```
### Client param
- host: default value 'localhost', Statsd Server ip
- port: default value 8125, Statsd Server port
- protocol: default value ProtocolFlag.udp, Transport Layer Prrotocol, Select Tcp:`ProtocolFlag.udp` or Udp:`ProtocolFlag.tcp` 
- timeout: default value 0, send msg timeout, if timeout==0, not enable timeout
- debug: default value False, enable debug
- close_timeout: default value 5, Within a few seconds after the client is closed, continue to send messages which in the queue
- create_timeout: default value 5, Createe connection timeout
- read_timeout: default value 0.5, read messages from queue timeout
- sample_rate(Use in StatsD Client, DogStatsD Client): default value 1, use sample rate in Statsd or DogStatsD
### send metric
```Python
import asyncio
from aio_statsd import StatsdClient


async def main():
    async with StatsdClient() as client:
        client.gauge('test.key', 1)
        client.counter('test.key', 1)
        client.sets('test.key', 1)
        client.timer('test.key', 1)
        with client.timeit('test'):
            pass  # run your code
        
        # all metric support sample rate
        client.gauge('test1.key', 1, sample_rate=0.5)
        
        # mutli metric support(not support sample rate, the sample rate will always be set to 1)
        from aio_statsd import StatsdProtocol
        metric = StatsdProtocol()   
        metric.gauge('test2.key', 1)
        metric.sets('test2.key', 1)
        client.send_statsd(metric)     

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```
### Other Client
#### Graphite(carbon)
```python
import asyncio
from aio_statsd import GraphiteClient


loop = asyncio.get_event_loop()
client = GraphiteClient()
loop.run_until_complete(client.connect())
client.send_graphite('test.key', 1) # Multiple clients timestamp interval synchronization
loop.run_forever()
```
#### DogStatsD
>Note: Not tested in production
```python
import asyncio
from aio_statsd import DogStatsdClient


async def main():
    async with DogStatsdClient() as client:
        client.gauge('test.key', 1)
        client.distribution('test.key', 1)
        client.increment('test.key',1)
        client.histogram('test.key', 1)
        client.timer('test.key', 1)
        with client.timeit('test'):
            pass  # run your code
        
        # all metric support sample rate and DogStatsD tag
        client.gauge('test1.key', 1, sample_rate=0.5, tag_dict={'tag': 'tag1'})
        
        # mutli metric support(
        #   DogStatsdProtocol will store the message in its own queue and
        #   DogStatsDClient traverses to read DogStatsdProtocol's message and send it
        # )
        from aio_statsd import DogStatsdProtocol
        metric = DogStatsdProtocol()   
        metric.gauge('test2.key', 1, tag_dict={'tag': 'tag1'})
        metric.histogram('test2.key', 1)
        client.send_dog_statsd(metric, sample_rate=0.5)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```
### Use in web frameworks
[fastapi example](https://github.com/so1n/fastapi-tools/blob/master/example/statsd_middleware.py)