## aiostasd
an asyncio-based client for send metric to `StatsD`, `Graphite.carbon`, `TelegrafStatsD` and `DogStatsD`.

## Installation
```Bash
pip install aio_statsd
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
Use context manager
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
- create_timeout: default value 5, Create connection timeout
- max_len: default value 10000, deque length
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
#### TelegrafStatsd
>Note: Not tested in production
```python
import asyncio

from aio_statsd import TelegrafStatsdClient


async def main():
    async with TelegrafStatsdClient() as client:
        client.gauge('test.key', 1)
        client.distribution('test.key', 1)
        client.increment('test.key',1)
        client.histogram('test.key', 1)
        client.timer('test.key', 1)
        with client.timeit('test'):
            pass  # run your code
        
        # all metric support sample rate and TelegrafStatsd tag
        client.gauge('test1.key', 1, sample_rate=0.5, tag_dict={'tag': 'tag1'})
        
        # mutli metric support(
        #   TelegrafStatsdProtocol will store the message in its own queue and
        #   TelegrafStatsDClient traverses to read TelegrafStatsdProtocol's message and send it
        # )
        from aio_statsd import TelegrafStatsdProtocol 
        metric = TelegrafStatsdProtocol()   
        metric.gauge('test2.key', 1, tag_dict={'tag': 'tag1'})
        metric.histogram('test2.key', 1)
        client.send_telegraf_statsd(metric, sample_rate=0.5)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```
#### Telegraf
>Note: Not tested in production
```Python
import asyncio

from aio_statsd import TelegrafClient

async def main():
    async with TelegrafClient() as client:
        client.send_telegraf('test.key', {"field1": 100}, user_server_time=True)
```
### Use in web frameworks
[fast_tools example](https://github.com/so1n/fast-tools/blob/master/example/statsd_middleware.py)