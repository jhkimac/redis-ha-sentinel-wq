# Lightweight Redis Work Queue Processor for use with redis-ha helm chart

Main py file contains two classes: **RedisWQ** and **RedisWQProcessor**

## RedisWQ

Based on [Website](http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html), it is a small lightweight class to handle work queue on redis using RPUSH, RPOPLPUSH and LREM.

## Using redis-ha chart

Since our development included deployment of High-Availability Redis Cluster ([redis-ha helm chart](https://github.com/helm/charts/tree/master/stable/redis-ha)) with Redis Sentinel ([Documentation](https://redis.io/topics/sentinel)), I needed another Processor wrapper to automatically discover the master/slave nodes to access the redis cluster

## RedisWQProcessor

Constructor receives three variables, queue_name, service_name, master_group_name as required.

- queue_name: Name of the work queue
- service_name: Service Name created by the redis-ha chart
- master_group_name: Master Group Name(`redis.masterGroupName`) set in values.yaml

Two variables are set by default:

- sentinel_port(`sentinel.port`): 26379
- port(`redis.port`): 6379

```python
from redis_wq_processor import RedisWQProcessor
processor = RedisWQProcessor('demo-works', 'fiery-ivan-redis-ha', 'trump')
processor.isEmpty()
# True
job = processor.getJob()
# Do some work on job
processor.releaseJob()
```
