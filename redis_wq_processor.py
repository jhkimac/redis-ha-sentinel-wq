#!/usr/bin/env python

# Based on http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html 
# and the suggestion in the redis documentation for RPOPLPUSH, at 
# http://redis.io/commands/rpoplpush, which suggests how to implement a work-queue.

import redis
from redis.sentinel import Sentinel
import uuid
import random
import hashlib

class RedisWQ(object):
  def __init__(self, name, **kwargs)
  """Default: host='localhost', port=6379, db=0
  name is the name of the work queue (required)
  """
    self._db = redis.StrictRedis(**kwargs)
    self._session = str(uuid.uuid4())
    self._main_q_key = name
    self._processing_q_key = name + ":processing"
    self._lease_key_prefix = name + ":leased_by_session:"
  def sessionID(self):
    return self._session
  def _main_qsize(self):
    return self._db.llen(self._main_q_key)
  def _processing_qsize(self):
    return self._db.llen(self._processing_q_key)
  def empty(self):
    return self._main_qsize() == 0 and self._processing_qsize() == 0
  def push(self, item):
    self._db.rpush(self._main_q_key, item)
  def _itemkey(self, item):
    return hashlib.sha224(item).hexdigest()
  def _lease_exists(self, item):
    return self._db.exists(self._lease_key_prefix + self._itemkey(item))
  def lease(self, lease_secs=60, block=True, timeout=None):
    if self._db.llen(self._main_q_key) == 0:
      return None
    else:
      if block:
        item = self._db.brpoplpush(self._main_q_key, self._processing_q_key, timeout=timeout)
      else:
        item = self._db.rpoplpush(self._main_q_key, self._processing_q_key)
      if item:
        itemkey = self._itemkey(item)
        self._db.setex(self._lease_key_prefix + itemkey, lease_secs, self._session)
      return item
  def complete(self, value):
    self._db.lrem(self._processing_q_key, 0, value)
    itemkey = self._itemkey(value)
    self._db.delete(self._lease_key_prefix + itemkey, self._session)
    return itemkey

class RedisWQProcessor(object):
  def __init__(self, queue_name, service_name, master_group_name, port=6379, sentinel_port=26379):
    self._current_job = None
    self._host_name = host_name
    self._queue_name = queue_name
    self.open_client()
  def get_redis_instances(self, redis_service, master_group_name):
    sentinel = Sentinel([(redis_service, sentinel_port)], socket_timeout=0.1)
    master_host, master_port = sentinel.discover_master(master_group_name)
    slaves = sentinel.discover_slaves(master_group_name)
    slave_host, slave_port = random.choice(slaves)
    return master_host, slave_host
  def open_client(self):
    master_host, slave_host = self.get_redis_instances(self._host_name)
    self.Q = RedisWQ(name=self._queue_name, host=master_host)
    self.slave_Q = RedisWQ(name=self._queue_name, host=slave_host)
  def isEmpty(self):
    max_retries = 3
    num_tries = 0
    while num_tries < max_retries:
      try:
        return self.slave_Q.empty()
      except Exception:
        num_tries += 1
  def getJob(self):
    max_retries = 3
    num_tries = 0
    while num_tries < max_retries:
      try:
        job = self.Q.lease(lease_secs=60, block=True)
        if job is not None:
          return job
        else:
          return None
      except Exception:
        num_tries += 1
    del self.Q
    del self.slave_Q
    self.open_client()
  def releaseJob(self):
    max_retries = 3
    num_tries = 0
    while num_tries < max_retries:
      try:
        self.Q.complete(self._current_job)
        break
      except Exception:
        num_tries += 1
    self._current_job = None