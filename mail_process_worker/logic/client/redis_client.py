import redis

from mail_process_worker.setting import RedisConfig

rdb = redis.Redis(url=RedisConfig.REDIS_URL)