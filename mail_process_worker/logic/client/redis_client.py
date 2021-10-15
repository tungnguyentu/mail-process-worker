import redis

from mail_process_worker.setting import RedisConfig

rdb = redis.Redis.from_url(RedisConfig.REDIS_URL)