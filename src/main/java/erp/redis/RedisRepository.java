package erp.redis;

import erp.repository.Repository;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        this(redisTemplate, redissonClient, Long.MAX_VALUE);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        super(new RedisStore<>(redisTemplate), new RedissonMutexes<>(redissonClient, maxLockTime));
    }
}
