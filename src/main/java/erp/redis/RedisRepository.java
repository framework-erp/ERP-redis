package erp.redis;

import erp.repository.Repository;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected RedisTemplate<Object, Object> redisTemplate;
    protected RedissonClient redissonClient;

    protected RedisRepository(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient) {
        this(redisTemplate, redissonClient, 30000);
    }

    protected RedisRepository(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(redissonClient, maxLockTime));
        ((RedisStore) store).setEntityType(entityType);
        ((RedisMutexes) mutexes).setEntityType(entityType);
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }

    public RedisRepository(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient, Class<E> entityType) {
        this(redisTemplate, redissonClient, 30000, entityType);
    }

    public RedisRepository(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime, Class<E> entityType) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(redissonClient, maxLockTime), entityType);
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }
}
