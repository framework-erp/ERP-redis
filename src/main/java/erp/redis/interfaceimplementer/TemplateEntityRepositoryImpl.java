package erp.redis.interfaceimplementer;

import erp.redis.RedisRepository;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

public class TemplateEntityRepositoryImpl extends RedisRepository<TemplateEntity, Object> implements TemplateEntityRepository {
    public TemplateEntityRepositoryImpl(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient) {
        super(redisTemplate, redissonClient);
    }

    public TemplateEntityRepositoryImpl(RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        super(redisTemplate, redissonClient, maxLockTime);
    }
}
