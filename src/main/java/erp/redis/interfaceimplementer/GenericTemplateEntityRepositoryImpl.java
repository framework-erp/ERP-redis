package erp.redis.interfaceimplementer;

import erp.redis.RedisRepository;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

public class GenericTemplateEntityRepositoryImpl extends RedisRepository<TemplateEntity, Object> implements GenericTemplateEntityRepository<TemplateEntity, Object> {

    public GenericTemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        super(redisTemplate, redissonClient);
    }

    public GenericTemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        super(redisTemplate, redissonClient, maxLockTime);
    }

}
