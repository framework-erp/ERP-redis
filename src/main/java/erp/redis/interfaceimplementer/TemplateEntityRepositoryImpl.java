package erp.redis.interfaceimplementer;

import erp.redis.RedisRepository;
import org.springframework.data.redis.core.RedisTemplate;

public class TemplateEntityRepositoryImpl extends RedisRepository<TemplateEntityImpl, Object> implements TemplateEntityRepository {
    public TemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    public TemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate, long maxLockTime) {
        super(redisTemplate, maxLockTime);
    }
}
