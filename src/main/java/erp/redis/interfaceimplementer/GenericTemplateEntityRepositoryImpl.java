package erp.redis.interfaceimplementer;

import erp.redis.RedisRepository;
import org.springframework.data.redis.core.RedisTemplate;

public class GenericTemplateEntityRepositoryImpl extends RedisRepository<TemplateEntityImpl, Object> implements GenericTemplateEntityRepository<TemplateEntityImpl, Object> {

    public GenericTemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    public GenericTemplateEntityRepositoryImpl(RedisTemplate<String, Object> redisTemplate, long maxLockTime) {
        super(redisTemplate, maxLockTime);
    }

}
