package erp.redis;

import erp.AppContext;
import erp.repository.Repository;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected String repositoryKey;
    protected RedisTemplate<String, Object> redisTemplate;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate) {
        this.repositoryKey = entityType.getSimpleName();
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, String repositoryName) {
        super(repositoryName);
        this.repositoryKey = repositoryName;
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass) {
        super(entityClass);
        this.repositoryKey = entityClass.getSimpleName();
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass, String repositoryName) {
        super(entityClass, repositoryName);
        this.repositoryKey = repositoryName;
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    @Override
    public E take(ID id) {
        E entity = super.take(id);
        if (entity == null) {
            //Repository的takeOrPutIfAbsent方法的逻辑认为空entity是没有锁的，所以这里要解锁
            ((RedisMutexes) this.mutexes).unlock(id);
        }
        return entity;
    }

    public String getRepositoryKey() {
        return repositoryKey;
    }
}
