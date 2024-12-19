package erp.redis;

import erp.repository.SingletonEntity;
import erp.repository.SingletonRepository;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisSingletonRepository<E> extends SingletonRepository<E> {

    private static RedisRepository<SingletonEntity, String> SINGLETON_CONTAINER;

    private static void init(RedisTemplate<String, Object> redisTemplate) {
        if (SINGLETON_CONTAINER == null) {
            synchronized (RedisSingletonRepository.class) {
                if (SINGLETON_CONTAINER == null) {
                    SINGLETON_CONTAINER = new RedisRepository<>(redisTemplate, SingletonEntity.class,
                            "erp.redis.RedisSingletonRepository");
                }
            }
        }
    }


    protected RedisSingletonRepository(RedisTemplate<String, Object> redisTemplate) {
        init(redisTemplate);
        this.singletonEntitiesContainer = SINGLETON_CONTAINER;
    }

    public RedisSingletonRepository(RedisTemplate<String, Object> redisTemplate, String repositoryName) {
        super(repositoryName);
        init(redisTemplate);
        this.singletonEntitiesContainer = SINGLETON_CONTAINER;
    }

    public RedisSingletonRepository(RedisTemplate<String, Object> redisTemplate, E entity) {
        super(entity);
        init(redisTemplate);
        this.singletonEntitiesContainer = SINGLETON_CONTAINER;
        ensureEntity(entity);
    }

    public RedisSingletonRepository(RedisTemplate<String, Object> redisTemplate, E entity, String repositoryName) {
        super(repositoryName);
        init(redisTemplate);
        this.singletonEntitiesContainer = SINGLETON_CONTAINER;
        ensureEntity(entity);
    }

    private void ensureEntity(E entity) {
        SingletonEntity singletonEntity = new SingletonEntity();
        singletonEntity.setName(name);
        singletonEntity.setEntity(entity);
        try {
            this.singletonEntitiesContainer.getStore().insert(name, singletonEntity);
        } catch (DuplicateKeyException e) {
            //什么也不用做
        }
    }
}
