package erp.redis;

import erp.AppContext;
import erp.process.ProcessContext;
import erp.process.ThreadBoundProcessContextArray;
import erp.repository.Repository;
import erp.repository.impl.mem.MemMutexes;
import erp.repository.impl.mem.MemStore;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.List;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected String repositoryKey;
    protected RedisTemplate<String, Object> redisTemplate;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate) {
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = entityType.getSimpleName();
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, String repositoryName) {
        super(repositoryName);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = repositoryName;
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass) {
        super(entityClass);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = entityClass.getSimpleName();
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass, String repositoryName) {
        super(entityClass, repositoryName);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = repositoryName;
        this.store = new JsonRedisStore<>(redisTemplate, entityType, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    @Override
    public E take(ID id) {
        if (redisTemplate == null) {
            return super.take(id);
        }
        E entity = super.take(id);
        if (entity == null) {
            //Repository的takeOrPutIfAbsent方法的逻辑认为空entity是没有锁的，所以这里要解锁
            ((RedisMutexes) this.mutexes).unlock(id);
        }
        return entity;
    }

    public List<E> findByIds(List<ID> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        List<E> result = new ArrayList<>();
        List<ID> idsToLoad = new ArrayList<>();

        ProcessContext processContext = ThreadBoundProcessContextArray.getProcessContext();
        for (ID id : ids) {
            if (processContext != null && processContext.isStarted()) {
                E entity = processContext.copyEntityInProcess(name, id);
                if (entity != null) {
                    result.add(entity);
                    continue;
                }
            }
            idsToLoad.add(id);
        }

        if (!idsToLoad.isEmpty()) {
            if (store instanceof JsonRedisStore) {
                result.addAll(((JsonRedisStore<E, ID>) store).loadAll(idsToLoad));
            } else {
                for (ID id : idsToLoad) {
                    E entity = find(id);
                    if (entity != null) {
                        result.add(entity);
                    }
                }
            }
        }
        return result;
    }

    public String getRepositoryKey() {
        return repositoryKey;
    }
}
