package erp.redis;

import erp.AppContext;
import erp.repository.Repository;
import erp.repository.impl.mem.MemMutexes;
import erp.repository.impl.mem.MemStore;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SetOperations;

import java.util.ArrayList;
import java.util.List;

public class AllIdQuerySupportedRedisRepository<E, ID> extends Repository<E, ID> {
    protected String repositoryKey;
    protected RedisTemplate<String, Object> redisTemplate;
    protected String keyOfKeySet;
    protected int scanArgsCount = 1000;
    protected long scanSleepTime = 100;

    protected AllIdQuerySupportedRedisRepository(RedisTemplate<String, Object> redisTemplate) {
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = entityType.getSimpleName();
        this.keyOfKeySet = "repositorykeyset:" + repositoryKey;
        this.store = new KeySetAttachedRedisStore(new JsonRedisStore<>(redisTemplate, entityType, repositoryKey), redisTemplate, keyOfKeySet);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    protected AllIdQuerySupportedRedisRepository(RedisTemplate<String, Object> redisTemplate, String repositoryName) {
        super(repositoryName);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = repositoryName;
        this.keyOfKeySet = "repositorykeyset:" + repositoryKey;
        this.store = new KeySetAttachedRedisStore(new JsonRedisStore<>(redisTemplate, entityType, repositoryKey), redisTemplate, keyOfKeySet);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public AllIdQuerySupportedRedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass) {
        super(entityClass);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = entityClass.getSimpleName();
        this.keyOfKeySet = "repositorykeyset:" + repositoryKey;
        this.store = new KeySetAttachedRedisStore(new JsonRedisStore<>(redisTemplate, entityType, repositoryKey), redisTemplate, keyOfKeySet);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public AllIdQuerySupportedRedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass, String repositoryName) {
        super(entityClass, repositoryName);
        if (redisTemplate == null) {
            this.store = new MemStore<>();
            this.mutexes = new MemMutexes<>();
            AppContext.registerRepository(this);
            return;
        }
        this.repositoryKey = repositoryName;
        this.keyOfKeySet = "repositorykeyset:" + repositoryKey;
        this.store = new KeySetAttachedRedisStore(new JsonRedisStore<>(redisTemplate, entityType, repositoryKey), redisTemplate, keyOfKeySet);
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

    public List<String> queryAllIds() {
        if (redisTemplate == null) {
            return null;
        }
        List<String> idList = new ArrayList<>();
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        ScanOptions scanOptions = ScanOptions.scanOptions()
                .count(scanArgsCount)
                .build();
        Cursor cursor = setOperations.scan(keyOfKeySet, scanOptions);
        int scanCount = scanArgsCount;
        while (cursor.hasNext()) {
            String rawId = (String) cursor.next();
            idList.add(rawId);
            scanCount--;
            if (scanCount == 0) {
                try {
                    Thread.sleep(scanSleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                scanCount = scanArgsCount;
            }
        }
        cursor.close();
        return idList;
    }

    public void setScanCount(int count) {
        this.scanArgsCount = count;
    }

    public void setScanSleepTime(long time) {
        this.scanSleepTime = time;
    }

    public String getRepositoryKey() {
        return repositoryKey;
    }
}
