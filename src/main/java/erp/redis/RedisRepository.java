package erp.redis;

import erp.AppContext;
import erp.repository.Repository;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.ArrayList;
import java.util.List;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected String repositoryKey;
    protected RedisTemplate<String, Object> redisTemplate;
    protected int scanArgsCount = 1000;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate) {
        this.repositoryKey = entityType.getSimpleName();
        this.store = new RedisStore<>(redisTemplate, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, String repositoryName) {
        super(repositoryName);
        this.repositoryKey = repositoryName;
        this.store = new RedisStore<>(redisTemplate, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass) {
        super(entityClass);
        this.repositoryKey = entityClass.getSimpleName();
        this.store = new RedisStore<>(redisTemplate, repositoryKey);
        this.mutexes = new RedisMutexes<>(redisTemplate, repositoryKey, 30000L);
        this.redisTemplate = redisTemplate;
        AppContext.registerRepository(this);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass, String repositoryName) {
        super(entityClass, repositoryName);
        this.repositoryKey = repositoryName;
        this.store = new RedisStore<>(redisTemplate, repositoryKey);
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

    public List<String> queryAllIds() {
        if (redisTemplate == null) {
            return null;
        }
        List<String> idList = new ArrayList<>();
        ScanOptions scanOptions = ScanOptions.scanOptions()
                .match("*repository:" + repositoryKey + ":*")
                .count(scanArgsCount)
                .build();
        Cursor<String> cursor = redisTemplate.scan(scanOptions);
        while (cursor.hasNext()) {
            String rawId = cursor.next();
            idList.add(rawId.split(":")[2]);
        }
        cursor.close();
        return idList;
    }

    public void setScanCount(int count) {
        this.scanArgsCount = count;
    }

    public String getRepositoryKey() {
        return repositoryKey;
    }
}
