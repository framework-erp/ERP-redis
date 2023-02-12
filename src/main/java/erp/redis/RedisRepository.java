package erp.redis;

import erp.repository.Repository;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected RedisTemplate<String, Object> redisTemplate;
    protected RedissonClient redissonClient;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        this(redisTemplate, redissonClient, 30000);
    }

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(redissonClient, maxLockTime));
        ((RedisStore) store).setEntityType(entityType);
        ((RedisMutexes) mutexes).setEntityType(entityType);
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, Class<E> entityType) {
        this(redisTemplate, redissonClient, 30000, entityType);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime, Class<E> entityClass) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(redissonClient, maxLockTime), entityClass);
        ((RedisStore) store).setEntityType(entityType);
        ((RedisMutexes) mutexes).setEntityType(entityType);
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }

    public List<String> queryAllIds() {
        if (redisTemplate == null) {
            return null;
        }
        ScanOptions scanOptions = ScanOptions.scanOptions()
                .match("*" + entityType + ":*")
                .count(Integer.MAX_VALUE)
                .build();
        Cursor<String> cursor = redisTemplate.scan(scanOptions);
        List<String> rawList = cursor.stream().collect(Collectors.toList());
        List<String> idList = new ArrayList<>(rawList.size());
        for (String rawId : rawList) {
            idList.add(rawId.split(":")[1]);
        }
        return idList;
    }
}
