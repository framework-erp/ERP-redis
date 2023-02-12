package erp.redis;

import erp.repository.Repository;
import org.redisson.api.RedissonClient;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected RedisTemplate<String, Object> redisTemplate;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate) {
        this(redisTemplate, 30000);
    }

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate, long maxLockTime) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(null, maxLockTime));
        ((RedisStore) store).setEntityType(entityType);
        ((RedisMutexes) mutexes).setEntityType(entityType);
        ((RedisMutexes) mutexes).setRedissonClient(getRedissonClientFromTemplate(redisTemplate));
        this.redisTemplate = redisTemplate;
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, Class<E> entityType) {
        this(redisTemplate, redissonClient, 30000, entityType);
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime, Class<E> entityClass) {
        super(new RedisStore<>(redisTemplate), new RedisMutexes<>(redissonClient, maxLockTime), entityClass);
        ((RedisStore) store).setEntityType(entityType);
        ((RedisMutexes) mutexes).setEntityType(entityType);
        ((RedisMutexes) mutexes).setRedissonClient(getRedissonClientFromTemplate(redisTemplate));
        this.redisTemplate = redisTemplate;
    }

    private RedissonClient getRedissonClientFromTemplate(RedisTemplate<String, Object> redisTemplate) {
        if (redisTemplate == null) {
            return null;
        }
        RedissonConnectionFactory redissonConnectionFactory = (RedissonConnectionFactory) redisTemplate.getConnectionFactory();
        Field fieldRedisson;
        try {
            fieldRedisson = RedissonConnectionFactory.class.getDeclaredField("redisson");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("field 'redisson' not found in RedissonConnectionFactory", e);
        }
        fieldRedisson.setAccessible(true);
        try {
            return (RedissonClient) fieldRedisson.get(redissonConnectionFactory);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("can not get field 'redisson' from RedissonConnectionFactory", e);
        }
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
