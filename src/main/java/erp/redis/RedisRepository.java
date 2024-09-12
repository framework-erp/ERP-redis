package erp.redis;

import erp.repository.Repository;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected RedisTemplate<String, Object> redisTemplate;
    protected int scanArgsCount = 1000;

    protected RedisRepository(RedisTemplate<String, Object> redisTemplate) {
        this.store = new RedisStore<>(redisTemplate, this.entityType);
        this.mutexes = new RedisMutexes<>(redisTemplate, this.entityType, 30000L);
        this.redisTemplate = redisTemplate;
    }

    public RedisRepository(RedisTemplate<String, Object> redisTemplate, Class<E> entityClass) {
        super(entityClass.getName());
        this.store = new RedisStore<>(redisTemplate, this.entityType);
        this.mutexes = new RedisMutexes<>(redisTemplate, this.entityType, 30000L);
        this.redisTemplate = redisTemplate;
    }


    public List<String> queryAllIds() {
        if (redisTemplate == null) {
            return null;
        }
        ScanOptions scanOptions = ScanOptions.scanOptions()
                .match("*entity:" + entityType + ":*")
                .count(scanArgsCount)
                .build();
        Cursor<String> cursor = redisTemplate.scan(scanOptions);
        List<String> rawList = cursor.stream().collect(Collectors.toList());
        List<String> idList = new ArrayList<>(rawList.size());
        for (String rawId : rawList) {
            idList.add(rawId.split(":")[2]);
        }
        return idList;
    }

    public void setScanCount(int count) {
        this.scanArgsCount = count;
    }
}
