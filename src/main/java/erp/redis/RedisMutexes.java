package erp.redis;

import erp.repository.Mutexes;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author zheng chengdong
 */
public class RedisMutexes<ID> implements Mutexes<ID> {
    private RedissonClient redissonClient;
    private String entityType;
    private boolean mock;
    private long maxLockTime;

    public RedisMutexes(RedisTemplate<String, Object> redisTemplate, long maxLockTime) {
        this(redisTemplate, maxLockTime, null);
    }

    public RedisMutexes(RedisTemplate<String, Object> redisTemplate, long maxLockTime, String entityType) {
        if (redisTemplate == null) {
            mock = true;
            return;
        }
        this.redissonClient = getRedissonClientFromTemplate(redisTemplate);
        this.maxLockTime = maxLockTime;
        this.entityType = entityType;
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

    @Override
    public int lock(ID id, String processName) {
        if (mock) {
            return 1;
        }
        RLock lock = redissonClient.getLock(getLockName(id));
        if (!lock.isLocked()) {
            return -1;
        }
        try {
            if (lock.tryLock(0, maxLockTime, TimeUnit.MILLISECONDS)) {
                RBucket<String> processNameRBucket = redissonClient.getBucket(getProcessNameRBucketKey(id));
                processNameRBucket.set(processName);
                return 1;
            } else {
                return 0;
            }
        } catch (InterruptedException e) {
            return 0;
        }
    }

    @Override
    public boolean newAndLock(ID id, String processName) {
        if (mock) {
            return true;
        }
        RLock lock = redissonClient.getLock(getLockName(id));
        try {
            if (lock.tryLock(0, maxLockTime, TimeUnit.MILLISECONDS)) {
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public void unlockAll(Set<Object> ids) {
        if (mock) {
            return;
        }
        for (Object id : ids) {
            RLock lock = redissonClient.getLock(getLockName((ID) id));
            lock.unlock();
        }
    }

    @Override
    public String getLockProcess(ID id) {
        if (mock) {
            return null;
        }
        RBucket<String> processNameRBucket = redissonClient.getBucket(getProcessNameRBucketKey(id));
        return processNameRBucket.get();
    }

    @Override
    public void removeAll(Set<Object> ids) {
        if (mock) {
            return;
        }
        for (Object id : ids) {
            RBucket<String> processNameRBucket = redissonClient.getBucket(getProcessNameRBucketKey((ID) id));
            processNameRBucket.delete();
        }
    }

    public void setRedissonClient(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    private String getLockName(ID id) {
        return entityType + ":" + id.toString();
    }

    private String getProcessNameRBucketKey(ID id) {
        return "mutexes:" + entityType + ":" + id.toString();
    }
}
