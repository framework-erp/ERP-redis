package erp.redis;

import erp.repository.Mutexes;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

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

    public RedisMutexes(RedissonClient redissonClient, long maxLockTime) {
        this(redissonClient, maxLockTime, null);
    }

    public RedisMutexes(RedissonClient redissonClient, long maxLockTime, String entityType) {
        if (redissonClient == null) {
            mock = true;
            return;
        }
        this.redissonClient = redissonClient;
        this.maxLockTime = maxLockTime;
        this.entityType = entityType;
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
