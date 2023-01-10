package erp.redis;

import erp.repository.Mutexes;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.Set;

/**
 * @author zheng chengdong
 */
public class RedissonMutexes<ID> implements Mutexes<ID> {
    private RedissonClient redissonClient;
    private String entityType;

    @Override
    public int lock(ID id, String processName) {
        RLock lock = redissonClient.getLock(getLockName(id));
    }

    @Override
    public boolean newAndLock(ID id, String processName) {
        return false;
    }

    @Override
    public void unlockAll(Set<Object> ids) {

    }

    @Override
    public String getLockProcess(ID id) {
        return null;
    }

    @Override
    public void removeAll(Set<Object> ids) {

    }

    private String getLockName(ID id) {
        return entityType + ":" + id.toString();
    }
}
