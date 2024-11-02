package erp.redis;

import erp.repository.Mutexes;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisMutexes<ID> implements Mutexes<ID> {

    RedisTemplate<String, Object> redisTemplate;
    private String mutexesKey;
    private boolean mock;
    private long maxLockTime;

    public RedisMutexes(RedisTemplate<String, Object> redisTemplate, String mutexesKey, long maxLockTime) {
        if (redisTemplate == null) {
            mock = true;
            return;
        }
        this.redisTemplate = redisTemplate;
        this.maxLockTime = maxLockTime;
        this.mutexesKey = mutexesKey;
    }


    @Override
    public int lock(ID id, String processName) {
        if (mock) {
            return 1;
        }
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        String lock = (String) valueOperations.get(getKey(id));
        if (lock == null || lock.isEmpty()) {
            return -1;
        }
        boolean set = valueOperations.setIfAbsent(getKey(id), processName, maxLockTime, TimeUnit.MILLISECONDS);
        return set ? 1 : 0;
    }

    @Override
    public boolean newAndLock(ID id, String processName) {
        if (mock) {
            return true;
        }
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        boolean set = valueOperations.setIfAbsent(getKey(id), processName, maxLockTime, TimeUnit.MILLISECONDS);
        return set;
    }

    @Override
    public void unlockAll(Set<Object> ids) {
        if (mock) {
            return;
        }
        List<String> strIdList = new ArrayList<>();
        for (Object id : ids) {
            strIdList.add(getKey((ID) id));
        }
        redisTemplate.delete(strIdList);
    }

    @Override
    public String getLockProcess(ID id) {
        if (mock) {
            return null;
        }
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        String lockProcess = (String) valueOperations.get(getKey(id));
        return lockProcess;
    }

    @Override
    public void removeAll(Set<Object> ids) {
        if (mock) {
            return;
        }
        List<String> strIdList = new ArrayList<>();
        for (Object id : ids) {
            strIdList.add(getKey((ID) id));
        }
        redisTemplate.delete(strIdList);
    }

    private String getKey(ID id) {
        return "mutexes:" + mutexesKey + ":" + id.toString();
    }

    public void setMutexesKey(String mutexesKey) {
        this.mutexesKey = mutexesKey;
    }
}
