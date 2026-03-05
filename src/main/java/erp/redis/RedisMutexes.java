package erp.redis;

import erp.repository.LockResult;
import erp.repository.Mutexes;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisMutexes<ID> implements Mutexes<ID> {

    RedisTemplate<String, Object> redisTemplate;
    private String mutexesKey;
    private final long maxLockTime;

    private static final String LOCK_LUA =
            "if redis.call('set', KEYS[1], ARGV[1], 'nx', 'px', ARGV[2]) then " +
            "  return 'OK' " +
            "else " +
            "  return redis.call('get', KEYS[1]) " +
            "end";

    private final RedisScript<String> lockScript = new DefaultRedisScript<>(LOCK_LUA, String.class);

    public RedisMutexes(RedisTemplate<String, Object> redisTemplate, String mutexesKey, long maxLockTime) {
        this.redisTemplate = redisTemplate;
        this.maxLockTime = maxLockTime;
        this.mutexesKey = mutexesKey;
    }


    @Override
    public LockResult lock(ID id, String processName) {
        String result = redisTemplate.execute(
                lockScript,
                redisTemplate.getStringSerializer(),
                redisTemplate.getStringSerializer(),
                Collections.singletonList(getKey(id)),
                processName,
                String.valueOf(maxLockTime)
        );
        if ("OK".equals(result)) {
            return LockResult.success();
        } else {
            return LockResult.failed(result);
        }
    }

    @Override
    public boolean newAndLock(ID id, String processName) {
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        boolean set = valueOperations.setIfAbsent(getKey(id), processName, maxLockTime, TimeUnit.MILLISECONDS);
        return set;
    }

    @Override
    public void unlockAll(Set<Object> ids) {
        List<String> strIdList = new ArrayList<>();
        for (Object id : ids) {
            strIdList.add(getKey((ID) id));
        }
        redisTemplate.delete(strIdList);
    }

    public void unlock(ID id) {
        redisTemplate.delete(getKey(id));
    }

    @Override
    public String getLockProcess(ID id) {
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        String lockProcess = (String) valueOperations.get(getKey(id));
        return lockProcess;
    }

    @Override
    public void removeAll(Set<Object> ids) {
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
