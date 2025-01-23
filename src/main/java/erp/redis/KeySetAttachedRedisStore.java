package erp.redis;

import erp.process.ProcessEntity;
import erp.repository.Store;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import java.util.Map;
import java.util.Set;

public class KeySetAttachedRedisStore<E, ID> implements Store<E, ID> {

    private Store<E, ID> delegate;
    private RedisTemplate<String, Object> redisTemplate;
    private String keyOfKeySet;

    public KeySetAttachedRedisStore(Store<E, ID> delegate, RedisTemplate<String, Object> redisTemplate, String keyOfKeySet) {
        this.delegate = delegate;
        this.redisTemplate = redisTemplate;
        this.keyOfKeySet = keyOfKeySet;
    }

    @Override
    public E load(ID id) {
        return delegate.load(id);
    }

    @Override
    public void insert(ID id, E entity) {
        delegate.insert(id, entity);
    }

    @Override
    public void saveAll(Map<Object, Object> entitiesToInsert, Map<Object, ProcessEntity> entitiesToUpdate) {
        delegate.saveAll(entitiesToInsert, entitiesToUpdate);
        if (redisTemplate == null) {
            return;
        }
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        String[] keys = entitiesToInsert.keySet().stream().map(Object::toString).toArray(String[]::new);
        if (keys.length > 0) {
            setOperations.add(keyOfKeySet, keys);
        }
    }

    @Override
    public void removeAll(Set<Object> ids) {
        delegate.removeAll(ids);
        if (redisTemplate == null) {
            return;
        }
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        String[] keys = ids.stream().map(Object::toString).toArray(String[]::new);
        if (keys.length > 0) {
            setOperations.remove(keyOfKeySet, keys);
        }
    }

}
