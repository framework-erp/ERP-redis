package erp.redis;

import erp.process.ProcessEntity;
import erp.repository.Store;
import erp.repository.impl.mem.MemStore;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisStore<E, ID> implements Store<E, ID> {

    private RedisTemplate<String, Object> redisTemplate;
    private HashOperations<String, byte[], byte[]> hashOperations;
    private HashMapper<Object, byte[], byte[]> mapper = new ObjectHashMapper();
    private String repositoryKey;
    private MemStore<E, ID> mockStore;

    public RedisStore(RedisTemplate<String, Object> redisTemplate, String repositoryKey) {
        if (redisTemplate == null) {
            initAsMock();
            return;
        }
        this.redisTemplate = redisTemplate;
        this.repositoryKey = repositoryKey;
        this.hashOperations = redisTemplate.opsForHash();
    }

    private void initAsMock() {
        mockStore = new MemStore<E, ID>();
    }

    private boolean isMock() {
        return mockStore != null;
    }

    @Override
    public E load(ID id) {
        if (isMock()) {
            return mockStore.load(id);
        }
        Map<byte[], byte[]> loadedHash = hashOperations.entries(getKey(id));
        return (E) mapper.fromHash(loadedHash);
    }

    @Override
    public void insert(ID id, E entity) {
        if (isMock()) {
            mockStore.insert(id, entity);
            return;
        }
        String key = getKey(id);
        if (redisTemplate.hasKey(key)) {
            throw new DuplicateKeyException(key);
        }
        Map<byte[], byte[]> mappedHash = mapper.toHash(entity);
        hashOperations.putAll(key, mappedHash);
    }

    @Override
    public void saveAll(Map<Object, Object> entitiesToInsert, Map<Object, ProcessEntity> entitiesToUpdate) {
        if (isMock()) {
            mockStore.saveAll(entitiesToInsert, entitiesToUpdate);
            return;
        }
        if (entitiesToInsert != null) {
            for (Map.Entry<Object, Object> entry : entitiesToInsert.entrySet()) {
                insert((ID) entry.getKey(), (E) entry.getValue());
            }
        }
        if (entitiesToUpdate != null) {

            //必须先删掉，否则无法处理有值变成null的情况
            removeAll(entitiesToUpdate.keySet());

            for (Map.Entry<Object, ProcessEntity> entry : entitiesToUpdate.entrySet()) {
                insert((ID) entry.getKey(), (E) entry.getValue().getEntity());
            }
        }
    }

    @Override
    public void removeAll(Set<Object> ids) {
        if (isMock()) {
            mockStore.removeAll(ids);
            return;
        }
        List<String> strIdList = new ArrayList<>();
        for (Object id : ids) {
            strIdList.add(getKey((ID) id));
        }
        redisTemplate.delete(strIdList);
    }

    private String getKey(ID id) {
        return "repository:" + repositoryKey + ":" + id;
    }

}
