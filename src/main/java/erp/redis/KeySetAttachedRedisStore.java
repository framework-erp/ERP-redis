package erp.redis;

import erp.AppContext;
import erp.process.ProcessEntity;
import erp.process.ThreadBoundProcessContextArray;
import erp.redis.pipeline.Operations;
import erp.redis.pipeline.PipelineProcessContext;
import erp.redis.pipeline.PipelineProcessListener;
import erp.redis.pipeline.ThreadBoundPipelineProcessContextArray;
import erp.repository.Store;
import erp.repository.impl.mem.MemStore;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import java.util.Map;
import java.util.Set;

public class KeySetAttachedRedisStore<E, ID> implements Store<E, ID> {

    private Store<E, ID> delegate;
    private RedisTemplate<String, Object> redisTemplate;
    private String keyOfKeySet;
    private PipelineProcessListener pipelineProcessListener;
    private MemStore<E, ID> mockStore;

    public KeySetAttachedRedisStore(Store<E, ID> delegate, RedisTemplate<String, Object> redisTemplate, String keyOfKeySet) {
        if (redisTemplate == null) {
            initAsMock();
            return;
        }
        this.delegate = delegate;
        this.redisTemplate = redisTemplate;
        this.keyOfKeySet = keyOfKeySet;
        this.pipelineProcessListener = AppContext.getProcessListener(PipelineProcessListener.class);
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
        return delegate.load(id);
    }

    @Override
    public void insert(ID id, E entity) {
        if (isMock()) {
            mockStore.insert(id, entity);
            return;
        }
        delegate.insert(id, entity);
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        setOperations.add(keyOfKeySet, id.toString());
    }

    @Override
    public void saveAll(Map<Object, Object> entitiesToInsert, Map<Object, ProcessEntity> entitiesToUpdate) {
        if (isMock()) {
            mockStore.saveAll(entitiesToInsert, entitiesToUpdate);
            return;
        }
        delegate.saveAll(entitiesToInsert, entitiesToUpdate);
        if (redisTemplate == null) {
            return;
        }
        if (isPipelineProcess()) {
            PipelineProcessContext pipelineProcessContext = ThreadBoundPipelineProcessContextArray.getProcessContext();
            SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
            String[] keys = entitiesToInsert.keySet().stream().map(Object::toString).toArray(String[]::new);
            if (keys.length > 0) {
                pipelineProcessContext.addOperation(redisTemplate, Operations.add_SetOperations, keyOfKeySet, keys);
            }
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
        if (isMock()) {
            mockStore.removeAll(ids);
            return;
        }
        delegate.removeAll(ids);
        if (redisTemplate == null) {
            return;
        }
        if (isPipelineProcess()) {
            PipelineProcessContext pipelineProcessContext = ThreadBoundPipelineProcessContextArray.getProcessContext();
            String[] keys = ids.stream().map(Object::toString).toArray(String[]::new);
            if (keys.length > 0) {
                pipelineProcessContext.addOperation(redisTemplate, Operations.remove_SetOperations, keyOfKeySet, keys);
            }
            return;
        }
        SetOperations<String, Object> setOperations = redisTemplate.opsForSet();
        String[] keys = ids.stream().map(Object::toString).toArray(String[]::new);
        if (keys.length > 0) {
            setOperations.remove(keyOfKeySet, keys);
        }
    }

    private boolean isPipelineProcess() {
        return pipelineProcessListener != null &&
                pipelineProcessListener.isPipelineProcess(ThreadBoundProcessContextArray.getProcessContext().getProcessName());
    }

}
