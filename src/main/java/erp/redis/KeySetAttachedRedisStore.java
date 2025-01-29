package erp.redis;

import erp.AppContext;
import erp.process.ProcessEntity;
import erp.process.ThreadBoundProcessContextArray;
import erp.redis.pipeline.Operations;
import erp.redis.pipeline.PipelineProcessContext;
import erp.redis.pipeline.PipelineProcessListener;
import erp.redis.pipeline.ThreadBoundPipelineProcessContextArray;
import erp.repository.Store;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import java.util.Map;
import java.util.Set;

public class KeySetAttachedRedisStore<E, ID> implements Store<E, ID> {

    private Store<E, ID> delegate;
    private RedisTemplate<String, Object> redisTemplate;
    private String keyOfKeySet;
    private PipelineProcessListener pipelineProcessListener;

    public KeySetAttachedRedisStore(Store<E, ID> delegate, RedisTemplate<String, Object> redisTemplate, String keyOfKeySet) {
        this.delegate = delegate;
        this.redisTemplate = redisTemplate;
        this.keyOfKeySet = keyOfKeySet;
        this.pipelineProcessListener = AppContext.getProcessListener(PipelineProcessListener.class);
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
