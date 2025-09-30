package erp.redis;

import erp.AppContext;
import erp.process.ProcessEntity;
import erp.process.ThreadBoundProcessContextArray;
import erp.redis.pipeline.Operations;
import erp.redis.pipeline.PipelineProcessContext;
import erp.redis.pipeline.PipelineProcessListener;
import erp.redis.pipeline.ThreadBoundPipelineProcessContextArray;
import erp.repository.Store;
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
    private PipelineProcessListener pipelineProcessListener;
    private String entityIDField;

    public RedisStore(RedisTemplate<String, Object> redisTemplate, String repositoryKey, String entityIDField) {
        this.redisTemplate = redisTemplate;
        this.repositoryKey = repositoryKey;
        this.entityIDField = entityIDField;
        this.hashOperations = redisTemplate.opsForHash();
        this.pipelineProcessListener = AppContext.getProcessListener(PipelineProcessListener.class);
    }

    @Override
    public E load(ID id) {
        Map<byte[], byte[]> loadedHash = hashOperations.entries(getKey(id));
        return (E) mapper.fromHash(loadedHash);
    }

    @Override
    public void insert(ID id, E entity) {
        String key = getKey(id);
        Map<byte[], byte[]> mappedHash = mapper.toHash(entity);
        for (Map.Entry<byte[], byte[]> entry : mappedHash.entrySet()) {
            if (entityIDField.equals(new String(entry.getKey()))) {
                boolean success = hashOperations.putIfAbsent(key, entry.getKey(), entry.getValue());
                if (!success) {
                    throw new DuplicateKeyException(key);
                }
                break;
            }
        }
        hashOperations.putAll(key, mappedHash);
    }


    @Override
    public void saveAll(Map<Object, Object> entitiesToInsert, Map<Object, ProcessEntity> entitiesToUpdate) {
        if (isPipelineProcess()) {
            PipelineProcessContext pipelineProcessContext = ThreadBoundPipelineProcessContextArray.getProcessContext();
            if (entitiesToInsert != null) {
                for (Map.Entry<Object, Object> entry : entitiesToInsert.entrySet()) {
                    ID id = (ID) entry.getKey();
                    E entity = (E) entry.getValue();
                    String key = getKey(id);
                    Map<byte[], byte[]> mappedHash = mapper.toHash(entity);
                    for (Map.Entry<byte[], byte[]> mappedHashEntry : mappedHash.entrySet()) {
                        if (entityIDField.equals(new String(mappedHashEntry.getKey()))) {
                            pipelineProcessContext.addOperation(redisTemplate, Operations.putIfAbsent_HashOperations,
                                    key, mappedHashEntry.getKey(), mappedHashEntry.getValue());
                            break;
                        }
                    }
                    pipelineProcessContext.addOperation(redisTemplate, Operations.putAll_HashOperations, key, mappedHash);
                }
            }
            if (entitiesToUpdate != null) {

                //必须先删掉，否则无法处理有值变成null的情况
                removeAll(entitiesToUpdate.keySet());

                for (Map.Entry<Object, ProcessEntity> entry : entitiesToUpdate.entrySet()) {
                    ID id = (ID) entry.getKey();
                    E entity = (E) entry.getValue();
                    String key = getKey(id);
                    Map<byte[], byte[]> mappedHash = mapper.toHash(entity);
                    pipelineProcessContext.addOperation(redisTemplate, Operations.putAll_HashOperations, key, mappedHash);
                }
            }
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
        if (ids.isEmpty()) {
            return;
        }
        if (isPipelineProcess()) {
            PipelineProcessContext pipelineProcessContext = ThreadBoundPipelineProcessContextArray.getProcessContext();
            List<String> strIdList = new ArrayList<>();
            for (Object id : ids) {
                strIdList.add(getKey((ID) id));
            }
            pipelineProcessContext.addOperation(redisTemplate, Operations.deleteKeys_RedisOperations, strIdList);
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

    private boolean isPipelineProcess() {
        return pipelineProcessListener != null &&
                pipelineProcessListener.isPipelineProcess(ThreadBoundProcessContextArray.getProcessContext().getProcessName());
    }

}
