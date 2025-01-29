package erp.redis;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
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
import org.springframework.data.redis.core.ValueOperations;

import java.util.*;

public class JsonRedisStore<E, ID> implements Store<E, ID> {

    private Class<E> entityType;
    private RedisTemplate<String, Object> redisTemplate;
    private String repositoryKey;
    private MemStore<E, ID> mockStore;
    private PipelineProcessListener pipelineProcessListener;
    ObjectMapper mapper;

    public JsonRedisStore(RedisTemplate<String, Object> redisTemplate, Class<E> entityType) {
        if (redisTemplate == null) {
            initAsMock();
            return;
        }
        this.redisTemplate = redisTemplate;
        this.repositoryKey = entityType.getSimpleName();
        this.entityType = entityType;
        this.pipelineProcessListener = AppContext.getProcessListener(PipelineProcessListener.class);
        mapper = new ObjectMapper();
        mapper.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL);
        // 配置 ObjectMapper 只关注字段
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY); // 字段可见
        mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE); // 忽略 getter
        mapper.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE); // 忽略 setter
        mapper.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
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
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        String entityJson = (String) valueOperations.get(getKey(id));
        if (entityJson == null) {
            return null;
        }
        E entity = null;
        try {
            entity = mapper.readValue(entityJson, entityType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return entity;
    }

    @Override
    public void insert(ID id, E entity) {
        if (isMock()) {
            mockStore.insert(id, entity);
            return;
        }
        String key = getKey(id);
        String entityJson = null;
        try {
            entityJson = mapper.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
        boolean set = valueOperations.setIfAbsent(key, entityJson);
        if (!set) {
            throw new DuplicateKeyException(key);
        }
    }

    @Override
    public void saveAll(Map<Object, Object> entitiesToInsert, Map<Object, ProcessEntity> entitiesToUpdate) {
        if (isMock()) {
            mockStore.saveAll(entitiesToInsert, entitiesToUpdate);
            return;
        }
        if (isPipelineProcess()) {
            PipelineProcessContext pipelineProcessContext = ThreadBoundPipelineProcessContextArray.getProcessContext();
            if (entitiesToInsert != null && !entitiesToInsert.isEmpty()) {
                Map<String, String> entityJsonsToInsert = new HashMap<>();
                for (Map.Entry<Object, Object> entry : entitiesToInsert.entrySet()) {
                    try {
                        entityJsonsToInsert.put(getKey((ID) entry.getKey()), mapper.writeValueAsString(entry.getValue()));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
                pipelineProcessContext.addOperation(redisTemplate, Operations.multiSetIfAbsent_ValueOperations,
                        entityJsonsToInsert);
            }
            if (entitiesToUpdate != null && !entitiesToUpdate.isEmpty()) {
                Map<String, String> entityJsonsToUpdate = new HashMap<>();
                for (Map.Entry<Object, ProcessEntity> entry : entitiesToUpdate.entrySet()) {
                    try {
                        entityJsonsToUpdate.put(getKey((ID) entry.getKey()), mapper.writeValueAsString(entry.getValue().getEntity()));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
                pipelineProcessContext.addOperation(redisTemplate, Operations.multiSet_ValueOperations,
                        entityJsonsToUpdate);
            }
            return;
        }
        if (entitiesToInsert != null) {
            Map<String, String> entityJsonsToInsert = new HashMap<>();
            for (Map.Entry<Object, Object> entry : entitiesToInsert.entrySet()) {
                try {
                    entityJsonsToInsert.put(getKey((ID) entry.getKey()), mapper.writeValueAsString(entry.getValue()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
            boolean set = valueOperations.multiSetIfAbsent(entityJsonsToInsert);
            if (!set) {
                throw new DuplicateKeyException(entityJsonsToInsert.keySet().toString());
            }
        }
        if (entitiesToUpdate != null) {
            Map<String, String> entityJsonsToUpdate = new HashMap<>();
            for (Map.Entry<Object, ProcessEntity> entry : entitiesToUpdate.entrySet()) {
                try {
                    entityJsonsToUpdate.put(getKey((ID) entry.getKey()), mapper.writeValueAsString(entry.getValue().getEntity()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            ValueOperations<String, Object> valueOperations = redisTemplate.opsForValue();
            valueOperations.multiSet(entityJsonsToUpdate);
        }
    }

    @Override
    public void removeAll(Set<Object> ids) {
        if (isMock()) {
            mockStore.removeAll(ids);
            return;
        }
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
