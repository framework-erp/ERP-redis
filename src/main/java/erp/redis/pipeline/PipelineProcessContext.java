package erp.redis.pipeline;

import erp.redis.DuplicateKeyException;
import org.springframework.data.redis.core.*;

import java.util.*;

public class PipelineProcessContext {
    private Map<RedisTemplate<String, Object>, List<Operation>> operationMap = new HashMap<>();

    public void clear() {
        operationMap.clear();
    }

    public void addOperation(RedisTemplate<String, Object> redisTemplate, Operations operations, Object... args) {
        Operation operation = new Operation();
        operation.setOperations(operations);
        operation.setArgs(args);
        List<Operation> operationsList = operationMap.get(redisTemplate);
        if (operationsList == null) {
            operationsList = new ArrayList<>();
            operationMap.put(redisTemplate, operationsList);
        }
        operationsList.add(operation);
    }

    public void flush() {
        for (Map.Entry<RedisTemplate<String, Object>, List<Operation>> entry : operationMap.entrySet()) {
            RedisTemplate<String, Object> redisTemplate = entry.getKey();
            List<Operation> operationList = entry.getValue();
            List<Object> results = redisTemplate.executePipelined(new SessionCallback<Object>() {
                @Override
                public Object execute(RedisOperations operations) {
                    HashOperations<String, byte[], byte[]> hashOperations;
                    ValueOperations<String, Object> valueOperations;
                    SetOperations<String, Object> setOperations;
                    for (Operation operation : operationList) {
                        switch (operation.getOperations()) {
                            case multiSetIfAbsent_ValueOperations:
                                valueOperations = operations.opsForValue();
                                valueOperations.multiSetIfAbsent((Map<String, String>) operation.getArgs()[0]);
                                break;
                            case multiSet_ValueOperations:
                                valueOperations = operations.opsForValue();
                                valueOperations.multiSet((Map<String, String>) operation.getArgs()[0]);
                                break;
                            case add_SetOperations:
                                setOperations = operations.opsForSet();
                                setOperations.add((String) operation.getArgs()[0], (String[]) operation.getArgs()[1]);
                                break;
                            case remove_SetOperations:
                                setOperations = operations.opsForSet();
                                setOperations.remove((String) operation.getArgs()[0], (String[]) operation.getArgs()[1]);
                                break;
                            case deleteKeys_RedisOperations:
                                operations.delete((Collection) operation.getArgs()[0]);
                                break;
                            case putIfAbsent_HashOperations:
                                hashOperations = operations.opsForHash();
                                hashOperations.putIfAbsent((String) operation.getArgs()[0],
                                        (byte[]) operation.getArgs()[1], (byte[]) operation.getArgs()[2]);
                                break;
                            case putAll_HashOperations:
                                hashOperations = operations.opsForHash();
                                hashOperations.putAll((String) operation.getArgs()[0],
                                        (Map<? extends byte[], ? extends byte[]>) operation.getArgs()[1]);
                                break;
                        }
                    }
                    return null;
                }
            });

            // 检查结果
            int i = 0;
            for (Operation operation : operationList) {
                switch (operation.getOperations()) {
                    case multiSetIfAbsent_ValueOperations:
                        Boolean multiSetIfAbsentResult = (Boolean) results.get(i);
                        if (!multiSetIfAbsentResult) {
                            throw new DuplicateKeyException(((Map<String, String>) operation.getArgs()[0]).keySet().toString());
                        }
                        i++;
                        break;
                    case add_SetOperations:
                        i++;
                        break;
                    case remove_SetOperations:
                        i++;
                        break;
                    case deleteKeys_RedisOperations:
                        i++;
                        break;
                    case putIfAbsent_HashOperations:
                        Boolean putIfAbsentResult = (Boolean) results.get(i);
                        if (!putIfAbsentResult) {
                            throw new DuplicateKeyException(operation.getArgs()[0].toString());
                        }
                        i++;
                        break;
                }
            }
        }
    }


}
