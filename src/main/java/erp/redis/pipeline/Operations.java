package erp.redis.pipeline;

public enum Operations {
    multiSetIfAbsent_ValueOperations,
    multiSet_ValueOperations,
    add_SetOperations,
    remove_SetOperations,
    putIfAbsent_HashOperations,
    putAll_HashOperations,
    deleteKeys_RedisOperations
}
