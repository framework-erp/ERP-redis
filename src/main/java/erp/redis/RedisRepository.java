package erp.redis;

import erp.repository.Mutexes;
import erp.repository.Repository;
import erp.repository.Store;

public class RedisRepository<E, ID> extends Repository<E, ID> {
    protected RedisRepository(Store<E, ID> store, Mutexes<ID> mutexes) {
        super(store, mutexes);
    }
}
