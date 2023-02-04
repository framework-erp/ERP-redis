package erp.redis.interfaceimplementer;

public interface TemplateEntityRepository {
    TemplateEntity take(Object id);

    TemplateEntity find(Object id);

    void put(TemplateEntity entity);

    TemplateEntity putIfAbsent(TemplateEntity entity);

    TemplateEntity takeOrPutIfAbsent(Object id, TemplateEntity newEntity);

    TemplateEntity remove(Object id);
}
