package erp.redis;

public class DuplicateKeyException extends RuntimeException{
    public DuplicateKeyException(String key) {
        super("Duplicate key: " + key);
    }
}
