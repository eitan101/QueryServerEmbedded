package db.infra;

/**
 * @param <V>
 */
public class ChangeEvent<V> {
    public enum ChangeType { update, delete };
    final ChangeType type;
    final V entity;

    public ChangeEvent(ChangeType type, V entity) {
        this.type = type;
        this.entity = entity;
    }

    public V getEntity() {
        return entity;
    }

    public ChangeType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" + "type=" + type + ", t=" + entity + '}';
    }    
}
