package db.infra;

import events.EventsStream;
import events.Pair;
import events.PushStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @param <K>
 * @param <V>
 */
public class CacheData<K, V extends Indexed<K>> {

    ConcurrentHashMap<K, V> map;
    PushStream<Pair<V, V>> output;

    Consumer<ChangeEvent<V>> inputConsumer;
    private final EventsStream<ChangeEvent<V>> updatesStream;

    public CacheData(EventsStream<ChangeEvent<V>> updatesStream,ExecutorService exec) {
        map = new ConcurrentHashMap<>();
        output  = new PushStream<Pair<V, V>>() {
            @Override
            public void register(Consumer<Pair<V, V>> c) {                
                exec.execute(() -> {
                    System.out.println("registering by "+this);
                    map.values().stream().forEach(
                            v->c.accept(new Pair(null,v))
                    );
                    super.register(c);
                });
            }
        };
        this.updatesStream = updatesStream;
        this.inputConsumer = t -> {
            switch (t.getType()) {
                case update:
                    output.publish(new Pair<>(map.put(t.getEntity().getId(), t.getEntity()), t.getEntity()));
                    break;
                case delete:
                    output.publish(new Pair<>(map.remove(t.getEntity().getId()), null));
                    break;
                default:
                    throw new RuntimeException();
            }
        };
    }

    public CacheData<K,V> start() {
        updatesStream.register(inputConsumer);
        return this;
    }
    
    public CacheData<K,V> stop() {
        updatesStream.unRegister(inputConsumer);
        return this;
    }


    public EventsStream<Pair<V, V>> getOutput() {
        return output.registerThrough();
    }
    
    public V get(K key) {
        return map.get(key);
    }
}
