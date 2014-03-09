package db.infra;

import events.EventsStream;
import events.Pair;
import events.PushStream;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @param <K1> parent primary key
 * @param <V1> parent type
 * @param <K2> child primary key
 * @param <V2> child type
 */
public class Denormalizer<K1, V1 extends Indexed<K1>, K2, V2 extends Indexed<K2>> {

    private final CacheData<K1, V1> parentCache;
    private final CacheData<K2, V2> childCache;
    private final Consumer<Pair<V2, V2>> childChangeInput;
    private final Index<K1, V1, K2> index;
    private final PushStream<Pair<Pair<V1, V2>, Pair<V1, V2>>> output;

    public Denormalizer(CacheData<K1, V1> parent, CacheData<K2, V2> child, Function<V1, K2> indexer) {
        this.parentCache = parent;
        this.childCache = child;
        this.index = new Index<>(indexer);
        this.output = new PushStream<Pair<Pair<V1, V2>, Pair<V1, V2>>>() {
            @Override
            public void register(Consumer<Pair<Pair<V1, V2>, Pair<V1, V2>>> c) {
                parentCache.getOutput().register(parentChange -> {
                    c.accept(new Pair<>(
                            new Pair<>(parentChange.getFirst(), parentChange.getFirst() != null ? childCache.get(indexer.apply(parentChange.getFirst())) : null),
                            new Pair<>(parentChange.getSecond(), parentChange.getSecond() != null ? childCache.get(indexer.apply(parentChange.getSecond())) : null)));
                });
            }
        };
        childChangeInput = childChange -> {
            K2 childKey = childChange.getFirst() != null ? childChange.getFirst().getId() : childChange.getSecond().getId();
            index.getAll(childKey).stream().forEach(parentKey -> {
                V1 parentEntity = parentCache.get(parentKey);
                output.publish(new Pair<>(
                        new Pair<>(parentEntity, childChange.getFirst()),
                        new Pair<>(parentEntity, childChange.getSecond())));
            });
        };
    }

    public Denormalizer<K1, V1, K2, V2> start() {
        this.parentCache.getOutput().register(index.input());
        this.childCache.getOutput().register(childChangeInput);
        return this;
    }

    public Denormalizer<K1, V1, K2, V2> stop() {
        this.childCache.getOutput().unRegister(childChangeInput);
        this.parentCache.getOutput().unRegister(index.input());
        return this;
    }

    public EventsStream<Pair<Pair<V1, V2>, Pair<V1, V2>>> output() {
        return output.registerThrough();
    }
}
