package db.infra;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import events.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * @author handasa
 * @param <K1> primary key
 * @param <V>  object type
 * @param <K2> secnodary key
 */
public class Index<K1,V extends Indexed<K1>,K2> {
    final Multimap<K2,K1> index;
    final Consumer<Pair<V,V>> input;
    final Function<V,K2> indexer;

    public Index(Function<V,K2> indexer) {
        this.indexer = indexer;
        this.index = Multimaps.newSetMultimap(new ConcurrentHashMap<K2, Collection<K1>>(), () -> new HashSet<K1>());
        this.input = t -> {
            if (t.getSecond()==null) {
                index.remove(indexer.apply((t.getFirst())),t.getFirst().getId());
            } else if (t.getFirst()==null) {
                index.put(indexer.apply((t.getSecond())), t.getSecond().getId());
            } else if (indexer.apply(t.getFirst()) != indexer.apply(t.getSecond())) {
                index.remove(indexer.apply((t.getFirst())),t.getFirst().getId());
                index.put(indexer.apply((t.getSecond())), t.getSecond().getId());
            }
        };
    }

    public Consumer<Pair<V, V>> input() {
        return input;
    }
    
    public Collection<K1> getAll(K2 key) {
        return index.get(key);
    }
}
