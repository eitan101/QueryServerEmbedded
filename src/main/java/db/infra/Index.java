package db.infra;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import events.ChangePair;
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
    final Consumer<ChangePair<V>> input;
    final Function<V,K2> indexer;

    public Index(Function<V,K2> indexer) {
        this.indexer = indexer;
        this.index = Multimaps.newSetMultimap(new ConcurrentHashMap<K2, Collection<K1>>(), () -> new HashSet<K1>());
        this.input = t -> {
            if (t.getNew()==null) {
                index.remove(indexer.apply((t.getOld())),t.getOld().getId());
            } else if (t.getOld()==null) {
                index.put(indexer.apply((t.getNew())), t.getNew().getId());
            } else if (indexer.apply(t.getOld()) != indexer.apply(t.getNew())) {
                index.remove(indexer.apply((t.getOld())),t.getOld().getId());
                index.put(indexer.apply((t.getNew())), t.getNew().getId());
            }
        };
    }

    public Consumer<ChangePair<V>> input() {
        return input;
    }
    
    public Collection<K1> getAll(K2 key) {
        return index.get(key);
    }
}
