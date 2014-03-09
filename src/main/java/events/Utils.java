/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package events;

import db.infra.ChangeEvent;
import db.infra.Indexed;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 *
 * @author handasa
 */
public class Utils {

    public static <V> Function<Pair<V, V>, ChangeEvent<V>> kk(Predicate<V> p) {
        return (Pair<V, V> t) -> {
            boolean first = t.getFirst() != null && p.test(t.getFirst());
            boolean second = t.getSecond() != null && p.test(t.getSecond());
            if (first && !second)
                return new ChangeEvent(ChangeEvent.ChangeType.delete, t.getFirst());
            if (!first && second)
                return new ChangeEvent(ChangeEvent.ChangeType.update, t.getSecond());
            return null;
        };
    }

    public static <K, V extends Indexed<K>> Function<ChangeEvent<V>, Pair<V, V>> change(ConcurrentHashMap<K, V> map) {
        return t -> {
            switch (t.getType()) {
                case update:
                    return new Pair<>(map.put(t.getEntity().getId(), t.getEntity()), t.getEntity());
                case delete:
                    return new Pair<>(map.remove(t.getEntity().getId()), null);
                default:
                    throw new RuntimeException();
            }
        };
    }

    public static <V> Predicate<Pair<V, V>> changeFilter(Predicate<V> p) {
        return t -> ((t.getFirst() != null && p.test(t.getFirst()))
                != (t.getSecond() != null && p.test(t.getSecond())));
    }
}
