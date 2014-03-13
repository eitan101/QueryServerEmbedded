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
import org.apache.commons.jxpath.JXPathContext;

/**
 *
 * @author handasa
 */
public class Utils {

    public static <V> Function<ChangePair<V>, ChangeEvent<V>> PairToChangeEvent(Predicate<V> p) {
        return t -> {
            boolean old = t.getOld()!= null && p.test(t.getOld());
            boolean newVal = t.getNew()!= null && p.test(t.getNew());
            if (old && !newVal)
                return new ChangeEvent(ChangeEvent.ChangeType.delete, t.getOld());
            if (newVal)
                return new ChangeEvent(ChangeEvent.ChangeType.update, t.getNew());
            return null;
        };
    }

    public static <V> Predicate<V> xpath(String query) {
        return t -> (boolean) JXPathContext.newContext(t).iterate(query).next();
    }
}
