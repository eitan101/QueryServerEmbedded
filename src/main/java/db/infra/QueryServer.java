/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db.infra;

import db.infra.ChangeEvent;
import events.ChangePair;
import events.EventsStream;
import events.StreamRegisterer;
import events.Utils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 *
 * @author handasa
 */
public class QueryServer<V> {
    private final EventsStream<ChangePair<V>> inputFeed;
    private final Map<String, Query> queries;

    public QueryServer(EventsStream<ChangePair<V>> inputFeed) {
        this.inputFeed = inputFeed;
        this.queries = new ConcurrentHashMap<>();
    }

    public EventsStream<ChangeEvent<V>> get(String queryName) {
        return queries.get(queryName);
    }
    
    public EventsStream<ChangeEvent<V>> put(String name,Predicate<V> filter) {
        final Query newQuery = new Query(filter);
        Query old = queries.put(name, newQuery);
        if (old!=null)
            old.unRegister(null);
        return newQuery;
    }
    
    public void remove(String name) {
        Query old = queries.remove(name);
        if (old!=null)
            old.unRegister(null);
    }
    
    

    private class Query implements StreamRegisterer<ChangeEvent<V>> {
        private final EventsStream<ChangeEvent<V>> registerer;

        Query(Predicate<V> query) {
            this.registerer = inputFeed.map(Utils.PairToChangeEvent(query)).filter(p->p!=null);
        }

        @Override
        public void register(Consumer<ChangeEvent<V>> c) {
            registerer.register(c);
        }

        @Override
        public void unRegister(Consumer<ChangeEvent<V>> c) {
            registerer.unRegister(c);
        }
    }
}
