/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package events;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 *
 * @author handasa
 * @param <T1>
 */
public interface StreamRegisterer<T1> extends EventsStream<T1> {

    @Override
    default public <T2> StreamRegisterer<T2> map(Function<T1, T2> f) {
        return new StreamRegisterer<T2>() {
            ConcurrentHashMap<Consumer<T2>, Consumer<T1>> handlers = new ConcurrentHashMap<>();

            @Override
            public void register(Consumer<T2> c) {
                handlers.put(c, t -> c.accept(f.apply(t)));
                StreamRegisterer.this.register(handlers.get(c));
            }

            @Override
            public void unRegister(Consumer<T2> c) {
                if (c==null) {
                    handlers.values().stream().forEach(handler->StreamRegisterer.this.unRegister(handler));
                    handlers.clear();
                } else 
                    StreamRegisterer.this.unRegister(handlers.remove(c));
            }
        };
    }

    ;

    @Override
    default public StreamRegisterer<T1> filter(Predicate<T1> f) {
        return new StreamRegisterer<T1>() {
            ConcurrentHashMap<Consumer<T1>, Consumer<T1>> handlers = new ConcurrentHashMap<>();

            @Override
            public void register(Consumer<T1> c) {
                handlers.put(c, t -> {
                    if (f.test(t)) {
                        c.accept(t);
                    }
                });
                StreamRegisterer.this.register(handlers.get(c));
            }

            @Override
            public void unRegister(Consumer<T1> c) {
                if (c==null) {
                    handlers.keySet().stream().forEach(handler->StreamRegisterer.this.unRegister(handler));
                    handlers.clear();
                } else
                    StreamRegisterer.this.unRegister(handlers.remove(c));
            }
        };
    }

}
