package events;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class PushStream<T> implements EventsStream<T> {

    CopyOnWriteArraySet<Consumer<T>> listeners;

    public PushStream() {
        listeners = new CopyOnWriteArraySet<>();
    }

    @Override
    public void register(Consumer<T> c) {
        System.out.println("registering " + c);
        listeners.add(c);
    }

    @Override
    public void unRegister(Consumer<T> c) {
        if (c == null) {
            System.out.println("unregistering all");
            listeners.clear();
        } else {
            System.out.println("unregistering " + c);
            listeners.remove(c);
        }
    }

    public void publish(T event) {
        listeners.stream().forEach(listener -> listener.accept(event));
    }

    @Override
    public PushStream<T> filter(Predicate<T> f) {
        PushStream<T> filtered = new PushStream<>();
        register(event -> {
            if (f.test(event)) {
                filtered.publish(event);
            }
        });
        return filtered;
    }

    @Override
    public <U> PushStream<U> map(Function<T, U> f) {
        PushStream<U> mapped = new PushStream<>();
        register(event -> mapped.publish(f.apply(event)));
        return mapped;
    }

    public StreamRegisterer<T> registerThrough() {
        return new StreamRegisterer<T>() {
            @Override
            public void register(Consumer<T> c) {
                PushStream.this.register(c);
            }

            @Override
            public void unRegister(Consumer<T> c) {
                PushStream.this.unRegister(c);
            }
        };
    }
}
