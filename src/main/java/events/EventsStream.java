package events;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public interface EventsStream<T> {
    void register(Consumer<T> c);
    void unRegister(Consumer<T> c);
    EventsStream<T> filter(Predicate<T> f);
    <U> EventsStream<U> map(Function <T,U> f);
}
