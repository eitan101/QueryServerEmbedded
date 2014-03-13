package events;

public class ChangePair<V> {
    private final Pair<V,V> pair;

    public ChangePair(V first, V second) {
        pair = new Pair<>(first,second);
    }

    public V getOld() {
        return pair.getFirst();
    }

    public V getNew() {
        return pair.getSecond();
    }

    @Override
    public String toString() {
        return "ChangePair{" + "old=" + pair.getFirst() + ", new=" + pair.getSecond()+ '}';
    }
}
