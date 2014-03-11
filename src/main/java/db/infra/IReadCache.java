/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package db.infra;

import events.EventsStream;
import events.Pair;

public interface IReadCache<K,V> {
    V get(K k);
    EventsStream<Pair<V, V>> getOutput();
}
