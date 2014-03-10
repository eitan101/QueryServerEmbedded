/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryserver;

import db.data.Pm;
import db.data.Target;
import db.infra.CacheData;
import db.infra.ChangeEvent;
import db.infra.Denormalizer;
import events.EventsStream;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author handasa
 */
public class QueryServer {

    Denormalizer<Integer, Pm, Integer, Target> denormlizedPm;
    private final ExecutorService exec;
    private final EventsStream<ChangeEvent<Pm>> pmOuput;
    private final EventsStream<ChangeEvent<Target>> tgtOuput;

    public QueryServer(ExecutorService exec, EventsStream<ChangeEvent<Pm>> pmOuput, EventsStream<ChangeEvent<Target>> tgtOuput) {
        this.exec = exec;
        this.pmOuput = pmOuput;
        this.tgtOuput = tgtOuput;
    }

    
    public QueryServer start() {
        final CacheData<Integer, Pm> pmCache = new CacheData<>(pmOuput, exec).start();
        final CacheData<Integer, Target> targetCache = new CacheData<>(tgtOuput, exec).start();
        denormlizedPm = new Denormalizer<>(pmCache, targetCache, pm -> pm.getTargetId()).start();
        return this;
    }

    public QueryServer stop() {
        exec.shutdownNow();
        return this;
    }

    public Denormalizer<Integer, Pm, Integer, Target> getDenormlizedPm() {
        return denormlizedPm;
    }
}
