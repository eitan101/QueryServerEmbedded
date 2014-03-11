/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryserver;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import db.data.Pm;
import db.data.Target;
import db.infra.CacheData;
import db.infra.ChangeEvent;
import db.infra.Denormalizer;
import events.EventsStream;
import java.util.concurrent.ExecutorService;
import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 *
 * @author handasa
 */
public class QueryServer {

    Denormalizer<Integer, Pm> denormlizedPm;
    private final ExecutorService exec;
    private final CacheData<Integer, Pm> pmCache;
    private final CacheData<Integer, Target> targetCache;

    public QueryServer(ExecutorService exec, EventsStream<ChangeEvent<Pm>> pmOuput, EventsStream<ChangeEvent<Target>> tgtOuput) {
        this.exec = exec;
        this.pmCache = new CacheData<>(pmOuput, exec);
        this.targetCache = new CacheData<>(tgtOuput, exec);     
        this.denormlizedPm = new Denormalizer<>(pmCache, ImmutableList.of(
                new Denormalizer.SubEntityDef<Pm, Integer, Target>("target", Target.class, targetCache, pm->pm.getTargetId())));        
    }

    
    public QueryServer start() {
        pmCache.start();
        targetCache.start();
        denormlizedPm.start();
        return this;
    }

    public QueryServer stop() {
        pmCache.stop();
        targetCache.stop();
        denormlizedPm.stop();        
        exec.shutdownNow();
        return this;
    }

    public Denormalizer<Integer, Pm> getDenormlizedPm() {
        return denormlizedPm;
    }
}
