package db.infra;



import db.data.Pm;
import db.data.Target;
import db.infra.CacheData;
import db.infra.ChangeEvent;
import db.infra.Denormalizer;
import events.EventsStream;
import java.util.concurrent.ExecutorService;

public class QueryServerExample {

    Denormalizer<Integer, Pm> denormlizedPm;
    private final ExecutorService exec;
    private final CacheData<Integer, Pm> pmCache;
    private final CacheData<Integer, Target> targetCache;

    public QueryServerExample(ExecutorService exec, EventsStream<ChangeEvent<Pm>> pmOuput, EventsStream<ChangeEvent<Target>> tgtOuput) {
        this.exec = exec;
        this.pmCache = new CacheData<>(pmOuput, exec);
        this.targetCache = new CacheData<>(tgtOuput, exec);     
        this.denormlizedPm = new Denormalizer<>(pmCache, 
                new Denormalizer.SubEntityDef<Pm, Integer, Target>("target", Target.class, targetCache, pm->pm.getTargetId()));        
    }

    
    public QueryServerExample start() {
        pmCache.start();
        targetCache.start();
        denormlizedPm.start();
        return this;
    }

    public QueryServerExample stop() {
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
