/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package co.eitan101.examples;

import db.data.Pm;
import db.data.Target;
import db.infra.CacheData;
import db.infra.DenormalizedEntity;
import db.infra.Denormalizer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import db.infra.QueryServer;

public class FullPmQueryServerExample {
    private static QueryServer<DenormalizedEntity<Pm>> qs;
    private static ScheduledThreadPoolExecutor exec;
    public static QueryServer<DenormalizedEntity<Pm>> getPmQueryServer() {
        if (qs==null)
            qs = createPmQueryServer();
        return qs;    
    }
    
    public static void stop() {
        if (exec!=null)
        exec.shutdownNow();        
    }

    private static QueryServer<DenormalizedEntity<Pm>> createPmQueryServer() {
        exec = new ScheduledThreadPoolExecutor(1);
        final DataSimultor sim = new DataSimultor(exec).start(); 
        CacheData<Integer, Pm> pmCache = new CacheData<>(sim.getPmOuput(), exec).start();
        CacheData<Integer, Target> targetCache = new CacheData<>(sim.getTgtOuput(), exec).start();     
        Denormalizer<Integer, Pm> denormlizedPm = new Denormalizer<>(pmCache, 
                new Denormalizer.SubEntityDef<Pm, Integer, Target>("target", Target.class, targetCache, pm->pm.getTargetId())).start();   
        return new QueryServer<>(denormlizedPm.output());        
    }            
}
