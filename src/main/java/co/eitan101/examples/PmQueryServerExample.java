/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package co.eitan101.examples;

import db.data.Pm;
import db.infra.CacheData;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import queryserver.DataSimultor;
import queryserver.QueryServer;

/**
 *
 * @author handasa
 */
public class PmQueryServerExample {
    private static QueryServer<Pm> qs;
    private static ScheduledThreadPoolExecutor exec;
    public static QueryServer<Pm> gePmQueryServer() {
        if (qs==null)
            qs = createPmQueryServer();
        return qs;    
    }
    
    public static void stop() {
        if (exec!=null)
        exec.shutdownNow();        
    }

    private static QueryServer<Pm> createPmQueryServer() {
        exec = new ScheduledThreadPoolExecutor(1);
        return new QueryServer<>(new CacheData<>(new DataSimultor(exec).start().getPmOuput(), exec).start().getOutput());        
    }            
}
