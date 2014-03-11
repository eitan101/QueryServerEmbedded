package co.paralleluniverse.examples.test;

import queryserver.DataSimultor;
import events.EventsStream;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import queryserver.QueryServerExample;

@WebListener
public class MyServletContextListener implements ServletContextListener {

    public static EventsStream stream;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        DataSimultor ds = new DataSimultor(exec);
        QueryServerExample qs = new QueryServerExample(exec,ds.getPmOuput(),ds.getTgtOuput());
        qs.start();
        ds.start();
//        stream =  qs.getDenormlizedPm().output().
//                map(Utils.PairToChangeEvent(p -> p.getFirst()!=null && p.getSecond()!=null && 
//                        p.getSecond().getName().length()==4)).
////                map(Utils.kk(p -> p.getFirst()!=null && p.getSecond()!=null)).
//                filter(p -> p != null);

//        registerLoadTesters();        
    }

    private void registerLoadTesters() {
        AtomicInteger ai = new AtomicInteger();
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {
            System.out.println("add new subscriber "+ai.incrementAndGet());
//            stream.register( t -> { if (t.getEntity()==null) });
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
    }
}
