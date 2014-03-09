/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import db.data.Pm;
import db.data.Target;
import db.infra.ChangeEvent;
import events.EventsStream;
import events.Pair;
import events.PushStream;
import events.Utils;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import queryserver.DataSimultor;
import queryserver.QueryServer;

/**
 *
 * @author handasa
 */
public class NewEmptyJUnitTest {

    public NewEmptyJUnitTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void hello() {
//         fail("notImplementedYet");
        //       ListeningExecutorService sameThreadExecutor = MoreExecutors.sameThreadExecutor();
        //     new QueryServer(sameThreadExecutor, null, null);

        ObjectMapper mapper = new ObjectMapper();
        ListeningExecutorService exec = MoreExecutors.sameThreadExecutor();
        PushStream<ChangeEvent<Pm>> pmPublisher = new PushStream<>();
        PushStream<ChangeEvent<Target>> tgtPublisher = new PushStream<>();
        QueryServer qs = new QueryServer(exec, pmPublisher, tgtPublisher);
        qs.start();
        ArrayList<ChangeEvent<Pair<Pm,Target>>> res = new ArrayList<>();
        qs.getDenormlizedPm().output().map(Utils.kk(p -> p.getFirst()!=null && p.getSecond()!=null && 
                        p.getSecond().getName().length()==4)).filter(p -> p != null).register(t -> res.add(t));
        exec.submit(()-> tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update,new Target(1, null, "myTg"))));
        exec.submit(()-> pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update,new Pm(1, "myPm", null, 1, new Date()))));
        assertEquals(1, res.size());
        assertEquals("myPm",res.get(0).getEntity().getFirst().getName());
    }
}
