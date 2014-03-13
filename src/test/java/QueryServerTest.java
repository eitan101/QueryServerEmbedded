/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import db.data.Pm;
import db.data.Target;
import db.infra.CacheData;
import db.infra.ChangeEvent;
import db.infra.Denormalizer;
import events.DenormalizedEntity;
import events.EventsStream;
import events.PushStream;
import events.Utils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import queryserver.QueryServer;
import queryserver.QueryServerExample;

/**
 *
 * @author handasa
 */
public class QueryServerTest {

    PushStream<ChangeEvent<Pm>> pmPublisher;
    PushStream<ChangeEvent<Target>> tgtPublisher;
    private ArrayList<ChangeEvent<Pm>> res;
    private QueryServer<Pm> queryServer;

    public QueryServerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        ExecutorService exec = MoreExecutors.sameThreadExecutor();
        res = new ArrayList<>();
        pmPublisher = new PushStream<>();
        this.queryServer = new QueryServer<>(new CacheData<>(pmPublisher, exec).start().getOutput());
    }

    @After
    public void tearDown() {
    }

    @Test
    public void registerBefore() {
        EventsStream<ChangeEvent<Pm>> query = queryServer.put("testQuery", pm->pm.getName().length()==4);
        query.register(t->res.add(t));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getName());
    }

    @Test
    public void testUnregisterAll() {
        EventsStream<ChangeEvent<Pm>> query = queryServer.put("testQuery", pm->pm.getName().length()==4);
        query.register(t->res.add(t));
        query.unRegister(null);
    }
    
}

