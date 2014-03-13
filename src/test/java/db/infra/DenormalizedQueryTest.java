package db.infra;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.common.util.concurrent.MoreExecutors;
import db.data.Pm;
import db.data.Target;
import db.infra.ChangeEvent;
import db.infra.DenormalizedEntity;
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

public class DenormalizedQueryTest {

    PushStream<ChangeEvent<Pm>> pmPublisher;
    PushStream<ChangeEvent<Target>> tgtPublisher;
    EventsStream<ChangeEvent<DenormalizedEntity<Pm>>> queryOutput;
    private ArrayList<ChangeEvent<DenormalizedEntity<Pm>>> res;

    public DenormalizedQueryTest() {
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
        tgtPublisher = new PushStream<>();
        QueryServerExample qs = new QueryServerExample(exec, pmPublisher, tgtPublisher).start();
        queryOutput = qs.getDenormlizedPm().output().
                map(Utils.PairToChangeEvent(p ->
                        p.getSubEntity(Target.class, "target").orElse(Target.empty).getName().length() == 4)).
                filter(p -> p != null);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void registerBefore() {
        queryOutput.register(t -> res.add(t));
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getParent().getName());
    }

    @Test
    public void registerAfterTargetBeforePm() {
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        queryOutput.register(t -> res.add(t));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getParent().getName());
    }

    @Test
    public void registerAfterPm() {
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getParent().getName());
    }

    @Test
    public void registerBeforeTargetChange() {
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg1")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getParent().getName());
    }

    @Test
    public void registerTwoPmChangeTarget() {
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg1")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm1", null, 1, new Date())));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(2, "myPm2", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        assertEquals(0, res.size());
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        List<String> pmNamesRes = res.stream().map(change -> change.getEntity().getParent().getName()).collect(Collectors.toList());
        assertEquals(2, res.size());
        assertTrue(pmNamesRes.contains("myPm1"));
        assertTrue(pmNamesRes.contains("myPm2"));
    }
}

