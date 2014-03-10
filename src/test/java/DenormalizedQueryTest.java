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
import db.infra.Indexed;
import events.DenormalizedEntity;
import events.EventsStream;
import events.Pair;
import events.PushStream;
import events.Utils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import queryserver.QueryServer;

/**
 *
 * @author handasa
 */
public class DenormalizedQueryTest {

    PushStream<ChangeEvent<Pm>> pmPublisher;
    PushStream<ChangeEvent<Target>> tgtPublisher;
    EventsStream<ChangeEvent<Pair<Pm, Target>>> queryOutput;

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
        ListeningExecutorService exec = MoreExecutors.sameThreadExecutor();
        pmPublisher = new PushStream<>();
        tgtPublisher = new PushStream<>();
        QueryServer qs = new QueryServer(exec, pmPublisher, tgtPublisher).start();
        queryOutput = qs.getDenormlizedPm().output().map(Utils.PairToChangeEvent(p -> p.getFirst() != null && p.getSecond() != null
                && p.getSecond().getName().length() == 4)).filter(p -> p != null);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void registerBefore() {
        ArrayList<ChangeEvent<Pair<Pm, Target>>> res = new ArrayList<>();
        queryOutput.register(t -> res.add(t));
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getFirst().getName());
    }

    @Test
    public void registerAfterTargetBeforePm() {
        ArrayList<ChangeEvent<Pair<Pm, Target>>> res = new ArrayList<>();
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        queryOutput.register(t -> res.add(t));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getFirst().getName());
    }

    @Test
    public void registerAfterPm() {
        ArrayList<ChangeEvent<Pair<Pm, Target>>> res = new ArrayList<>();
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getFirst().getName());
    }

    @Test
    public void registerBeforeTargetChange() {
        ArrayList<ChangeEvent<Pair<Pm, Target>>> res = new ArrayList<>();
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg1")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        assertEquals(1, res.size());
        assertEquals("myPm", res.get(0).getEntity().getFirst().getName());
    }

    @Test
    public void registerTwoPmChangeTarget() {
        ArrayList<ChangeEvent<Pair<Pm, Target>>> res = new ArrayList<>();
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg1")));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(1, "myPm1", null, 1, new Date())));
        pmPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Pm(2, "myPm2", null, 1, new Date())));
        queryOutput.register(t -> res.add(t));
        assertEquals(0, res.size());
        tgtPublisher.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, new Target(1, null, "myTg")));
        List<String> pmNamesRes = res.stream().map(change -> change.getEntity().getFirst().getName()).collect(Collectors.toList());
        assertEquals(2, res.size());
        assertTrue(pmNamesRes.contains("myPm1"));
        assertTrue(pmNamesRes.contains("myPm2"));
    }

    @Test
    public void testSubEntity() {
        final Pm pm = new Pm(1, "name", null, 1, new Date());
        final Target target = new Target(1, null, "target");
        final Pm subPm = new Pm(2, "IamTheSon", null, 1, new Date());
        DenormalizedEntity<Pm> de = new DenormalizedEntity<>(pm,
                new DenormalizedEntity.SubEntityBuilder().add(target, "main").add(subPm, "son"));

        assertEquals("target", de.getSubEntity(Target.class, "main").getName());
        assertEquals(1, de.getSubEntity(Pm.class, "son").getTargetId());
    }
    
    @Test(expected=RuntimeException.class)
    public void testNoSubEntity() {
        final Pm pm = new Pm(1, "name", null, 1, new Date());
        final Target target = new Target(1, null, "target");
        final Pm subPm = new Pm(2, "IamTheSon", null, 1, new Date());
        DenormalizedEntity<Pm> de = new DenormalizedEntity<>(pm,
                new DenormalizedEntity.SubEntityBuilder().add(target, "main").add(subPm, "son"));
        de.getSubEntity(Target.class, "secondary");
    }
}

