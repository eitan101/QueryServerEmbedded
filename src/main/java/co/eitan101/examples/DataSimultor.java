package co.eitan101.examples;

import db.infra.ChangeEvent;
import db.data.Pm;
import static db.data.Pm.generateRandomDate;
import db.data.Target;
import events.PushStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import db.infra.Startable;

public class DataSimultor implements Startable {

    private final PushStream<ChangeEvent<Pm>> pmOuput;
    private final PushStream<ChangeEvent<Target>> tgtOuput;
    private final ScheduledThreadPoolExecutor exec;

    public DataSimultor(ScheduledThreadPoolExecutor exec) {
        pmOuput = new PushStream<>();
        tgtOuput = new PushStream<>();
        this.exec = exec;
    }
    public static final int MAX_PM = 100;
    public static final int MAX_TGT = 100;

    @Override
    public DataSimultor start() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        HashMap<Integer, Pm> pms = new HashMap<>();
        HashMap<Integer, Target> tgts = new HashMap<>();

        exec.scheduleAtFixedRate(() -> {
            try {
                int tgtId = random.nextInt(MAX_TGT);
                Target tgt = generateRandomTarget(random, tgtId);
                tgts.put(tgtId, tgt);
                tgtOuput.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update,tgt));

                int pmId = random.nextInt(MAX_PM);                
                int randomTgtId = tgts.keySet().stream().skip(random.nextInt(tgts.size())).findFirst().get();
                final Pm pm = generateRandomPm(random, pmId, new Date(), randomTgtId);
                pms.put(pmId, pm);
                pmOuput.publish(new ChangeEvent<>(ChangeEvent.ChangeType.update, pm));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }, 0, 10, TimeUnit.MILLISECONDS);
        return this;
    }

    public PushStream<ChangeEvent<Pm>> getPmOuput() {
        return pmOuput;
    }

    public PushStream<ChangeEvent<Target>> getTgtOuput() {
        return tgtOuput;
    }

    public static Pm generateRandomPm(Random random, int i, Date now, int targetId) {
        int[] fields = new int[Pm.PM_FIELDS];
        for (int j = 0; j < Pm.PM_FIELDS; j++) {
            fields[j] = random.nextInt();
        }
        Pm pm = new Pm(i, generateRandomWords(random), fields, targetId, generateRandomDate(random, now));
        return pm;
    }

    public static String generateRandomWords(Random random) {
        char[] word = new char[random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
        for (int j = 0; j < word.length; j++) {
            word[j] = (char) ('a' + random.nextInt(26));
        }
        return new String(word);
    }

    public static Target generateRandomTarget(Random random, int id) {
        int[] fields = new int[Target.TARGET_FIELDS];
        for (int j = 0; j < Target.TARGET_FIELDS; j++) {
            fields[j] = random.nextInt();
        }
        Target target = new Target(id, fields, generateRandomWords(random));
        return target;
    }

    @Override
    public Startable stop() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
