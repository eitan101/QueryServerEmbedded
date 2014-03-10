package db.data;

import db.infra.Indexed;
import java.util.Date;
import java.util.Random;

public class Pm implements Indexed<Integer> {

    public static int PM_FIELDS = 20;
    int id;
    String name;
    int[] fields;
    int targetId;
    Date dueDate;

    public Pm() {
    }

    public Pm(int id, String name, int[] fields, int targetId, Date dueDate) {
        this.id = id;
        this.name = name;
        this.fields = fields;
        this.targetId = targetId;
        this.dueDate = dueDate;
    }

    @Override
    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTargetId() {
        return targetId;
    }

    public void setTargetId(int targetId) {
        this.targetId = targetId;
    }

    public Date getDueDate() {
        return dueDate;
    }

    public void setDueDate(Date dueDate) {
        this.dueDate = dueDate;
    }

    @Override
    public String toString() {
        return "Pm{" + "id=" + id + ", name=" + name + ", targetId=" + targetId + ", dueDate=" + dueDate + '}';
    }

    public static Date generateRandomDate(Random random, Date now) {
        return new Date(now.getTime() + random.nextInt(24 * 365) * 3600l * 1000l);
    }
}
