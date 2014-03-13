package co.eitan101.examples;

import events.Utils;

public class MyTest {

    public static void main(String[] args) throws InterruptedException {
//        FullPmQueryServerExample.getPmQueryServer().put("default", pm -> pm.getSubEntity(Target.class, "target").orElse(Target.empty).getName().length()==4);
//        FullPmQueryServerExample.getPmQueryServer().put("default", Utils.xpath("starts-with(parent/name,'f')"));
        FullPmQueryServerExample.getPmQueryServer().put("default", Utils.xpath("starts-with(subEntities/target/name,'f')"));
        FullPmQueryServerExample.getPmQueryServer().get("default").register(System.out::println);
        Thread.sleep(5000);
        FullPmQueryServerExample.stop();
    }
}
