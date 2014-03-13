/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.eitan101.examples;


public class MyTest {
    public static void main(String[] args) throws InterruptedException {
        PmQueryServerExample.gePmQueryServer().put("all", pm->pm.getName().startsWith("a"));
        PmQueryServerExample.gePmQueryServer().get("all").register(System.out::println);
        Thread.sleep(10000);
        PmQueryServerExample.stop();
    }
}
