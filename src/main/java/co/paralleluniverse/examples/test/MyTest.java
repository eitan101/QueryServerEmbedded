/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.paralleluniverse.examples.test;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.codehaus.jackson.map.ObjectMapper;
import queryserver.DataSimultor;
import queryserver.QueryServerExample;

/**
 *
 * @author handasa
 */
public class MyTest {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        final ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        DataSimultor ds = new DataSimultor(exec);
        QueryServerExample qs = new QueryServerExample(exec, ds.getPmOuput(), ds.getTgtOuput());
        qs.start();

//        qs.getDenormlizedPm().output().register(p -> {
//            try {
//                System.out.println(mapper.writeValueAsString(p));
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        });
        ds.start();

        Thread.sleep(2000);
        exec.shutdownNow();
    }

}
