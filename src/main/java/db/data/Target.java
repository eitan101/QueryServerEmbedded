/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db.data;

import db.infra.Indexed;

/**
 *
 * @author handasa
 */
public class Target implements Indexed<Integer>{

    public static int MAX_TARGETS = 1000;
    public static int TARGET_FIELDS = 20;

    int id;
    int[] fields;
    String name;

    public Target() {
    }

    public Target(int id, int[] fields, String name) {
        this.id = id;
        this.fields = fields;
        this.name = name;
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
}
