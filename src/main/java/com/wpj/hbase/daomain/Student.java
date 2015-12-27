/*
 * Copyright (c) 2015 - 12 - 27  4 : 0 :23
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase.daomain;

/**
 * Name：Student
 * Time：2015/12/27 16:00
 * author：WPJ587
 * description：学生实体类
 **/

public class Student {
    private String  id;
    private String name;
    private String address;

    public Student(String id, String name, String address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
