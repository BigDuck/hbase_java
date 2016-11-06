package com.wpj.hbase.daomain;

/**
 * Name：User
 * Time：2016/11/3 23:26
 * author：WPJ587
 * description：
 **/

public class User {
    private String id;
    private String name;
    private String email;
    private String password;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "{id:"+id+",name:"+name+",email:"+email+",password:"+password+"}";
    }
}
