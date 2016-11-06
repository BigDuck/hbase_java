package com.wpj.hbase.test;

import com.wpj.hbase.common.RegionTest;
import com.wpj.hbase.dao.UserDao;
import com.wpj.hbase.dao.impl.UserDaoImpl;
import com.wpj.hbase.daomain.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Name：HbaseTwitter
 * Time：2016/11/3 23:13
 * author：WPJ587
 * description：操作hbase
 **/

public class HbaseTwitter {
    public static Configuration config;
    static HbaseTemplate hbaseTemplate;

    static {
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "192.168.254.100");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseTemplate = new HbaseTemplate(config);
    }

    public static void main(String[] args) throws IOException {
        UserDao userDao = new UserDaoImpl(hbaseTemplate);
     //   delete(userDao);
        createTable();
    }

    public static void get(UserDao userDao) {
        User user = userDao.getByRowKey("users0001");
        System.out.println(user);
    }
    public static void update(UserDao userDao){
        User user=new User();
        user.setId("users0001");
        user.setEmail("wupeji587@foxmail.com");
        userDao.addOrUpdate(user);

    }
    public static void save(UserDao userDao) {
        User user;
        List<User> adds = new ArrayList();
        for (int i = 0; i < 20; i++) {
            user = new User();
            user.setId("user0002" + i);
            user.setName("吴培基");
            user.setEmail("757671834@qq.com");
            user.setPassword("123456789");
            adds.add(user);
        }
       userDao.addOrUpdate(adds);
    }
    public static void delete(UserDao userDao) {
        User u=new User();
        u.setId("user00021");

        userDao.delete(u);
    }
    public static void createTable() throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor("twits");
        HColumnDescriptor c=new HColumnDescriptor("twits");
        c.setMaxVersions(1);
        tableDescriptor.addFamily(c);
        HBaseAdmin admin=new HBaseAdmin(hbaseTemplate.getConfiguration());
        RegionTest.createTable(admin,tableDescriptor,RegionTest.getHexSplits("0","9",9));

    }
    public static void addTwits(){
        int longLength=Long.SIZE/8;
        byte[] userHash;
    }
}
