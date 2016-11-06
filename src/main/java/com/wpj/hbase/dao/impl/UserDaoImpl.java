package com.wpj.hbase.dao.impl;

import com.wpj.hbase.dao.UserDao;
import com.wpj.hbase.daomain.User;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Name：UserDaoImpl
 * Time：2016/11/3 23:29
 * author：WPJ587
 * description：
 **/

public class UserDaoImpl implements UserDao {
    private HbaseTemplate hbaseTemplate;
    private String tableName = "users";
    private String familyName = "info";
    private byte[] name, email, password, byteFamilyName;

    public UserDaoImpl(HbaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
        name = Bytes.toBytes("name");
        email = Bytes.toBytes("email");
        password = Bytes.toBytes("password");
        byteFamilyName = Bytes.toBytes("info");

    }

    public void save(User user) throws IOException {

        hbaseTemplate.put(tableName, user.getId(), familyName, "name", Bytes.toBytes(user.getName()));
    }

    public void addOrUpdate(User user) {
        List<User> u = new ArrayList<User>();
        u.add(user);
        addOrUpdate(u);
    }

    public void addOrUpdate(final List<User> users) {
        hbaseTemplate.execute(tableName, new TableCallback<Boolean>() {
            public Boolean doInTable(HTableInterface table) throws Throwable {
                List<Put> dataList = new ArrayList<Put>();
                Put put;
                for (User user : users) {
                    put = new Put(Bytes.toBytes(user.getId()));
                    if(user.getName()!=null){
                        put.addColumn(byteFamilyName, name, Bytes.toBytes(user.getName()));
                    }
                    if(user.getPassword()!=null){
                        put.addColumn(byteFamilyName, password, Bytes.toBytes(user.getPassword()));
                    }
                    if(user.getEmail()!=null){
                        put.addColumn(byteFamilyName, email, Bytes.toBytes(user.getEmail()));
                    }
                    dataList.add(put);
                }
                table.put(dataList);
                return true;
            }
        });
    }

    public User getUser(User user) {
        // String result= get(tableName,"users0001","info",null);
        get(tableName, "users0001", null, null);
        return null;
    }

    /**
     * 根据rowkey获取数据
     *
     * @param rowkey
     * @return
     */
    public User getByRowKey(final String rowkey) {
        final User u = new User();
        hbaseTemplate.execute(tableName, new TableCallback<User>() {
            public User doInTable(HTableInterface table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                Cell[]kv=result.rawCells();
                for (int i = 0; i <kv.length ; i++) {
                    System.out.println("版本"+i+Bytes.toString(kv[i].getValueArray(), kv[i].getValueOffset(), kv[i].getValueLength()));
                }
                u.setName(Bytes.toString(result.getValue(byteFamilyName, name)));
                u.setPassword(Bytes.toString(result.getValue(byteFamilyName, password)));
                u.setEmail(Bytes.toString(result.getValue(byteFamilyName, email)));
                return u;
            }
        });
        return u;
    }

    public void delete(User user) {
        // 如果有指定字段的话就只会删除指定的记录
        String family = null;
        if (user.getEmail() != null || user.getPassword() != null || user.getName() != null) {
            family = "info";
        }
        final Delete delete = new Delete(Bytes.toBytes(user.getId()));

        if (user.getName() != null) {
            // delete.setAttribute("name",Bytes.toBytes(user.getName()));
            delete.addColumn(byteFamilyName, name);
        }
        if (user.getEmail() != null) {
            delete.addColumn(byteFamilyName, email);

        }
        if (user.getPassword() != null) {
            delete.addColumn(byteFamilyName, password);
        }
        hbaseTemplate.execute(tableName, new TableCallback<Boolean>() {

            public Boolean doInTable(HTableInterface table) throws Throwable {
                table.delete(delete);
                return true;
            }
        });

    }

    private User get(String tableName, String rowName, final String familyName, final String qualifier) {
        User result = new User();
        hbaseTemplate.get(tableName, rowName, familyName, qualifier, new RowMapper<String>() {
            public String mapRow(Result result, int rowNum) throws Exception {
                Cell[]kv=result.rawCells();
                for (int i = 0; i <kv.length ; i++) {
                    System.out.println("版本"+i+Bytes.toString(kv[i].getValueArray(), kv[i].getValueOffset(), kv[i].getValueLength()));
                }

                List<Cell> ceList = result.listCells();
                List<String> resList = new ArrayList();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        resList.add(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }
                for (String val : resList) {
                    System.out.println(val);
                }
                return resList.toString();
            }
        });
        return result;

    }

    private User get(String rowKey) {
        User result = new User();

        return result;

    }
}
