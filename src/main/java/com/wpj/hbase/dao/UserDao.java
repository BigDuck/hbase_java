package com.wpj.hbase.dao;

import com.wpj.hbase.daomain.User;

import java.io.IOException;
import java.util.List;

/**
 * Created by WPJ587 on 2016/11/3.
 */
public interface UserDao {
    void save(User user) throws IOException;

    User getUser(User user);

    User getByRowKey(final String rowkey);

    void delete(User user);
    void addOrUpdate(final List<User> users);
    void addOrUpdate(final User users);


}
