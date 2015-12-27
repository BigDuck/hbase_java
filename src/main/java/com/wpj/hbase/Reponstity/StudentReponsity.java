/*
 * Copyright (c) 2015 - 12 - 27  4 : 4 :7
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase.Reponstity;

import com.wpj.hbase.daomain.PageHBase;
import com.wpj.hbase.daomain.Student;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import java.io.IOException;
import java.util.*;

/**
 * Name：StudentReponsity
 * Time：2015/12/27 16:04
 * author：WPJ587
 * description：学生操作
 **/

public class StudentReponsity {
    private HbaseTemplate hbaseTemplate;
    private String tableName;
    private byte[] name,id,address,home,school;
    public static byte[] ADDRESS = Bytes.toBytes("address");

    public StudentReponsity(HbaseTemplate hbaseTemplate, String tableName) {
        this.hbaseTemplate = hbaseTemplate;
        this.tableName = tableName;
        name=Bytes.toBytes("name");
        id=Bytes.toBytes("id");
        address=Bytes.toBytes("address");
        home=Bytes.toBytes("home");
        school=Bytes.toBytes("school");
    }

    public String get(String tableName ,String rowName, String familyName, String qualifier) {
        return hbaseTemplate.get(tableName, rowName,familyName,qualifier ,new RowMapper<String>(){
            public String mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList =   result.listCells();
                String res = "";
                if(ceList!=null&&ceList.size()>0){
                    for(Cell cell:ceList){
                        res = Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    }
                }
                return res;
            }
        });

    }

}

