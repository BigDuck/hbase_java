/*
 * Copyright (c) 2016 - 1 - 10  7 : 15 :58
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Name：Test
 * Time：2016/1/10 19:15
 * author：WPJ587
 * description：
 **/

public class Test {
    private static final String TB_SHARES = "Stock";
    public static Configuration config;
    static HbaseTemplate hbaseTemplate;
static int count=0;
    static {
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "192.168.1.111");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseTemplate = new HbaseTemplate(config);
    }
    public static void main(String[]args){


        mv("F:/dataMessage/");
        //  readFileByLines("F:/dataMessage/600978/6009781.txt");
    }
    public static void mv(String paths){
        // String paths="F:/dataMessage/";
        File path=new File(paths);

        if(path.isDirectory()){
            File[] files=path.listFiles();

            for (int i = 0; i < files.length; i++) {

                File file=files[i];
                if(file.isFile()){
                    readFileByLines(file.getAbsolutePath());
                }
                if(file.isDirectory()){
                    mv(file.getPath());
                }
            }
        }else{
            System.out.println("\n您输入的不是目录！");
        }

    }
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        String row=file.getName().substring(0,6);
        System.out.println("文件名"+row);
        Connection connection = null;
        TableName tableName=null;
        Table hTable=null;
        try {
            connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            tableName   = TableName.valueOf(TB_SHARES);
             hTable   =connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                String arr[]=tempString.split("\\|");

                HBaseAdmin hBaseAdmin=new HBaseAdmin(connection);
                List<Put> list=new ArrayList<Put>();
                for (int i = 0; i <arr.length ; i++) {
                    System.out.println("第"+(++count)+"条");
                    String TB_SHARES = "Stock";
                    if(line!=1){
                        Put put = new Put(Bytes.toBytes(row+arr[0].replace("-","")));
                        put.addColumn(Bytes.toBytes("companyName"), null,Bytes.toBytes(fileName.substring(0,6)));
                        put.addColumn(Bytes.toBytes("date"), null, Bytes.toBytes(arr[0]));
                        put.addColumn(Bytes.toBytes("tradingVolume"), null,Bytes.toBytes(arr[5]));
                        put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("startPrice"), Bytes.toBytes(arr[1]));
                        put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("highestPrice"), Bytes.toBytes(arr[2]));
                        put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("overPrice"), Bytes.toBytes(arr[3]));
                        put.addColumn(Bytes.toBytes("money"),Bytes.toBytes("lowestPrice"),Bytes.toBytes(arr[4]));
                        put.addColumn(Bytes.toBytes("money"),Bytes.toBytes("tradingTotalMoney"),Bytes.toBytes(arr[6]));
                        list.add(put);
                    }

                }
                hTable.put(list);
                line++;

            }
            hTable.close();
            connection.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

}
