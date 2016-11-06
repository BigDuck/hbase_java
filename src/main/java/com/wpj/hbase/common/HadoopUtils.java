/*
 * Copyright (c) 2016 - 1 - 10  6 : 59 :59
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HadoopUtils {

  private static Configuration conf;
  public  static void initialConf(){
    conf = new Configuration();
    conf.set("mapred.job.tracker", "192.168.254.100:9001");
    conf.set("fs.default.name", "192.168.254.100:9000");
    conf.set("hbase.zookeeper.quorum", "192.168.254.100");
  }
  public  static void initialConf(String host){
    conf = new Configuration();
    conf.set("mapred.job.tracker", host+":9001");
    conf.set("fs.default.name", host+":9000");
    conf.set("hbase.zookeeper.quorum", host);
  }
  public static Configuration getConf(){
    if(conf==null){
      initialConf();
    }
    return conf;
  }
  
  public static List<String> readFromHDFS(String fileName) throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(URI.create(fileName), conf);
    FSDataInputStream hdfsInStream = fs.open(new Path(fileName));
    // 按行读取（新版本的方法）
    LineReader inLine = new LineReader(hdfsInStream, conf);
    Text txtLine = new Text();
    
    int iResult = inLine.readLine(txtLine); //读取第一行
    List<String> list = new ArrayList<String>();
    while (iResult > 0 ) {
      list.add(txtLine.toString());
      iResult = inLine.readLine(txtLine);
    }
    
    hdfsInStream.close();
    fs.close();
    return list;
  }

  public static void main(String[] args) {
    try {
      readFromHDFS("shork");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}