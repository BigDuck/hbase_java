package com.wpj.hbase;


import com.wpj.hbase.Reponstity.StudentReponsity;
import com.wpj.hbase.common.RegionTest;
import com.wpj.hbase.daomain.PageHBase;
import com.wpj.hbase.daomain.PageModel;
import com.wpj.hbase.daomain.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import java.io.IOException;
import java.util.*;

/**
 * Hello world!
 */
public class App {
    public static Configuration config;
    static HbaseTemplate hbaseTemplate;

    static {
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "192.168.254.100");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseTemplate = new HbaseTemplate(config);
    }

    public static void main(String[] args) {
   //    createTable("Stocks");
       // test();
//        String TABLE_NAME = "student";
//        String TB_COMPANY = "company";
       String TB_SHARES = "Stocks";
     //   QueryAll("Stock");
        try {
            Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
            TableName tableName = TableName.valueOf(TB_SHARES);
            HBaseAdmin hBaseAdmin=new HBaseAdmin(connection);
            Table hTable=connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("30018820150331"));
            put.addColumn(Bytes.toBytes("companyName"), null,Bytes.toBytes("美亚柏科"));
            put.addColumn(Bytes.toBytes("date"), null, Bytes.toBytes("20150331"));
            put.addColumn(Bytes.toBytes("tradingVolume"), null,Bytes.toBytes("9620109"));
            put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("startPrice"), Bytes.toBytes("42.440"));
            put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("highestPrice"), Bytes.toBytes("42.440"));
            put.addColumn(Bytes.toBytes("money"), Bytes.toBytes("overPrice"), Bytes.toBytes("42.440"));
            put.addColumn(Bytes.toBytes("money"),Bytes.toBytes("lowestPrice"),Bytes.toBytes("42.440"));
            put.addColumn(Bytes.toBytes("money"),Bytes.toBytes("tradingTotalMoney"),Bytes.toBytes("418659200"));
            hTable.put(put);
            hTable.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        //  get(TABLE_NAME,"ff1b526e-ecf4-491a-8b7a-e201ca300b15");
        //  test();
//        boolean res = putOne(new Student(UUID.randomUUID().toString(), "wpj", "泉州市|福州市"), TABLE_NAME);
//        List<Student> listStudents = new ArrayList<Student>();
//        for (int i = 111; i < 222; i++) {
//            listStudents.add(new Student(UUID.randomUUID().toString(), "吴培基" + i, "厦门市|南安市" + i));
//        }
//        putList(listStudents, TABLE_NAME);
//        System.out.println("---wpjlovehome@gmail.com-----res 值=" + res + "," + "App.main()");
        // 拿出row key的起始行和结束行
        // #<0<9<:
//        String startRow = "1";
//        String stopRow = "ff1b526e-ecf4-491a-8b7a-e201ca300";
//        int currentPage = 1;
//        int pageSize = 10;
//        // 执行hbase查询
//      PageModel pageModel= getDataMap("table", startRow, stopRow, currentPage, pageSize);
//        System.out.println("---wpjlovehome@gmail.com-----pageModel.toString()值=" + pageModel.toString() + "," + "App.main()");

        // QueryAll(TABLE_NAME);
        // QueryByCondition1(TB_COMPANY);
        // findAll(TABLE_NAME);
//        findSome(TABLE_NAME);
        //执行了删除
//         boolean res=  execute(TABLE_NAME);
//        System.out.println("---wpjlovehome@gmail.com-----res值=" + res + "," + "App.main()");
    }

    public static void test() {
        StudentReponsity studentReponsity = new StudentReponsity(hbaseTemplate, "student");
        String res = studentReponsity.get("student", "1", "address", null);
        System.out.println("---wpjlovehome@gmail.com-----res值=" + res + "," + "App.test()");
    }

    public static void createTable(String tableName) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        try {
            HBaseAdmin admin = new HBaseAdmin(hbaseTemplate.getConfiguration());
            tableDescriptor.addFamily(new HColumnDescriptor("money"));//族列
            tableDescriptor.addFamily(new HColumnDescriptor("date"));//族列
            tableDescriptor.addFamily(new HColumnDescriptor("tradingVolume"));//族列
            tableDescriptor.addFamily(new HColumnDescriptor("companyName"));//族列
            RegionTest.createTable(admin,tableDescriptor,RegionTest.getHexSplits("000000","999999",9));

           // admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据 rowkey删除一条记录
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey) {
        try {
            HTable table = new HTable(config, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static boolean putOne(Student student, String tableName) {
        try {
            HTable hTable = new HTable(config, tableName);
            Put put = new Put(Bytes.toBytes(student.getId()));
            put.add(Bytes.toBytes("address"), Bytes.toBytes("home"),
                    Bytes.toBytes(student.getAddress().substring(0, student.getAddress().lastIndexOf("|"))));
            put.add(Bytes.toBytes("address"), Bytes.toBytes("school"),
                    Bytes.toBytes(student.getAddress().substring(student.getAddress().lastIndexOf("|"),
                            student.getAddress().length())));
            put.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes(student.getName()));
            hTable.put(put);
            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void putList(List<Student> students, String tableName) {
        for (Student s : students) {
            try {
                HTable hTable = new HTable(config, tableName);
                Put put = new Put(Bytes.toBytes(s.getId()));
                put.add(Bytes.toBytes("address"), Bytes.toBytes("home"),
                        Bytes.toBytes(s.getAddress().substring(0, s.getAddress().lastIndexOf("|"))));
                put.add(Bytes.toBytes("address"), Bytes.toBytes("school"),
                        Bytes.toBytes(s.getAddress().substring(s.getAddress().lastIndexOf("|"),
                                s.getAddress().length())));
                put.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes(s.getName()));
                hTable.put(put);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    public static PageModel getDataMap(String tableName, String startRow, String stopRow,
                                       Integer currentPage, Integer pageSize) {
        List<Map<String, String>> mapList = null;
        mapList = new LinkedList<Map<String, String>>();
        ResultScanner scanner = null;
        PageModel pageModel = null;
        if (pageSize == null || pageSize == 0L) {
            pageSize = 50;
        }
        if (currentPage == null || currentPage == 0) {
            currentPage = 1;//页码
        }
//计算起始页和结束页
        Integer firstPage = (currentPage - 1) * pageSize;
        Integer endPage = firstPage + pageSize;
        Connection connectionFactory = null;
        try {
            connectionFactory = ConnectionFactory.createConnection();
            Table table = connectionFactory.getTable(TableName.valueOf(tableName));
            Scan scan = getScan(startRow, stopRow);
            // 给筛选对象放入过滤器(true标识分页,具体方法在下面)
            scan.setFilter(packageFilters(true));
            // 缓存1000条数据
            scan.setCaching(1000);
            scan.setCacheBlocks(false);
            scanner = table.getScanner(scan);
            int i = 0;
            List<byte[]> rowList = new LinkedList<byte[]>();
            //遍历扫描器对象，并将要查询的row key取出来
            for (Result result : scanner) {
                String row = toStr(result.getRow());
                if (i >= firstPage && i < endPage) {
                    rowList.add(Bytes.toBytes(row));
                }
                i++;
            }
            // 获取取出的row key的GET对象
            List<Get> getList = getList(rowList);
            Result[] results = table.get(getList);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 获取扫描器对象
    private static Scan getScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        return scan;
    }

    /**
     * 封装查询条件
     */
    private static FilterList packageFilters(boolean isPage) {
        FilterList filterList = null;
        // MUST_PASS_ALL(条件 AND) MUST_PASS_ONE（条件OR）
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = null;
        Filter filter2 = null;
//        filter1 = newFilter(Bytes.toBytes("name"), Bytes.toBytes(""),
//                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("address"));// 当列column1的值为aaa时进行查询

//        filter2 = newFilter(Bytes.toBytes("family2"), Bytes.toBytes("column1"),
//                CompareFilter.CompareOp.LESS, Bytes.toBytes("condition2"));
//        filterList.addFilter(filter1);
//        filterList.addFilter(filter2);
        if (isPage) {
            filterList.addFilter(new FirstKeyOnlyFilter());
        }
        return filterList;
    }

    private static Filter newFilter(byte[] f, byte[] c, CompareFilter.CompareOp op, byte[] v) {
        return new SingleColumnValueFilter(f, c, op, v);
    }

    /* 根据ROW KEY集合获取GET对象集合 */
    private static List<Get> getList(List<byte[]> rowList) {
        List<Get> list = new LinkedList<Get>();
        for (byte[] row : rowList) {
            Get get = new Get(row);
            get.addColumn(Bytes.toBytes("address"), Bytes.toBytes("home"));
            get.addColumn(Bytes.toBytes("address"), Bytes.toBytes("school"));
            get.addColumn(Bytes.toBytes("name"), Bytes.toBytes(""));
            list.add(get);
        }
        return list;
    }

    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    /**
     * 封装每行数据
     */
    private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        for (byte[] key : dataMap.keySet()) {

            byte[] value = dataMap.get(key);

            map.put(toStr(key), toStr(value));

        }
        return map;
    }

    private static String toStr(byte[] bt) {
        return Bytes.toString(bt);
    }

    /**
     * 查询全部
     *
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        Connection connectionFactory = null;
        Table table = null;
        try {
            connectionFactory = ConnectionFactory.createConnection();
            table = connectionFactory.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // HTablePool pool = new HTablePool(config, 1000);
        //  HTable table = (HTable) pool.getTable(tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 171.     * 单条件查询,根据rowkey查询唯一一条记录
     * 172.     * @param tableName
     * 173.
     */
    public static void QueryByCondition1(String tableName) {
        Connection connectionFactory;
        Table table = null;
        try {
            connectionFactory = ConnectionFactory.createConnection();
            table = connectionFactory.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Get scan = new Get("ff1b526e-ecf4-491a-8b7a-e201ca300".getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void findAll(String tableName) {
        hbaseTemplate.find(tableName, "address", new RowMapper<Student>() {
            public Student mapRow(Result result, int rowNum) throws Exception {
                System.out.println("---wpjlovehome@gmail.com-----值=" + toStr(result.getValue(Bytes.toBytes("name"), null)) + "," + "App.mapRow()");
                System.out.println(toStr(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("home"))));
                System.out.println(toStr(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("school"))));
                return null;
            }
        });
    }

    public static void findSome(String tableName) {
        hbaseTemplate.find(tableName, "address", new RowMapper<Student>() {
            public Student mapRow(Result result, int rowNum) throws Exception {
                for (Cell cell : result.rawCells()) {
                    String key = new String(cell.getQualifier());
                    String value = new String(cell.getValue());
                    System.out.println("---wpjlovehome@gmail.com-----key值=" + key + "," + "App.mapRow()");
                    System.out.println("---wpjlovehome@gmail.com-----value值=" + value + "," + "App.mapRow()");
                }
                return null;
            }
        });
    }

    public static boolean execute(String tableName) {
//        用于更新操作
        return hbaseTemplate.execute(tableName, new TableCallback<Boolean>() {
            public Boolean doInTable(HTableInterface table) throws Throwable {
                boolean flag = false;
                try {
                    Delete delete = new Delete(Bytes.toBytes("11"));
                    table.delete(delete);
                    flag = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return flag;
            }
        });

    }

    /**
     * 获取单条记录
     *
     * @param tableName
     * @paramrowKey
     */
    public static void get(String tableName, String rowKey) {
        try {
            HTable hTable = new HTable(config, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
//            get.addColumn("")
            getResult(hTable.get(get));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getResult(Result result) {
        for (Cell cell : result.rawCells()) {
            String rowKey = toStr(result.getRow());
            String family = toStr(CellUtil.cloneFamily(cell));
            String qua = toStr(CellUtil.cloneQualifier(cell));
            String value = toStr(CellUtil.cloneValue(cell));
            String row = toStr(CellUtil.cloneRow(cell));
            System.out.println("[rowkey:" + rowKey + "],[family:" + family + "],[quelifier:" + qua + "],[value:" + value + "],[row:" + row + "]");
        }
    }

    //分页看看
    public static void getPageHBaseData(String tableName, String startRow, String endRow, PageHBase pager) {
        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        if (pager.getNextPageRowkey() == null) pager.setNextPageRowkey(startRow.toString());
        Scan scan = pager.getScan(pager.getNextPageRowkey(), endRow.toString());
        List<Filter> filters = new ArrayList<Filter>();

    }

    /***
     * 几个过滤器简介
     * 提取rowkey以01结尾数据
     * Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*01$"));
     * 提取rowkey以包含201407的数据
     * Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("201407"));
     * 提取rowkey以123开头的数据
     * Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("123".getBytes()));
     */
    /**
     *
     */

}
