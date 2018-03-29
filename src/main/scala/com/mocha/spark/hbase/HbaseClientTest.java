package com.mocha.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yangxq
 * @version 2017/3/15 14:59
 */
public class HbaseClientTest {

    static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;
    static String tableName = "user-info-java";

    /**
     * 创建客戶端连接
     */
    static {
        //  Configuration conf=new Configuration();    //会加载hbase-site.xml配置文件
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h1:2181,h2:2181,h3:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建表 DDL
     *
     * @throws IOException
     */
    private static void createHbaseTable() throws IOException {
        TableName name = TableName.valueOf(tableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(name);
        HColumnDescriptor base_info = new HColumnDescriptor("base_info").setMaxVersions(3); //创建列名,给列族增加版本约束
        tableDescriptor.addFamily(base_info);    //将列族添加到表描述对象中
        admin.createTable(tableDescriptor);  //用createTable方法创建一个tabelDescriptor所描述的对象
        close();  //关闭连接
        System.out.println("createHbaseTable: " + tableName + " finish");
    }


    /**
     * 查看已有表
     *
     * @throws IOException
     */
    private static void listHbaseTable() throws IOException {
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();  //关闭连接
        System.out.println("createHbaseTable: " + tableName + " finish");
    }


    /**
     * 新增单条数据 DML
     *
     * @throws IOException
     */
    private static void addInfoToHbaseTable() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes("rk-10002"));
        put.addColumn("base_info".getBytes(), "name".getBytes(), "yangxq2".getBytes());
        put.addColumn("base_info".getBytes(), "age".getBytes(), "23".getBytes());
        put.addColumn("base_info".getBytes(), "city".getBytes(), "tianjin".getBytes());
        table.put(put);
        close();  //关闭连接
        System.out.println("addInfoToHbaseTable: " + tableName + " finish");
    }


    /**
     * 批量新增数据 DML
     *
     * @ tableName 表名称
     * @ rowKey RowKey
     * @ colFamily 列族
     * @ col 列
     * @ value 值
     */
    private static void addMoreToHbaseTable() throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<Put>();
        /*Put put1 = new Put(Bytes.toBytes("rk-10003"));
        put1.addColumn("base_info".getBytes(), "name".getBytes(), "xuezhiqian".getBytes());
        put1.addColumn("base_info".getBytes(), "age".getBytes(), "24".getBytes());
        put1.addColumn("base_info".getBytes(), "city".getBytes(), "shanghai".getBytes());*/

        Put put2 = new Put(Bytes.toBytes("rk-10004"));
        put2.addColumn("base_info".getBytes(), "name".getBytes(), "dimaxi".getBytes());
        put2.addColumn("base_info".getBytes(), "age".getBytes(), "26".getBytes());
        put2.addColumn("base_info".getBytes(), "city".getBytes(), "beijin".getBytes());

        //putList.add(put1);
        putList.add(put2);
        table.put(putList);

        close();  //关闭连接
        System.out.println("addInfoToHbaseTable: " + tableName + " finish");
    }


    /**
     * 根据RowKey获取数据
     *
     * @param tableName 表名称
     * @param rowKey    RowKey名称
     * @param colFamily 列族名称
     * @param col       列名称
     * @throws IOException
     */
    public static void getDataFromHbaseTable(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }


    /**
     * 格式化输出
     *
     * @param result
     */
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("timestamp: " + cell.getTimestamp() + " ");
            System.out.println("colFamily: " + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("colName: " + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("colValue: " + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }


    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void deleteFromHbaseTable(String tableName, String rowKey, String colFamily, String col) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " not exists.");
        } else {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
           /* if (colFamily != null) {
                del.addFamily(Bytes.toBytes(colFamily));    //删除colFamily
            }*/
            if (colFamily != null && col != null) {
                del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));    //删除colFamily中的col
            }
            /*
             * 批量删除 List<Delete> deleteList = new ArrayList<Delete>();
             * deleteList.add(delete);
             * table.delete(deleteList);
             */
            table.delete(del);
            table.close();
        }
        close();
        System.out.println("deleteFromHbaseTable: " + tableName + " finish");
    }


    public static void main(String args[]) throws IOException {
        //createHbaseTable();
        //listHbaseTable();
        //addInfoToHbaseTable();
        //addMoreToHbaseTable();
        getDataFromHbaseTable(tableName, "rk-10003", "base_info", "name");
        //deleteFromHbaseTable(tableName, "rk-10004", "base_info", "age");
    }

}
