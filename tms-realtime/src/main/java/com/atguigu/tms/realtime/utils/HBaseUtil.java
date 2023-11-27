package com.atguigu.tms.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.common.TmsConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Hliang
 * @create 2023-09-25 16:54
 */
public class HBaseUtil {
    private static Connection conn = null;
    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    // 通过饿汉式创建单例模式
    static {
        // 如果没有传入configuration，默认回去类路径下找hbase-default.xml或者hbase-site.xml
        try {
            conn = ConnectionFactory.createConnection();
        } catch (IOException e) {
            logger.error("创建HBase连接失败！");
            e.printStackTrace();
        }
    }

    /**
     * 创建HBase表
     * @param namespace HBase名称空间
     * @param table HBase表名
     * @param columnFamilies HBase的列族
     */
    public static void createTable(String namespace,String table,String...columnFamilies) {
        // 如果当前没有传入列族，打印日志，并且默认创建info列族
        if(columnFamilies == null || columnFamilies.length < 1){
            logger.error("创建HBase表时，未传入列族！默认创建info列族");
            columnFamilies = new String[]{TmsConfig.HBASE_DEFAULT_COLUMN_FAMILY};
        }

        // 创建DDL对象
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            // 判断当前表是否存在，如果存在，输出日志
            if(admin.tableExists(TableName.valueOf(namespace,table))){
                logger.error("HBase的" + namespace + "名称空间下已存在" + table + "表！");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());
            logger.info("HBase在" + namespace + "名称空间下创建" + table + "表成功！");
        } catch (IOException e) {
            logger.error("创建HBase表失败！");
            e.printStackTrace();
        } finally {
            if(admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.error("关闭admin失败！");
                    e.printStackTrace();
                }
            }
        }

    }


    /**
     * 向HBase指定命名空间和指定表下写入一行数据
     * @param namespace 名称空间
     * @param tableName 表名
     * @param put 写入的一行数据
     */
    public static void putRow(String namespace,String tableName,Put put){
        // HBase中的异步批量写入
        BufferedMutatorParams mutatorParams = new BufferedMutatorParams(TableName.valueOf(namespace, tableName));
        // 批量刷写大小5M、超过3000Ms也刷写
        mutatorParams.writeBufferSize(5*1024*1024);
        mutatorParams.setWriteBufferPeriodicFlushTimeoutMs(3000L);
        BufferedMutator bufferedMutator = null;
        try {
            bufferedMutator = conn.getBufferedMutator(mutatorParams);
            bufferedMutator.mutate(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(bufferedMutator != null){
                try {
                    bufferedMutator.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 通过RowKey获取一行
     * @param namespace 名称空间
     * @param tableName 表名
     * @param rowKey rowKey的值
     * @return
     */
    public static JSONObject getRowByPrimaryKey(String namespace,String tableName,String rowKey){
        Table table = null;
        JSONObject jsonObj = null;
        try {
            table = conn.getTable(TableName.valueOf(namespace, tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            Cell[] cells = null;
            if(result != null){
                // 如果没有查询到结果
                cells = result.rawCells();
            }
            if(cells == null || cells.length <= 0){
                logger.warn("名称空间：" + namespace + "下的表" + tableName + "中当前没有rowKey为" + rowKey + "的数据！");
            }else{
                jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    jsonObj.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("关闭table失败！");
                    e.printStackTrace();
                }
            }
        }
        return jsonObj;
    }


    /**
     * 通过外键过滤数据行
     * @param namespace 名称空间
     * @param tableName 表名
     * @param columnFamily 列族
     * @param foreignKeyNameAndValue 外键的名称和值
     * @return
     */
    public static List<JSONObject> getRowByForeignKey(String namespace, String tableName, String columnFamily, Tuple2<String,String> foreignKeyNameAndValue){
        String foreignKeyName = foreignKeyNameAndValue.f0;
        String foreignKeyValue = foreignKeyNameAndValue.f1;
        Table table = null;
        ArrayList<JSONObject> list = new ArrayList<>();
        try {
             table = conn.getTable(TableName.valueOf(namespace, tableName));
            Scan scan = new Scan();
            // 封装过滤条件
            FilterList filterList = new FilterList();
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(foreignKeyName),
                    CompareOperator.EQUAL,
                    Bytes.toBytes(foreignKeyValue)
            );
            // 如果当前列不存在，则不反回结果（因为HBase过滤匹配是按照列的值进行的，默认不会管该列是否存在）
            singleColumnValueFilter.setFilterIfMissing(true);

            filterList.addFilter(singleColumnValueFilter);
            scan.setFilter(filterList);

            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iter = scanner.iterator();
            while (iter.hasNext()){
                Result result = iter.next();
                Cell[] cells = result.rawCells();
                JSONObject jsonObj = new JSONObject();
                // 补全主键id
                String rowKey = Bytes.toString(result.getRow());
                jsonObj.put("id",rowKey);
                // 补全其它属性
                for (Cell cell : cells) {
                    jsonObj.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
                }

                list.add(jsonObj);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

    public static void main(String[] args) {
//        System.out.println(getRowByPrimaryKey(TmsConfig.HBASE_NAMESPACE, "dim_user_info", "99"));
        System.out.println(getRowByForeignKey(TmsConfig.HBASE_NAMESPACE, "dim_base_organ", "info",Tuple2.of("region_id","210114")));

    }
}