package com.bayax.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class HBaseServiceImpl {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private Connection connection;

    public String createTable(String tableName, String... families) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        try  {
            Admin admin = connection.getAdmin();

            for (String family : families) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {

                logger.info("Table:[" + tableName + "] Exists");

                return "Table Exists";

            } else {
                admin.createTable(tableDescriptor);

                logger.info("Create table Successfully!!!Table Name:[" + tableName + "]");


            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());

            return e.getMessage();
        }

        return "Create table Successfully!!!Table Name:[" + tableName + "]";
    }

    public void deleteTable(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName table = TableName.valueOf(tableName);
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                logger.info("[" + tableName + "] is not existed. Delete failed!");
                return;
            }
            admin.disableTable(table);
            admin.deleteTable(table);
            System.out.println("delete table " + tableName + " successfully!");
            logger.info("delete table " + tableName + " successfully!");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void putRowValue(String tableName, String rowKey, String familyColumn, String columnName, String value) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
            logger.info("update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",column:" + columnName + ",value:" + value + " successfully!");
            System.out.println("Update table success");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void putRowValueBatch(String tableName, String rowKey, String familyColumn, List<String> columnNames, List<String> values) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int j = 0; j < columnNames.size(); j++) {
                put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnNames.get(j)), Bytes.toBytes(values.get(j)));
            }
            table.put(put);
            logger.info("update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",columns:" + columnNames + ",values:" + values + " successfully!");
            System.out.println("Update table success");

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void putRowValueBatch(String tableName, String rowKey, String familyColumn, Map<String, String> columnValues) {
        logger.info("begin to update table:" + tableName + ",rowKey:" + rowKey + ",family:" + familyColumn + ",columnValues:" + columnValues.toString());
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : columnValues.entrySet()) {
                put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            table.put(put);
            logger.info("update table:" + tableName + ",rowKey:" + rowKey + " successfully!");
            System.out.println("Update table success");

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public List<Cell> scanRegexRowKey(String tableName, String regexKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexKey));
            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                return r.listCells();
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }

    public void deleteAllColumn(String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delAllColumn = new Delete(Bytes.toBytes(rowKey));
            table.delete(delAllColumn);
            System.out.println("Delete AllColumn Success");
            logger.info("Delete rowKey:" + rowKey + "'s all Columns Successfully");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void deleteColumn(String tableName, String rowKey, String familyName, String columnName) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delColumn = new Delete(Bytes.toBytes(rowKey));
            delColumn.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            table.delete(delColumn);
            System.out.println("Delete Column Success");
            logger.info("Delete rowKey:" + rowKey + "'s Column:" + columnName + " Successfully");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }
}
