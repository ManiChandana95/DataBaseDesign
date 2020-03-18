package test;

import common.Constant;
import common.Utils;
import datatypes.DataTypeText;
import datatypes.base.DT;
import datatypes.base.DataTypeNumeral;
import storage.StorageManager;
import storage.model.AllDataRecords;

import java.util.ArrayList;
import java.util.List;

public class Test {

    public void run(int numberOfTestCases) {
        switch (numberOfTestCases) {
            case 1:
                fetchTableColumns(Constant.SYSTEM_COLUMNS_TABLENAME);

            case 2:
                fetchSelectiveTableColumns(Constant.SYSTEM_COLUMNS_TABLENAME);

            case 3:
                selectAll(Constant.SYSTEM_TABLES_TABLENAME);
                break;

            case 4:
                deleteTableName(Constant.SYSTEM_COLUMNS_TABLENAME);

            case 5:
                deleteTableColumns(Constant.SYSTEM_COLUMNS_TABLENAME);
        }
    }

    public void fetchTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        columnIndexList.add((byte) 3);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));
        valueList.add(new DataTypeText("TEXT"));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);
        conditionList.add(DataTypeNumeral.EQUALS);
        List<AllDataRecords> records = manager.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);
        for (AllDataRecords record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    public void fetchSelectiveTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);
        List<Byte> selectionIndexList = new ArrayList<>();
        selectionIndexList.add((byte) 0);
        selectionIndexList.add((byte) 5);
        selectionIndexList.add((byte) 2);
        List<AllDataRecords> records = manager.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, selectionIndexList, false);
        for (AllDataRecords record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void selectAll(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        List<AllDataRecords> records = manager.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,false);
        for (AllDataRecords record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void deleteTableName(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DataTypeText(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DataTypeNumeral.EQUALS);
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,true));
    }

    private void deleteTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList,false));
    }
}
