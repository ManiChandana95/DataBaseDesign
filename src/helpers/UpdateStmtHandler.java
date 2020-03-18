package helpers;

import common.CatalogDatabase;
import common.Constant;
import common.Utils;
import datatypes.DT_Int;
import datatypes.DataTypeText;
import storage.StorageManager;
import storage.model.InternalColumn;
import storage.model.InternalCondition;
import storage.model.AllDataRecords;
import storage.model.Page;

import java.util.ArrayList;
import java.util.List;

public class UpdateStmtHandler {

    public int updateSystemTablesTable(String databaseName, String tableName, int columnCount) {
        
        
        StorageManager manager = new StorageManager();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(CatalogDatabase.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(tableName)));
        List<AllDataRecords> result = manager.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, conditions, true);
        if(result != null && result.size() == 0) {
            int returnValue = 1;
            Page<AllDataRecords> page = manager.getLastRecordAndPage(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME);
           
            AllDataRecords lastRecord = null;
            if (page.getPageRecords().size() > 0) {
                lastRecord = page.getPageRecords().get(0);
            }
            AllDataRecords record = new AllDataRecords();
            if(lastRecord == null) {
                record.setRowIdx(1);
            }
            else {
                record.setRowIdx(lastRecord.getRowIdx() + 1);
            }
            record.getColumnValueList().add(new DT_Int(record.getRowIdx()));
            record.getColumnValueList().add(new DataTypeText(databaseName));
            record.getColumnValueList().add(new DataTypeText(tableName));
            record.getColumnValueList().add(new DT_Int(0));
            if(lastRecord == null) {
                record.getColumnValueList().add(new DT_Int(1));
                record.getColumnValueList().add(new DT_Int(columnCount + 1));
            }
            else {
                DT_Int startingColumnIndex = (DT_Int) lastRecord.getColumnValueList().get(CatalogDatabase.TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID);
                returnValue = startingColumnIndex.getValue();
                record.getColumnValueList().add(new DT_Int(returnValue));
                record.getColumnValueList().add(new DT_Int(returnValue + columnCount));
            }
            record.populateSize();
            if(manager.writeRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, record)) {
                conditions.clear();
                conditions.add(InternalCondition.CreateCondition(CatalogDatabase.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(Constant.SYSTEM_TABLES_TABLENAME)));
                List<Byte> updateColumnsIndexList = new ArrayList<>();
                updateColumnsIndexList.add(CatalogDatabase.TABLES_TABLE_SCHEMA_RECORD_COUNT);
                List<Object> updateValueList = new ArrayList<>();
                updateValueList.add(new DT_Int(1));
                manager.updateRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, conditions, updateColumnsIndexList, updateValueList, true);
            }
            return returnValue;
        }
        else {
            System.out.println("Table already exists!");
            return -1;
        }
    }

    public boolean updateSystemColumnsTable(String databaseName, String tableName, int startingRowId, List<InternalColumn> columns) {
     
        StorageManager manager = new StorageManager();
        if(columns != null && columns.size() == 0) return false;
        int i = 0;
        for(; i < columns.size(); i++) {
            AllDataRecords record = new AllDataRecords();
            record.setRowIdx(startingRowId++);
            record.getColumnValueList().add(new DT_Int(record.getRowIdx()));
            record.getColumnValueList().add(new DataTypeText(databaseName));
            record.getColumnValueList().add(new DataTypeText(tableName));
            record.getColumnValueList().add(new DataTypeText(columns.get(i).getName()));
            record.getColumnValueList().add(new DataTypeText(columns.get(i).getDataType()));
            record.getColumnValueList().add(new DataTypeText(columns.get(i).getStringIsPrimary()));
            record.getColumnValueList().add(new DT_Int(i + 1));
            record.getColumnValueList().add(new DataTypeText(columns.get(i).getStringIsNullable()));
            record.populateSize();
            if (!manager.writeRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, record)) {
                break;
            }
        }
        if(i > 0) {
            List<InternalCondition> conditions = new ArrayList<>();
            conditions.add(InternalCondition.CreateCondition(CatalogDatabase.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(Constant.SYSTEM_COLUMNS_TABLENAME)));
            List<Byte> updateColumnsIndexList = new ArrayList<>();
            updateColumnsIndexList.add(CatalogDatabase.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            List<Object> updateValueList = new ArrayList<>();
            updateValueList.add(new DT_Int(i));
            manager.updateRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, conditions, updateColumnsIndexList, updateValueList, true);
        }
        return true;
    }

    public static int incrementRowCount(String tableName) {
        return updateRowCount(tableName, 1);
    }

    public static int decrementRowCount(String tableName) {
        return updateRowCount(tableName, -1);
    }

    private static int updateRowCount(String tableName, int rowCount) {
        StorageManager manager = new StorageManager();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(CatalogDatabase.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(tableName)));
        List<Byte> updateColumnsIndexList = new ArrayList<>();
        updateColumnsIndexList.add(CatalogDatabase.TABLES_TABLE_SCHEMA_RECORD_COUNT);
        List<Object> updateValueList = new ArrayList<>();
        updateValueList.add(new DT_Int(rowCount));
        return manager.updateRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_TABLES_TABLENAME, conditions, updateColumnsIndexList, updateValueList, true);
    }
}
