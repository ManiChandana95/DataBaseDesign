package queries;

import Model.*;
import common.CatalogDatabase;
import common.Constant;
import common.Utils;
import datatypes.DataTypeText;
import datatypes.base.DT;
import javafx.util.Pair;
import storage.StorageManager;
import storage.model.AllDataRecords;
import storage.model.InternalCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SelectQuery implements IQuery {
    public String database_Name;
    public String table_Name;
    public ArrayList<String> columns;
    public boolean isSelectAll;
    public ArrayList<Conditn> conditions = new ArrayList<>();

    
    public SelectQuery(String db_Name, String tableName, ArrayList<String> columns, ArrayList<Conditn> conditions, boolean isSelectAll) {
        this.database_Name = db_Name;
        this.table_Name = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.isSelectAll = isSelectAll;
    }

    @Override
    public Results ExecuteQuery() {
        ResultSets resultSet = ResultSets.CreateResultSet();

        ArrayList<Records> records = GetData();
        resultSet.setColumns(this.columns);
        for(Records record : records){
            resultSet.addRecords(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.table_Name);
        HashMap<String, Integer> columnToIdMap = maps.getKey();
        StorageManager manager = new StorageManager();

        if (!manager.checkTableExists(Utils.getUserDatabasePath(this.database_Name), table_Name)) {
            Utils.printMissingTableError(table_Name);
            return false;
        }

        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(table_Name);

        if (conditions != null) {
            List<String> retrievedColumns = manager.fetchAllTableColumns(table_Name);

            for (Conditn condition : conditions) {
                if (!Utils.checkValueDataTypeValid(columnDataTypeMapping, retrievedColumns, condition)) {
                    return false;
                }
            }
        }

        if(this.columns != null){
            for(String column : this.columns){
                if(!columnToIdMap.containsKey(column)){
                    Utils.printError(String.format("Unknown column '%s' in table '%s'", column, this.table_Name));
                    return false;
                }
            }
        }

        if(conditions != null) {
            for (Conditn condition : conditions) {
                if (!columnToIdMap.containsKey(condition.column)) {
                    Utils.printError((String.format("Unknown column '%s' in table '%s'", condition.column, this.table_Name)));
                    return false;
                }
            }
        }

        return true;
    }

    private ArrayList<Records> GetData(){
        ArrayList<Records> records = new ArrayList<>();
        Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.table_Name);
        HashMap<String, Integer> columnToIdMap = maps.getKey();
        ArrayList<Byte> columnsList = new ArrayList<>();
        List<AllDataRecords> internalRecords;
        StorageManager manager = new StorageManager();

        InternalCondition internalCondition = null;

        if(this.conditions != null){
            for(Conditn condition : this.conditions) {
                internalCondition = new InternalCondition();
                if (columnToIdMap.containsKey(condition.column)) {
                    internalCondition.setIndex(columnToIdMap.get(condition.column).byteValue());
                }

                DT dataType = DT.CreateDT(condition.value);
                internalCondition.setValue(dataType);

                Short operatorShort = Utils.ConvertFromOperator(condition.operator);
                internalCondition.setConditionType(operatorShort);
            }
        }

        if(this.columns == null) {
            internalRecords = manager.findRecord(Utils.getUserDatabasePath(this.database_Name),
                    this.table_Name, internalCondition, false);

            HashMap<Integer, String> idToColumnMap = maps.getValue();
            this.columns = new ArrayList<>();
            for (int i=0; i<columnToIdMap.size();i++) {
                if(idToColumnMap.containsKey(i)){
                    columnsList.add((byte)i);
                    this.columns.add(idToColumnMap.get(i));
                }
            }
        }
        else {
            for (String column : this.columns) {
                if (columnToIdMap.containsKey(column)) {
                    columnsList.add(columnToIdMap.get(column).byteValue());
                }
            }

            internalRecords = manager.findRecord(Utils.getUserDatabasePath(this.database_Name),
                    this.table_Name, internalCondition, columnsList, false);
        }

        Byte[] columnIds = new Byte[columnsList.size()];
        int k = 0;
        for(Byte column : columnsList){
            columnIds[k] = column;
            k++;
        }

        HashMap<Integer, String> idToColumnMap = maps.getValue();
        Arrays.sort(columnIds);
        for(AllDataRecords internalRecord : internalRecords){
            Object[] dataTypes = new DT[internalRecord.getColumnValueList().size()];
            k=0;
            for(Object columnValue : internalRecord.getColumnValueList()){
                dataTypes[k] = columnValue;
                k++;
            }
            Records record = Records.CreateRecord();
            for(int i=0;i<columnIds.length;i++) {
                Literals literal;
                if(idToColumnMap.containsKey((int)columnIds[i])) {
                    literal = Literals.CreateLiteral((DT)dataTypes[i], Utils.resolveClass(dataTypes[i]));
                    record.put(idToColumnMap.get((int)columnIds[i]), literal);
                }
            }
            records.add(record);
        }

        return records;
    }


    private Pair<HashMap<String, Integer>, HashMap<Integer, String>> mapOrdinalIdToColumnName(String tableName) {
        HashMap<Integer, String> idToColumnMap = new HashMap<>();
        HashMap<String, Integer> columnToIdMap = new HashMap<>();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(CatalogDatabase.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataTypeText(tableName)));

        StorageManager manager = new StorageManager();
        List<AllDataRecords> records = manager.findRecord(Utils.getSystemDatabasePath(), Constant.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            AllDataRecords record = records.get(i);
            Object object = record.getColumnValueList().get(CatalogDatabase.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            idToColumnMap.put(i, ((DT) object).getStringValue());
            columnToIdMap.put(((DT) object).getStringValue(), i);
        }

        return new Pair<>(columnToIdMap, idToColumnMap);
    }
}
