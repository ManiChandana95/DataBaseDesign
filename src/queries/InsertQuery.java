package queries;

import Model.IQuery;
import Model.Literals;
import Model.Results;
import common.Constant;
import common.Utils;
import datatypes.*;
import datatypes.base.DT;
import storage.StorageManager;
import storage.model.AllDataRecords;

import java.util.*;

public class InsertQuery implements IQuery {
    public String tableName;
    public ArrayList<String> columns;
    public ArrayList<Literals> values;
    public String databaseName;

    public InsertQuery(String databaseName, String tableName, ArrayList<String> columns, ArrayList<Literals> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
        this.databaseName = databaseName;
    }

    @Override
    public Results ExecuteQuery() {
        StorageManager manager = new StorageManager();
        List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

        AllDataRecords record  = new AllDataRecords();
        generateRecords(record.getColumnValueList(), columnDataTypeMapping, retrievedColumns);

        int rowID = findRowID(manager, retrievedColumns);
        record.setRowIdx(rowID);
        record.populateSize();

        Results result = null;
        boolean status = manager.writeRecord(Utils.getUserDatabasePath(this.databaseName), tableName, record);
        if (status) {
            result = new Results(1);
        }
        else {
            result = new Results(0);
        }

        return result;
    }

    @Override
    public boolean ValidateQuery() {
        StorageManager manager = new StorageManager();
        if (!manager.checkTableExists(Utils.getUserDatabasePath(this.databaseName), tableName)) {
            Utils.printMissingTableError(tableName);
            return false;
        }

        List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

        if (columns == null) {
            if (values.size() < retrievedColumns.size() || values.size() > retrievedColumns.size()) {
                Utils.printError("Column count doesn't match value count at row 1");
                return false;
            }

            Utils utils = new Utils();
            if(!utils.checkDataTypeValidity(columnDataTypeMapping, retrievedColumns, values)) {
                return false;
            }
        }
        else  {
             if (columns.size() > retrievedColumns.size()) {
                Utils.printError("Column count doesn't match value count at row 1");
                return false;
            }

            boolean areColumnsValid = checkColumnValidity(retrievedColumns);
            if (!areColumnsValid) {
                return false;
            }

            boolean areColumnsDataTypeValid = validateColumnDataTypes(columnDataTypeMapping);
            if (!areColumnsDataTypeValid) {
                return false;
            }
        }

        boolean isNullConstraintValid = checkNullConstraint(manager, retrievedColumns);
        if (!isNullConstraintValid) {
            return false;
        }

       boolean isPrimaryKeyConstraintValid = checkPrimaryKeyConstraint(manager, retrievedColumns);
        if (!isPrimaryKeyConstraintValid) {
            return false;
        }

        return true;
    }

    private boolean validateColumnDataTypes(HashMap<String, Integer> columnDataTypeMapping) {
        if (!checkColumnDataTypeValidity(columnDataTypeMapping)) {
            return false;
        }


        return true;
    }

    private boolean checkColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (String tableColumn : columns) {
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
                break;
            }
        }

        if (!columnsValid) {
            Utils.printError("Invalid column '" + invalidColumn + "'");
            return false;
        }

        return true;
    }

    private boolean checkNullConstraint(StorageManager manager, List<String> retrievedColumnNames) {
        HashMap<String, Integer> columnsList = new HashMap<>();

        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                columnsList.put(columns.get(i), i);
            }
        }
        else {
            for (int i = 0; i < retrievedColumnNames.size(); i++) {
                columnsList.put(retrievedColumnNames.get(i), i);
            }
        }

        if (!manager.checkNullConstraint(tableName, columnsList)) {
            return false;
        }

        return true;
    }

    private boolean checkPrimaryKeyConstraint(StorageManager manager, List<String> retrievedColumnNames) {
        String primaryKeyColumnName = manager.getTablePrimaryKey(tableName);
        List<String> columnList = (columns != null) ? columns : retrievedColumnNames;

        if (primaryKeyColumnName.length() > 0) {
                if (columnList.contains(primaryKeyColumnName.toLowerCase())) {
                     int primaryKeyIndex = columnList.indexOf(primaryKeyColumnName);
                    if (!manager.checkIfValueForPrimaryKeyExists(this.databaseName, tableName, Integer.parseInt(values.get(primaryKeyIndex).value))) {
                    } else {
                        Utils.printError("Duplicate entry '" + values.get(primaryKeyIndex).value + "' for key 'PRIMARY'");
                        return false;
                    }
                }
        }

        return true;
    }

    private boolean checkColumnDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping) {
        String invalidColumn = "";

        for (String columnName : columns) {
            int dataTypeIndex = columnDataTypeMapping.get(columnName);
            int idx = columns.indexOf(columnName);
            Literals literal = values.get(idx);

            if (dataTypeIndex != Constant.INVALID_CLASS && dataTypeIndex <= Constant.DOUBLE) {

                boolean isValid = Utils.checkStringToDouble(literal.value);
                if (!isValid) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataTypeIndex == Constant.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataTypeIndex == Constant.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }
        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;

        if (!valid) {
            Utils.printError("Incorrect value for column '" + invalidColumn  + "' at row 1");
            return false;
        }

        return true;
    }

    public void generateRecords(List<Object> columnList, HashMap<String, Integer> columnDataTypeMapping, List<String> retrievedColumns) {
        for (String column : retrievedColumns) {
            if (columns != null) {
                if (columns.contains(column)) {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();

                    int idx = columns.indexOf(column);

                    DT obj = getDataTypeObject(dataType);
                    String val = values.get(idx).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                } else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DT obj = getDataTypeObject(dataType);

                    columnList.add(obj);
                }
            }
            else {
                Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();

                int columnIndex = retrievedColumns.indexOf(column);
                DT obj = getDataTypeObject(dataType);
                String val = values.get(columnIndex).toString();

                obj.setValue(getDataTypeValue(dataType, val));
                columnList.add(obj);
            }
        }
    }

    public DT getDataTypeObject(byte dataType) {

        switch (dataType) {
            case Constant.TINYINT: {
                DataTypeTinyInt obj = new DataTypeTinyInt();
                return obj;
            }
            case Constant.SMALLINT: {
                DataTypeSmallInt obj = new DataTypeSmallInt();
                return obj;
            }
            case Constant.INT: {
                DT_Int obj = new DT_Int();
                return obj;
            }
            case Constant.BIGINT: {
                DataTypeInt obj = new DataTypeInt();
                return obj;
            }
            case Constant.REAL: {
                DataTypeReal obj = new DataTypeReal();
                return obj;
            }
            case Constant.DOUBLE: {
                DataTypeDouble obj = new DataTypeDouble();
                return obj;
            }
            case Constant.DATE: {
                DataTypeDate obj = new DataTypeDate();
                return obj;

            }
            case Constant.DATETIME: {
                DataTypeDateTime obj = new DataTypeDateTime();
                return obj;

            }
            case Constant.TEXT: {
                DataTypeText obj = new DataTypeText();
                return obj;
            }
            default: {
                DataTypeText obj = new DataTypeText();
                return obj;
            }
        }
    }

    public Object getDataTypeValue(byte dataType, String value) {

        switch (dataType) {
            case Constant.TINYINT: {
                return Byte.parseByte(value);
            }
            case Constant.SMALLINT: {
                return Short.parseShort(value);
            }
            case Constant.INT: {
                return Integer.parseInt(value);
            }
            case Constant.BIGINT: {
                return Long.parseLong(value);
            }
            case Constant.REAL: {
                return Float.parseFloat(value);
            }
            case Constant.DOUBLE: {
                return Double.parseDouble(value);
            }
            case Constant.DATE: {
                return new Utils().getDateEpoc(value, true);
            }
            case Constant.DATETIME: {
                return new Utils().getDateEpoc(value, false);
            }
            case Constant.TEXT: {
                return value;
            }
            default: {
                return value;
            }
        }
    }


    private int findRowID (StorageManager manager, List<String> retrievedList) {
        int rowCount = manager.getTableRecordCount(tableName);
        String primaryKeyColumnName = manager.getTablePrimaryKey(tableName);
        if (primaryKeyColumnName.length() > 0) {
            int primaryKeyIndex = (columns != null) ? columns.indexOf(primaryKeyColumnName) : retrievedList.indexOf(primaryKeyColumnName);
            int primaryKeyValue = Integer.parseInt(values.get(primaryKeyIndex).value);

            return primaryKeyValue;
        }
        else {
            return rowCount + 1;
        }
    }
}