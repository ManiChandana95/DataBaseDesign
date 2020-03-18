package Model;

import QueryParser.DbHelper;

public class Col {
    public String str;
    public DataType type;
    public boolean isNull;

    public static Col CreateColumn(String columnStr){
        String primaryKeyStr = "primary key";
        String notNullStr = "not null";
        boolean isNull = true;
        if(columnStr.toLowerCase().endsWith(primaryKeyStr)){
            columnStr = columnStr.substring(0, columnStr.length() - primaryKeyStr.length()).trim();
        }
        else if(columnStr.toLowerCase().endsWith(notNullStr)){
            columnStr = columnStr.substring(0, columnStr.length() - notNullStr.length()).trim();
            isNull = false;
        }

        String[] parts = columnStr.split(" ");
        String name = "";
        if(parts.length > 2){
            DbHelper.UnknownCommand(columnStr, "Expected Column format <name> <datatype> [PRIMARY KEY]/[NOT NULL]");
            return null;
        }

        if(parts.length > 1){
            name = parts[0].trim();
            DataType type = GetDataType(parts[1].trim());
            if(type == null){
                DbHelper.UnknownCommand(columnStr, "Unrecognised Data type " + parts[1]);
                return null;
            }

            Col column = new Col(name, type, isNull);
            return column;
        }

        DbHelper.UnknownCommand(columnStr, "Expected Column format <name> <datatype> [PRIMARY KEY]/[NOT NULL]");
        return null;
    }

    private static DataType GetDataType(String dataTypeString) {
        switch(dataTypeString){
            case "tinyint": return DataType.TINYINT;
            case "smallint": return DataType.SMALLINT;
            case "int": return DataType.INT;
            case "bigint": return DataType.BIGINT;
            case "real": return DataType.REAL;
            case "double": return DataType.DOUBLE;
            case "datetime": return DataType.DATETIME;
            case "date": return DataType.DATE;
            case "text": return DataType.TEXT;
        }

        return null;
    }

    private Col(String str, DataType type, boolean isNull) {
        this.str = str;
        this.type = type;
        this.isNull = isNull;
    }
}
