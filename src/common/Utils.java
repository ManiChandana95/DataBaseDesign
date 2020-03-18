package common;

import Model.Conditn;
import Model.Literals;
import Model.Optr;
import datatypes.*;
import datatypes.base.DataTypeNumeral;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class Utils {


    public static String getVersion() {
        return Constant.VERSION;
    }



    public static void displayVersion() {
        System.out.println("DanielBaseLite Version " + getVersion());
       
    }

    public static String getSystemDatabasePath() {
        return Constant.DEFAULT_DATA_DIRNAME + "/" + Constant.DEFAULT_CATALOG_DATABASENAME;
    }

    public static String getUserDatabasePath(String database) {
        return Constant.DEFAULT_DATA_DIRNAME + "/" + database;
    }

    public static void printError(String errorMessage) {
        printMessage(errorMessage);
    }

    public static void printMissingDBError(String databaseName) {
        printError("The database '" + databaseName + "' does not exist");
    }

    public static void printMissingTableError(String tableName) {
        printError("Table '" + tableName + "' doesn't exist.");
    }

    public static void printDuplicateTableError(String tableName) {
        printError("Table '" + tableName + "' already exist.");
    }

    public static void printMessage(String str) {
        System.out.println(str);
    }

    public static void printUnknownColumnValueError(String value) {
        printMessage("Unknown column value '" + value + "' in 'value list'");
    }

    public static void printUnknownConditionValueError(String value) {
        printMessage("Unknown column value '" + value + "' in 'value list'");
    }

    public static byte resolveClass(Object object) {
        if(object.getClass().equals(DataTypeTinyInt.class)) {
            return Constant.TINYINT;
        }
        else if(object.getClass().equals(DataTypeSmallInt.class)) {
            return Constant.SMALLINT;
        }
        else if(object.getClass().equals(DT_Int.class)) {
            return Constant.INT;
        }
        else if(object.getClass().equals(DataTypeInt.class)) {
            return Constant.BIGINT;
        }
        else if(object.getClass().equals(DataTypeReal.class)) {
            return Constant.REAL;
        }
        else if(object.getClass().equals(DataTypeDouble.class)) {
            return Constant.DOUBLE;
        }
        else if(object.getClass().equals(DataTypeDate.class)) {
            return Constant.DATE;
        }
        else if(object.getClass().equals(DataTypeDateTime.class)) {
            return Constant.DATETIME;
        }
        else if(object.getClass().equals(DataTypeText.class)) {
            return Constant.TEXT;
        }
        else {
            return Constant.INVALID_CLASS;
        }
    }

    public static byte stringToDataType(String string) {
        if(string.compareToIgnoreCase("TINYINT") == 0) {
            return Constant.TINYINT;
        }
        else if(string.compareToIgnoreCase("SMALLINT") == 0) {
            return Constant.SMALLINT;
        }
        else if(string.compareToIgnoreCase("INT") == 0) {
            return Constant.INT;
        }
        else if(string.compareToIgnoreCase("BIGINT") == 0) {
            return Constant.BIGINT;
        }
        else if(string.compareToIgnoreCase("REAL") == 0) {
            return Constant.REAL;
        }
        else if(string.compareToIgnoreCase("DOUBLE") == 0) {
            return Constant.DOUBLE;
        }
        else if(string.compareToIgnoreCase("DATE") == 0) {
            return Constant.DATE;
        }
        else if(string.compareToIgnoreCase("DATETIME") == 0) {
            return Constant.DATETIME;
        }
        else if(string.compareToIgnoreCase("TEXT") == 0) {
            return Constant.TEXT;
        }
        else {
            return Constant.INVALID_CLASS;
        }
    }

    public static boolean checkStringToDouble(String value) {
        try {
            Double dVal = Double.parseDouble(value);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public static boolean isvalidDateFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }

    public static boolean isvalidDateTimeFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            
            return false;
        }

        return true;
    }

    public static Short ConvertFromOperator(Optr operator) {
        switch (operator){
            case EQUALS: return DataTypeNumeral.EQUALS;
            case GREATER_THAN_EQUAL: return DataTypeNumeral.GREATER_THAN_EQUALS;
            case GREATER_THAN: return DataTypeNumeral.GREATER_THAN;
            case LESS_THAN_EQUAL: return DataTypeNumeral.LESS_THAN_EQUALS;
            case LESS_THAN: return DataTypeNumeral.LESS_THAN;
        }

        return null;
    }

 
    public static String line(String s, int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static boolean checkValueDataTypeValid(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, Conditn condition) {
        String invalidColumn = "";
        Literals literal = null;

        if (columnsList.contains(condition.column)) {
            int dataTypeIndex = columnDataTypeMapping.get(condition.column);
            literal = condition.value;

          
            if (dataTypeIndex != Constant.INVALID_CLASS && dataTypeIndex <= Constant.DOUBLE) {
                
                if (!Utils.checkStringToDouble(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constant.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constant.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            }
        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printUnknownConditionValueError(literal.value);
        }

        return valid;
    }


    public static String getDateEpocAsString(long value, Boolean isDate) {
        ZoneId zoneId = ZoneId.of ( "America/Chicago" );

        Instant i = Instant.ofEpochSecond (value);
        ZonedDateTime zdt2 = ZonedDateTime.ofInstant (i, zoneId);
        Date date = Date.from(zdt2.toInstant());

        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        formatter.setLenient(false);

        String dateStr = formatter.format(date);
        return dateStr;
    }
    

    public static long getDateEpoc(String value, Boolean isDate) {
        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        formatter.setLenient(false);
        Date date;
        try {
            date = formatter.parse(value);

            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(),
                    ZoneId.systemDefault());

     
            return zdt.toInstant().toEpochMilli() / 1000;
        }
        catch (ParseException ex) {
            return 0;
        }
    }

    public boolean checkDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, List<Literals> values) {
        String invalidColumn = "";
        Literals invalidLiteral = null;

        for (String columnName : columnsList) {

           int dataType_Id = columnDataTypeMapping.get(columnName);

            int idx = columnsList.indexOf(columnName);
            Literals literal = values.get(idx);
            invalidLiteral = literal;
  if (dataType_Id != Constant.INVALID_CLASS && dataType_Id <= Constant.DOUBLE) {
                boolean isValid = Utils.checkStringToDouble(literal.value);
                if (!isValid) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataType_Id == Constant.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            } else if (dataType_Id == Constant.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }

        }

        boolean chkValid = (invalidColumn.length() > 0) ? false : true;
        if (!chkValid) {
            Utils.printUnknownColumnValueError(invalidLiteral.value);
            return false;
        }

        return true;
    }
}
