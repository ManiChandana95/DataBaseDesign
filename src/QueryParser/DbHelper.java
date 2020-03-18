package QueryParser;

import Model.*;
import queries.*;

import java.io.File;
import java.util.ArrayList;

public class DbHelper {

    public static final String SELECT_COMMAND = "SELECT";
    public static final String DROP_TABLE_COMMAND = "DROP TABLE";
    public static final String DROP_DATABASE_COMMAND = "DROP DATABASE";
    public static final String HELP_COMMAND = "HELP";
    public static final String VERSION_COMMAND = "VERSION";
    public static final String EXIT_COMMAND = "EXIT";
    public static final String QUIT_COMMAND = "QUIT";
    public static final String SHOW_TABLES_COMMAND = "SHOW TABLES";
    public static final String SHOW_DATABASES_COMMAND = "SHOW DATABASES";
    public static final String INSERT_COMMAND = "INSERT INTO";
    public static final String DELETE_COMMAND = "DELETE FROM";
    public static final String UPDATE_COMMAND = "UPDATE";
    public static final String CREATE_TABLE_COMMAND = "CREATE TABLE";
    public static final String CREATE_DATABASE_COMMAND = "CREATE DATABASE";
    public static final String USE_DATABASE_COMMAND = "USE";
    public static final String DESC_TABLE_COMMAND = "DESC";
    public static final String DESCRIBE_TABLE_COMMAND = "DESCRIBE";
    private static final String NO_DATABASE_SELECTED_MESSAGE = "No database selected";

    public static String CurrentDatabaseName = "";
    public static String prompt = "davisbase> ";
    private static String version = "v1.0";

    public static IQuery ShowTable() {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new ShowTableQuery(DbHelper.CurrentDatabaseName);
    }

    public static IQuery DropTable(String tableName) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DropTableQuery(DbHelper.CurrentDatabaseName, tableName);
    }

    public static void UnknownCommand(String userCommand, String message) {
        System.out.println("Unrecognized Command " + userCommand);
        System.out.println("Message : " + message);
    }

    public static IQuery SelectQueryHandler(String[] attributes, String tableName, String conditionString) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        boolean isSelectAll = false;
        SelectQuery query;
        ArrayList<String> columns = new ArrayList<>();
        for(String attribute : attributes){
            columns.add(attribute.trim());
        }

        if(columns.size() == 1 && columns.get(0).equals("*")) {
            isSelectAll = true;
            columns = null;
        }

        if(conditionString.equals("")){
            query = new SelectQuery(DbHelper.CurrentDatabaseName, tableName, columns, null, isSelectAll);
            return query;
        }

        Conditn condition = Conditn.CreateCondition(conditionString);
        if(condition == null) return null;

        ArrayList<Conditn> conditionList = new ArrayList<>();
        conditionList.add(condition);
        query = new SelectQuery(DbHelper.CurrentDatabaseName, tableName, columns, conditionList, isSelectAll);
        return query;
    }

    public static void ShowVersionQueryHandler() {
        System.out.println("DavisBaseLite Version " + getVersion());
    }

    private static String getVersion() {
        return version;
    }



    public static void HelpQueryHandler() {
        System.out.println(line("-",100));
        System.out.println("List of all DavisBaseLite comments (case insensitive)");
        System.out.println();
        System.out.println("\tUSE DATABASE database_name;                      Change one database.");
        System.out.println("\tCREATE DATABASE database_name;                   Creates a atabase.");
        System.out.println("\tSHOW DATABASES;                                  Show all atabases.");
        System.out.println("\tDROP DATABASE database_name;                     Remove database.");
        System.out.println("\tSHOW TABLES;                                     Show all tables in the database.");
        System.out.println("\tDESC|DESCRIBE table_name;                        Show table schema.");
        System.out.println("\tCREATE TABLE table_name (                        Creates a table in the database.");
        System.out.println("\tDROP TABLE table_name;                           Remove whole table data");
        System.out.println("\tSELECT                                           Show the data you want to display.");
        System.out.println("\tINSERT INTO table_name                           Inserts data into the table.");
        System.out.println("\tDELETE FROM table_name                           Deletes data from a table.");
        System.out.println("\tUPDATE table_name SET                            Updates data from a table.");
        System.out.println("\tVERSION;                                         Show the program version.");
        System.out.println("\tHELP;                                            Show this help information");
        System.out.println("\tEXIT or QUIT;                                    Exit the program");
        System.out.println(line("-",100));
    }

    public static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static IQuery InsertQueryHandler(String tableName, String columnsString, String valuesList) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        ArrayList<String> columns = null;
        ArrayList<Literals> values = new ArrayList<>();

        if(!columnsString.equals("")) {
            columns = new ArrayList<>();
            String[] columnList = columnsString.split(",");
            for(String column : columnList){
                columns.add(column.trim());
            }
        }

        for(String value : valuesList.split(",")){
            Literals literal = Literals.CreateLiteral(value.trim());
            if(literal == null) return null;
            values.add(literal);
        }

        if(columns != null && columns.size() != values.size()){
            DbHelper.UnknownCommand("", "Number of columns and values don't match");
            return null;
        }

        query = new InsertQuery(DbHelper.CurrentDatabaseName, tableName, columns, values);
        return query;
    }

    public static IQuery DeleteQuery(String tableName, String conditionString) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        if(conditionString.equals("")){
            query = new DeleteQuery(DbHelper.CurrentDatabaseName, tableName, null);
            return query;
        }

        Conditn condition = Conditn.CreateCondition(conditionString);
        if(condition == null) return null;

        query = new DeleteQuery(DbHelper.CurrentDatabaseName, tableName, condition);
        return query;
    }

    public static IQuery UpdateQuery(String tableName, String clauseString, String conditionString) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;

        Conditn clause = Conditn.CreateCondition(clauseString);
        if(clause == null) return null;

        if(clause.operator != Optr.EQUALS){
            DbHelper.UnknownCommand(clauseString, "SET clause should only contain = operator");
            return null;
        }

        if(conditionString.equals("")){
            query = new UpdateQuery(DbHelper.CurrentDatabaseName, tableName, clause.column, clause.value, null);
            return query;
        }

        Conditn condition = Conditn.CreateCondition(conditionString);
        if(condition == null) return null;

        query = new UpdateQuery(DbHelper.CurrentDatabaseName, tableName, clause.column, clause.value, condition);
        return query;
    }

    public static IQuery CreateTableQueryHandler(String tableName, String columnsPart) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        IQuery query = null;
        boolean hasPrimaryKey = false;
        ArrayList<Col> columns = new ArrayList<>();
        String[] columnsList = columnsPart.split(",");

        for(String columnEntry : columnsList){
            Col column = Col.CreateColumn(columnEntry.trim());
            if(column == null) return null;
            columns.add(column);
        }

        if(columnsList.length > 0 && columnsList[0].toLowerCase().endsWith("primary key")){
            if(columns.get(0).type == DataType.INT){
                hasPrimaryKey = true;
            }
            else{
                DbHelper.UnknownCommand(columnsList[0], "PRIMARY KEY has to have INT datatype");
                return null;
            }

        }

        query = new CreateTableSQLQuery(DbHelper.CurrentDatabaseName, tableName, columns, hasPrimaryKey);
        return query;
    }

    public static IQuery DropDatabase(String databaseName) {
        return new DropDatabaseQuery(databaseName);
    }

    public static IQuery ShowDatabase() {
        return new ShowDatabaseQuery();
    }

    public static IQuery UseDatabase(String databaseName) {
        return new UseDatabaseQuery(databaseName);
    }

    public static IQuery CreateDatabase(String databaseName) {
        return new CreateDbQuery(databaseName);
    }

    public static boolean DatabaseExists(String databaseName) {
        String DEFAULT_DATA_DIRNAME = "data";
        File dirFile = new File(DEFAULT_DATA_DIRNAME+ "/" + databaseName);
        return dirFile.exists();
    }

    public static boolean TableExists(String databaseName, String tableName) {
        String DEFAULT_DATA_DIRNAME = "data";
        String DEFAULT_TABLE_EXTENSION = "tbl";

        File tableFile = new File(String.format("%s/%s/%s.%s", DEFAULT_DATA_DIRNAME, databaseName, tableName, DEFAULT_TABLE_EXTENSION));
        return tableFile.exists();
    }

    public static void ExecuteQuery(IQuery query) {
        if(query!= null && query.ValidateQuery()){
            Results result = query.ExecuteQuery();
            if(result != null){
                result.Display();
            }
        }
    }

    public static IQuery DescTableQueryHandler(String tableName) {
        if(DbHelper.CurrentDatabaseName.equals("")){
            System.out.println(DbHelper.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DescTableQuery(DbHelper.CurrentDatabaseName, tableName);
    }
}
