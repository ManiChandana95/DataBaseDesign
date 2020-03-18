import Model.IQuery;
import QueryParser.DbHelper;
import common.CatalogDatabase;
import common.Constant;

import java.io.File;
import java.util.Scanner;

public class UserPrompt {

  private static boolean isExit = false;
  private static Scanner scanner = new Scanner(System.in).useDelimiter(";");
  private static final String USE_HELP_MESSAGE = "Please use 'help' to see a list of commands";
 static String copyright = "Copy right : Chris Irwin Davis";

    public static void main(String[] args) {

        InitializeDatabase();
		splashScreen();

        while(!isExit) {
            System.out.print(DbHelper.prompt);
            String userCommand = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
    }

    private static void splashScreen() {
        System.out.println(DbHelper.line("-",100));
        System.out.println("Welcome to the DavisBaseLite monitor. Commands end with ;"); 
		System.out.println(getCopyright());
        DbHelper.ShowVersionQueryHandler();
        System.out.println(" ");
        System.out.println("Type 'help;' for help.");
        System.out.println(DbHelper.line("-",100));
    }

    
	public static String getCopyright() {
		return copyright;
	}
    private static void parseUserCommand (String userCommand) {
        if(userCommand.toLowerCase().equals(DbHelper.SHOW_TABLES_COMMAND.toLowerCase())){
            IQuery query = DbHelper.ShowTable();
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(DbHelper.SHOW_DATABASES_COMMAND.toLowerCase())){
            IQuery query = DbHelper.ShowDatabase();
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(DbHelper.HELP_COMMAND.toLowerCase())){
            DbHelper.HelpQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(DbHelper.VERSION_COMMAND.toLowerCase())){
            DbHelper.ShowVersionQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(DbHelper.EXIT_COMMAND.toLowerCase()) ||
                userCommand.toLowerCase().equals(DbHelper.QUIT_COMMAND.toLowerCase())){

            System.out.println("Exit Database");
            isExit = true;
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.USE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.USE_DATABASE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DbHelper.USE_DATABASE_COMMAND.length());
            IQuery query = DbHelper.UseDatabase(databaseName.trim());
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.DESC_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.DESC_TABLE_COMMAND) && !PartsEqual(userCommand, DbHelper.DESCRIBE_TABLE_COMMAND)) {
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName;
            if(userCommand.toLowerCase().startsWith(DbHelper.DESCRIBE_TABLE_COMMAND.toLowerCase()))
                tableName = userCommand.substring(DbHelper.DESCRIBE_TABLE_COMMAND.length());
            else
                tableName = userCommand.substring(DbHelper.DESC_TABLE_COMMAND.length());
            IQuery query = DbHelper.DescTableQueryHandler(tableName.trim());
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.DROP_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.DROP_TABLE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = userCommand.substring(DbHelper.DROP_TABLE_COMMAND.length());
            IQuery query = DbHelper.DropTable(tableName.trim());
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.DROP_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.DROP_DATABASE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DbHelper.DROP_DATABASE_COMMAND.length());
            IQuery query = DbHelper.DropDatabase(databaseName.trim());
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.SELECT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.SELECT_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            int index = userCommand.toLowerCase().indexOf("from");
            if(index == -1) {
                DbHelper.UnknownCommand(userCommand, "Expected FROM keyword");
                return;
            }

            String attributeList = userCommand.substring(DbHelper.SELECT_COMMAND.length(), index).trim();
            String restUserQuery = userCommand.substring(index + "from".length());

            index = restUserQuery.toLowerCase().indexOf("where");
            if(index == -1) {
                String tableName = restUserQuery.trim();
                IQuery query = DbHelper.SelectQueryHandler(attributeList.split(","), tableName, "");
                DbHelper.ExecuteQuery(query);
                return;
            }

            String tableName = restUserQuery.substring(0, index);
            String conditions = restUserQuery.substring(index + "where".length());
            IQuery query = DbHelper.SelectQueryHandler(attributeList.split(","), tableName.trim(), conditions);
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.INSERT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.INSERT_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String columns = "";

            int valuesIndex = userCommand.toLowerCase().indexOf("values");
            if(valuesIndex == -1) {
                DbHelper.UnknownCommand(userCommand, "Expected VALUES keyword");
                return;
            }

            String columnOptions = userCommand.toLowerCase().substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                tableName = userCommand.substring(DbHelper.INSERT_COMMAND.length(), openBracketIndex).trim();
                int closeBracketIndex = userCommand.indexOf(")");
                if(closeBracketIndex == -1) {
                    DbHelper.UnknownCommand(userCommand, "Expected ')'");
                    return;
                }

                columns = userCommand.substring(openBracketIndex + 1, closeBracketIndex).trim();
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DbHelper.INSERT_COMMAND.length(), valuesIndex).trim();
            }

            String valuesList = userCommand.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
                DbHelper.UnknownCommand(userCommand, "Expected '('");
                return;
            }

            if(!valuesList.endsWith(")")){
                DbHelper.UnknownCommand(userCommand, "Expected ')'");
                return;
            }

            valuesList = valuesList.substring(1, valuesList.length()-1);
            IQuery query = DbHelper.InsertQueryHandler(tableName, columns, valuesList);
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.DELETE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.DELETE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String condition = "";
            int index = userCommand.toLowerCase().indexOf("where");
            if(index == -1) {
                tableName = userCommand.substring(DbHelper.DELETE_COMMAND.length()).trim();
                IQuery query = DbHelper.DeleteQuery(tableName, condition);
                DbHelper.ExecuteQuery(query);
                return;
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(DbHelper.DELETE_COMMAND.length(), index).trim();
            }

            condition = userCommand.substring(index + "where".length());
            IQuery query = DbHelper.DeleteQuery(tableName, condition);
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.UPDATE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.UPDATE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String conditions = "";
            int setIndex = userCommand.toLowerCase().indexOf("set");
            if(setIndex == -1) {
                DbHelper.UnknownCommand(userCommand, "Expected SET keyword");
                return;
            }

            String tableName = userCommand.substring(DbHelper.UPDATE_COMMAND.length(), setIndex).trim();
            String clauses = userCommand.substring(setIndex + "set".length());
            int whereIndex = userCommand.toLowerCase().indexOf("where");
            if(whereIndex == -1){
                IQuery query = DbHelper.UpdateQuery(tableName, clauses, conditions);
                DbHelper.ExecuteQuery(query);
                return;
            }

            clauses = userCommand.substring(setIndex + "set".length(), whereIndex).trim();
            conditions = userCommand.substring(whereIndex + "where".length());
            IQuery query = DbHelper.UpdateQuery(tableName, clauses, conditions);
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.CREATE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.CREATE_DATABASE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(DbHelper.CREATE_DATABASE_COMMAND.length());
            IQuery query = DbHelper.CreateDatabase(databaseName.trim());
            DbHelper.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(DbHelper.CREATE_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, DbHelper.CREATE_TABLE_COMMAND)){
                DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
                return;
            }

            int openBracketIndex = userCommand.toLowerCase().indexOf("(");
            if(openBracketIndex == -1) {
                QueryParser.DbHelper.UnknownCommand(userCommand, "Expected (");
                return;
            }

            if(!userCommand.endsWith(")")){
                QueryParser.DbHelper.UnknownCommand(userCommand, "Missing )");
                return;
            }

            String tableName = userCommand.substring(DbHelper.CREATE_TABLE_COMMAND.length(), openBracketIndex).trim();
            String columnsPart = userCommand.substring(openBracketIndex + 1, userCommand.length()-1);
            IQuery query = DbHelper.CreateTableQueryHandler(tableName, columnsPart);
            DbHelper.ExecuteQuery(query);
        }
        else{
            DbHelper.UnknownCommand(userCommand, USE_HELP_MESSAGE);
        }
    }

    private static boolean PartsEqual(String userCommand, String expectedCommand) {
        String[] userParts = userCommand.toLowerCase().split(" ");
        String[] actualParts = expectedCommand.toLowerCase().split(" ");

        for(int i=0;i<actualParts.length;i++){
            if(!actualParts[i].equals(userParts[i])){
                return false;
            }
        }

        return true;
    }

    private static void InitializeDatabase() {
        File baseDir = new File(Constant.DEFAULT_DATA_DIRNAME);
        if(!baseDir.exists()) {
            File catalogDir = new File(Constant.DEFAULT_DATA_DIRNAME + "/" + Constant.DEFAULT_CATALOG_DATABASENAME);
            if(!catalogDir.exists()) {
                if(catalogDir.mkdirs()) {
                    new CatalogDatabase().createCatalogDB();
                }
            }
        }

    }
}