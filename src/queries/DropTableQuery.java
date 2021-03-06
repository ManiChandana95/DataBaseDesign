package queries;

import Model.Conditn;
import Model.IQuery;
import Model.Results;
import QueryParser.DbHelper;
import common.Utils;

import java.io.File;

public class DropTableQuery implements IQuery {
    public String databaseName;
    public String tableName;

    public DropTableQuery(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Results ExecuteQuery() {
        String DEFAULT_DATA_DIRNAME = "data";
        String CATALOG_TABLE = "davisbase_tables";
        String CATALOG_COLUMNS = "davisbase_columns";
        String CATALOG_DATABASE = "catalog";
        String TABLE_FILE_EXTENSION = "tbl";

        Conditn condition = Conditn.CreateCondition(String.format("table_name = '%s'", this.tableName));
        IQuery deleteEntryQuery = new DeleteQuery(CATALOG_DATABASE, CATALOG_TABLE, condition, true);
        DbHelper.ExecuteQuery(deleteEntryQuery);

        condition = condition = Conditn.CreateCondition(String.format("table_name = '%s'", this.tableName));
        deleteEntryQuery = deleteEntryQuery = new DeleteQuery(CATALOG_DATABASE, CATALOG_COLUMNS, condition, true);
        DbHelper.ExecuteQuery(deleteEntryQuery);

        File table = new File(String.format("%s/%s/%s.%s", DEFAULT_DATA_DIRNAME, this.databaseName, this.tableName, TABLE_FILE_EXTENSION));
        boolean isDeleted = RecursivelyDelete(table);

        if(!isDeleted){
            Utils.printError(String.format("Unable to delete table '%s.%s'", this.databaseName, this.tableName));
            return null;
        }


        Results result = new Results(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean tableExists = DbHelper.TableExists(this.databaseName, this.tableName);

        if(!tableExists){
            Utils.printError(String.format("Unknown table '%s.%s'", this.databaseName, this.tableName));
            return false;
        }

        return true;
    }

    public boolean RecursivelyDelete(File file){
        if(file == null) return true;
        boolean isDeleted = false;

        if(file.isDirectory()) {
            for (File childFile : file.listFiles()) {
                if (childFile.isFile()) {
                    isDeleted = childFile.delete();
                    if (!isDeleted) return false;
                } else {
                    isDeleted = RecursivelyDelete(childFile);
                    if (!isDeleted) return false;
                }
            }
        }

        return file.delete();
    }
}
