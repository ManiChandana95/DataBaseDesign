package queries;

import Model.IQuery;
import Model.Results;
import QueryParser.DbHelper;
import common.Utils;

import java.io.File;

public class DropDatabaseQuery implements IQuery {
    public String databaseName;

    public DropDatabaseQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Results ExecuteQuery() {
        String DEFAULT_DATA_DIRNAME = "data";
        File database = new File(DEFAULT_DATA_DIRNAME + "/" + this.databaseName);
        boolean isDeleted = RecursivelyDelete(database);

        if(!isDeleted){
            Utils.printError(String.format("Unable to delete database '%s'", this.databaseName));
            return null;
        }

        if(DbHelper.CurrentDatabaseName == this.databaseName){
            DbHelper.CurrentDatabaseName = "";
        }

        Results result = new Results(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DbHelper.DatabaseExists(this.databaseName);

        if(!databaseExists){
            Utils.printError(String.format("Database '%s' dosent exist", this.databaseName));
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
