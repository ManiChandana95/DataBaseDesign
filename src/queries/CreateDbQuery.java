package queries;

import Model.IQuery;
import Model.Results;
import QueryParser.DbHelper;
import common.Constant;

import java.io.File;

public class CreateDbQuery implements IQuery {
    public String databaseName;

    public CreateDbQuery(String dbName){
        this.databaseName = dbName;
    }

    @Override
    public Results ExecuteQuery() {
        File db = new File(Constant.DEFAULT_DATA_DIRNAME + "/" + this.databaseName);
        boolean isCreated = db.mkdir();

        if(!isCreated){
            System.out.println(String.format("Unable to create database '%s'", this.databaseName));
            return null;
        }

        Results result = new Results(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DbHelper.DatabaseExists(this.databaseName);

        if(databaseExists){
            System.out.println(String.format("Database '%s' already exists", this.databaseName));
            return false;
        }

        return true;
    }
}
