package queries;

import Model.Conditn;
import Model.IQuery;
import Model.Results;
import Model.ResultSets;
import QueryParser.DbHelper;
import common.Constant;
import common.Utils;

import java.util.ArrayList;

public class DescTableQuery implements IQuery {

    public String databaseName;
    public String tableName;

    public DescTableQuery(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Results ExecuteQuery() {

        ArrayList<String> columns = new ArrayList<>();
        columns.add("column_name");
        columns.add("data_type");
        columns.add("column_key");
        columns.add("is_nullable");

        Conditn condition = Conditn.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Conditn> conditionList = new ArrayList<>();
        conditionList.add(condition);
        condition = Conditn.CreateCondition(String.format("table_name = '%s'", this.tableName));
        conditionList.add(condition);

        IQuery query = new SelectQuery(Constant.DEFAULT_CATALOG_DATABASENAME, Constant.SYSTEM_COLUMNS_TABLENAME, columns, conditionList, false);
        if(query.ValidateQuery()) {
            ResultSets resultSet = (ResultSets) query.ExecuteQuery();
            return resultSet;
        }

        return null;
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
}
