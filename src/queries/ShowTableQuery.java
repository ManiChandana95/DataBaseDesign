package queries;

import Model.Conditn;
import Model.IQuery;
import Model.Results;
import Model.ResultSets;
import common.CatalogDatabase;
import common.Constant;

import java.util.ArrayList;

public class ShowTableQuery implements IQuery {

    public String databaseName;

    public ShowTableQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Results ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");

        Conditn condition = Conditn.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Conditn> conditionList = new ArrayList<>();
        conditionList.add(condition);

        IQuery query = new SelectQuery(Constant.DEFAULT_CATALOG_DATABASENAME, Constant.SYSTEM_TABLES_TABLENAME, columns, conditionList, false);
        ResultSets resultSet = (ResultSets) query.ExecuteQuery();
        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }
}
