package queries;

import Model.*;

import java.io.File;
import java.util.ArrayList;


public class ShowDatabaseQuery implements IQuery {
    @Override
    public Results ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("Database");
        ResultSets resultSet = ResultSets.CreateResultSet();
        resultSet.setColumns(columns);
        ArrayList<Records> records = DummyData();

        for(Records record : records){
            resultSet.addRecords(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }

    private ArrayList<Records> DummyData(){
        ArrayList<Records> records = new ArrayList<>();

        String DEFAULT_DATA_DIRNAME = "data";
        File baseData = new File(DEFAULT_DATA_DIRNAME);

        for(File data : baseData.listFiles()){
            if(!data.isDirectory()) continue;
            Records record = Records.CreateRecord();
            record.put("Database", Literals.CreateLiteral(String.format("\"%s\"", data.getName())));
            records.add(record);
        }

        return records;
    }
}
