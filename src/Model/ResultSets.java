package Model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ResultSets extends Results{
    private ArrayList<String> col;
    private ArrayList<Records> records;

    public ResultSets(int rowsAffected) {
        super(rowsAffected);
        this.records = new ArrayList<>();
    }

    public static ResultSets CreateResultSet(){
        return new ResultSets(0);
    }

    public void addRecords(Records record){
        if(record == null) return;

        if(this.records == null) {
            this.records = new ArrayList<>();
        }

        this.records.add(record);
        this.rowsAffected++;
    }

    public void setColumns(ArrayList<String> columns){
        this.col = columns;
    }

    @Override
    public void Display(){
        if(this.col == null && this.col.size() == 0) return;
        HashMap<String, Integer> columnSizeMap = new HashMap<>();

        if(this.records == null || this.records.size() == 0){
            System.out.println("Empty Set");
            return;
        }

        for(String column : this.col){
            int maxLength = column.length();
            for(Records record : records){
                if(record.valueMap.containsKey(column)){
                    String value = record.valueMap.get(column).toString();
                    if(value.length() > maxLength){
                        maxLength = value.length();
                    }
                }
            }

            columnSizeMap.put(column, maxLength);
        }

        String line = DisplayLine(col, columnSizeMap);
        System.out.println(line);
        String columns = DisplayColumns(columnSizeMap);
        System.out.println(columns);
        System.out.println(line);

        for(Records record : this.records) {
            String recordString = DisplayRecord(record, this.col, columnSizeMap);
            System.out.println(recordString);
        }

        System.out.println(line);
        System.out.println("Query Successful");
        System.out.println(String.format("%d rows in set", this.rowsAffected));
        System.out.println();
    }
    

    private String DisplayColumns(HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : this.col){
            buffer.append("| ");
            buffer.append(column);
            int size = columnSizeMap.get(column);
            String filler = FillerString(' ', size-column.length() + 1);
            buffer.append(filler);
        }

        buffer.append("|");

        return buffer.toString();
    }

    private String DisplayRecord(Records record, ArrayList<String> columns, HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : this.col){
            buffer.append("| ");
            if(record.valueMap.containsKey(column)) {
                String value = record.get(column);
                buffer.append(value);
                int size = columnSizeMap.get(column);
                String filler = FillerString(' ', size - value.length() + 1);
                buffer.append(filler);
            }
        }
        buffer.append("|");

        return buffer.toString();
    }


    private String DisplayLine(ArrayList<String> columns, HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : columns){
            buffer.append("+");

            String filler = FillerString('-', columnSizeMap.get(column) + 2);
            buffer.append(filler);
        }

        buffer.append("+");
        return buffer.toString();
    }

    private String FillerString(char character, int size) {
        char[] repeatCharacters = new char[size];
        Arrays.fill(repeatCharacters, character);
        return new String(repeatCharacters);
    }
}
