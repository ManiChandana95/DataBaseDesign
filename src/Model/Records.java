package Model;

import java.util.HashMap;

public class Records {
    HashMap<String, Literals> valueMap;

    public static Records CreateRecord(){
        return new Records();
    }

    private Records(){
        this.valueMap = new HashMap<>();
    }

    public void put(String columnName, Literals value){
        if(columnName.length() == 0) return;
        if(value == null) return;

        this.valueMap.put(columnName, value);
    }

    public String get(String col) {
        Literals literals = this.valueMap.get(col);
        return literals.toString();
    }
}
