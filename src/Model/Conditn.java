package Model;

import QueryParser.DbHelper;

public class Conditn {
    public String column;
    public Optr operator;
    public Literals value;

    public static Conditn CreateCondition(String conditionStr) {
        Optr optr = GetOperator(conditionStr);
        if(optr == null) {
            DbHelper.UnknownCommand(conditionStr, "Unrecognised operator. \nValid operators include =, >, <, >=, <=. \nPlease follow <column> <operator> <value>");
            return null;
        }

        Conditn condition = null;

        switch (optr){
            case GREATER_THAN:
                condition = getConditionInternal(conditionStr, optr, ">");
                break;
            case LESS_THAN:
                condition = getConditionInternal(conditionStr, optr, "<");
                break;
            case LESS_THAN_EQUAL:
                condition = getConditionInternal(conditionStr, optr, "<=");
                break;
            case GREATER_THAN_EQUAL:
                condition = getConditionInternal(conditionStr, optr, ">=");
                break;
            case EQUALS:
                condition = getConditionInternal(conditionStr, optr, "=");
                break;
        }

        return condition;
    }

    private static Conditn getConditionInternal(String conditionString, Optr operator, String operatorString) {
        String[] parts;
        String column;
        Literals literals;
        Conditn condition;
        parts = conditionString.split(operatorString);
        if(parts.length != 2) {
            DbHelper.UnknownCommand(conditionString, "Unrecognised condition. Please follow <column> <operator> <value>");
            return null;
        }

        column = parts[0].trim();
        literals = Literals.CreateLiteral(parts[1].trim());

        if (literals == null) {
            return null;
        }

        condition = new Conditn(column, operator, literals);
        return condition;
    }

    private Conditn(String column, Optr operator, Literals value){
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    private static Optr GetOperator(String conditionString) {

        if(conditionString.contains("<=")){
            return Optr.LESS_THAN_EQUAL;
        }

        if(conditionString.contains(">=")){
            return Optr.GREATER_THAN_EQUAL;
        }

        if(conditionString.contains(">")){
            return Optr.GREATER_THAN;
        }

        if(conditionString.contains("<")){
            return Optr.LESS_THAN;
        }

        if(conditionString.contains("=")){
            return Optr.EQUALS;
        }

        return null;
    }
}
