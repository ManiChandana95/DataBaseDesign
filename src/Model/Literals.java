package Model;

import QueryParser.DbHelper;
import common.Constant;
import common.Utils;
import datatypes.base.DT;

public class Literals {
    public DataType type;
    public String value;

    public static Literals CreateLiteral(DT value, Byte type) {
        if(type == Constant.INVALID_CLASS) return null;

        switch(type) {
            case Constant.TINYINT:
                return new Literals(DataType.TINYINT, value.getStringValue());
            case Constant.SMALLINT:
                return new Literals(DataType.SMALLINT, value.getStringValue());
            case Constant.INT:
                return new Literals(DataType.INT, value.getStringValue());
            case Constant.BIGINT:
                return new Literals(DataType.BIGINT, value.getStringValue());
            case Constant.REAL:
                return new Literals(DataType.REAL, value.getStringValue());
            case Constant.DOUBLE:
                return new Literals(DataType.DOUBLE, value.getStringValue());
            case Constant.DATE:
                return new Literals(DataType.DATE, Utils.getDateEpocAsString((long)value.getValue(), true));
            case Constant.DATETIME:
                return new Literals(DataType.DATETIME, Utils.getDateEpocAsString((long)value.getValue(), false));
            case Constant.TEXT:
                return new Literals(DataType.TEXT, value.getStringValue());
        }

        return null;
    }

    public static Literals CreateLiteral(String literalString){
        if(literalString.startsWith("'") && literalString.endsWith("'")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literals(DataType.TEXT, literalString);
        }

        if(literalString.startsWith("\"") && literalString.endsWith("\"")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literals(DataType.TEXT, literalString);
        }

        try{
            Integer.parseInt(literalString);
            return new Literals(DataType.INT, literalString);
        }
        catch (Exception e){}

        try{
            Double.parseDouble(literalString);
            return new Literals(DataType.REAL, literalString);
        }
        catch (Exception e){}

            DbHelper.UnknownCommand(literalString, "Unrecognised Literal Found. Please use integers, real or strings ");
        return null;
    }

    private Literals(DataType type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        if (this.type == DataType.TEXT) {
            return this.value;
        } else if (this.type == DataType.INT || this.type == DataType.TINYINT ||
                this.type == DataType.SMALLINT || this.type == DataType.BIGINT) {
            return this.value;
        } else if (this.type == DataType.REAL || this.type == DataType.DOUBLE) {
            return String.format("%.2f", Double.parseDouble(this.value));
        } else if (this.type == DataType.INT_REAL_NULL || this.type == DataType.SMALL_INT_NULL || this.type == DataType.TINY_INT_NULL || this.type == DataType.DOUBLE_DATETIME_NULL) {
            return "NULL";
        } else if (this.type == DataType.DATE || this.type == DataType.DATETIME) {
            return this.value;
        }

        return "";
    }
}
