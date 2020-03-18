package datatypes.base;

import Model.Literals;
import common.Constant;
import common.Utils;
import datatypes.*;

public abstract class DT<Any> {
	
	  protected final byte valueSerialCode;

	    protected final byte nullSerialCode;

    protected Any value;

    protected boolean isNull;

  

    public static DT CreateDT(Literals value) {
        switch(value.type) {
            case TINYINT:
                return new DataTypeTinyInt(Byte.valueOf(value.value));
            case SMALLINT:
                return new DataTypeSmallInt(Short.valueOf(value.value));
            case BIGINT:
                return new DataTypeInt(Long.valueOf(value.value));
            case INT:
                return new DT_Int(Integer.valueOf(value.value));
            case REAL:
                return new DataTypeReal(Float.valueOf(value.value));
            case DOUBLE:
                return new DataTypeDouble(Double.valueOf(value.value));
            case DATETIME:
                return new DataTypeDateTime();
            case DATE:
                return new DataTypeDate();
            case TEXT:
                return new DataTypeText(value.value);
        }

        return null;
    }

    public static DT createSystemDT(String value, byte dataType) {
        switch(dataType) {
            case Constant.TINYINT:
                return new DataTypeTinyInt(Byte.valueOf(value));
            case Constant.SMALLINT:
                return new DataTypeSmallInt(Short.valueOf(value));
            case Constant.BIGINT:
                return new DataTypeInt(Long.valueOf(value));
            case Constant.INT:
                return new DT_Int(Integer.valueOf(value));
            case Constant.REAL:
                return new DataTypeReal(Float.valueOf(value));
            case Constant.DOUBLE:
                return new DataTypeDouble(Double.valueOf(value));
            case Constant.DATETIME:
                return new DataTypeDateTime(Utils.getDateEpoc(value, false));
            case Constant.DATE:
                return new DataTypeDateTime(Utils.getDateEpoc(value, true));
            case Constant.TEXT:
                return new DataTypeText(value);
        }

        return null;
    }

    protected DT(int valueSerialCode, int nullSerialCode) {
        this.valueSerialCode = (byte) valueSerialCode;
        this.nullSerialCode = (byte) nullSerialCode;
    }

    public Any getValue() {
        return value;
    }
    
    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public byte getValueSerialCode() {
        return valueSerialCode;
    }

    public byte getNullSerialCode() {
        return nullSerialCode;
    }

    public String getStringValue() {
        if(value == null) {
            return "NULL";
        }
        return value.toString();
    }

    public void setValue(Any value) {
        this.value = value;
         if (value != null) {
             this.isNull = false;
         }
    }

    public boolean isNull() {
        return isNull;
    }

   
}
