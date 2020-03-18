package storage.model;

import common.Constant;
import common.Utils;
import datatypes.*;

import java.util.ArrayList;
import java.util.List;


public class AllDataRecords {

    private List<Object> columnValueList;

    private short size;

    private int rowId;

    private int pageLocated;

    private short offset;

    public AllDataRecords() {
        size = 0;
        columnValueList = new ArrayList<>();
        pageLocated = -1;
        offset = -1;
    }

    public List<Object> getColumnValueList() {
        return columnValueList;
    }

    public short getSize() {
        return size;
    }

    public void setSize(short size) {
        this.size = size;
    }

    public short getHeaderSize() {
        return (short)(Short.BYTES + Integer.BYTES);
    }

    public void populateSize() {
        this.size = (short) (this.columnValueList.size() + 1);
        for(Object object: columnValueList) {
            if(object.getClass().equals(DataTypeTinyInt.class)) {
                this.size += ((DataTypeTinyInt) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeSmallInt.class)) {
                this.size += ((DataTypeSmallInt) object).getSIZE();
            }
            else if(object.getClass().equals(DT_Int.class)) {
                this.size += ((DT_Int) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeInt.class)) {
                this.size += ((DataTypeInt) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeReal.class)) {
                this.size += ((DataTypeReal) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeDouble.class)) {
                this.size += ((DataTypeDouble) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeDateTime.class)) {
                size += ((DataTypeDateTime) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeDate.class)) {
                this.size += ((DataTypeDate) object).getSIZE();
            }
            else if(object.getClass().equals(DataTypeText.class)) {
                this.size += ((DataTypeText) object).getSize();
            }
        }
    }

    public int getRowIdx() {
        return rowId;
    }

    public void setRowIdx(int rowId) {
        this.rowId = rowId;
    }

    public int getPgLocated() {
        return pageLocated;
    }

    public void setPgLocated(int pageLocated) {
        this.pageLocated = pageLocated;
    }

    public short getOffset() {
        return offset;
    }

    public void setOffset(short offset) {
        this.offset = offset;
    }

    public byte[] getSerialTypeCodes() {
        byte[] serialTypeCodes = new byte[columnValueList.size()];
        byte index = 0;
        for(Object object: columnValueList) {
            switch (Utils.resolveClass(object)) {
                case Constant.TINYINT:
                    serialTypeCodes[index++] = ((DataTypeTinyInt) object).getSerialCode();
                    break;

                case Constant.SMALLINT:
                    serialTypeCodes[index++] = ((DataTypeSmallInt) object).getSerialCode();
                    break;

                case Constant.INT:
                    serialTypeCodes[index++] = ((DT_Int) object).getSerialCode();
                    break;

                case Constant.BIGINT:
                    serialTypeCodes[index++] = ((DataTypeInt) object).getSerialCode();
                    break;

                case Constant.REAL:
                    serialTypeCodes[index++] = ((DataTypeReal) object).getSerialCode();
                    break;

                case Constant.DOUBLE:
                    serialTypeCodes[index++] = ((DataTypeDouble) object).getSerialCode();
                    break;

                case Constant.DATETIME:
                    serialTypeCodes[index++] = ((DataTypeDateTime) object).getSerialCode();
                    break;

                case Constant.DATE:
                    serialTypeCodes[index++] = ((DataTypeDate) object).getSerialCode();
                    break;

                case Constant.TEXT:
                    serialTypeCodes[index++] = ((DataTypeText) object).getSerialCode();
                    break;
            }
        }
        return serialTypeCodes;
    }
}
