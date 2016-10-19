package com.github.hackerwin7.mysql.tracker.tracker.common;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by hp on 14-9-3.
 */
public class TableMeta {

    private String          fullName; // schema.table
    private List<FieldMeta> fileds;

    public TableMeta(String fullName, List<FieldMeta> fileds){
        this.fullName = fullName;
        this.fileds = fileds;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public List<FieldMeta> getFileds() {
        return fileds;
    }

    public void setFileds(List<FieldMeta> fileds) {
        this.fileds = fileds;
    }

    public static class FieldMeta {

        private String columnName;
        private String columnType;
        private String isNullable;
        private String iskey;
        private String defaultValue;
        private String extra;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        public String getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(String isNullable) {
            this.isNullable = isNullable;
        }

        public String getIskey() {
            return iskey;
        }

        public void setIskey(String iskey) {
            this.iskey = iskey;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public boolean isUnsigned() {
            return StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        public boolean isKey() {
            return StringUtils.equalsIgnoreCase(iskey, "PRI");
        }

        public boolean isNullable() {
            return StringUtils.equalsIgnoreCase(isNullable, "YES");
        }

        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", defaultValue="
                    + defaultValue + ", extra=" + extra + ", isNullable=" + isNullable + ", iskey=" + iskey + "]";
        }

    }

}
