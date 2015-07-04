package com.workerbee;

import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

public class Row<T extends Table> {
  private Map<Column, Object> map;
  private Table table;

  protected Row(T table, String record){
    this.table = table;
    this.map = parseRecordUsing(table, record);
  }

  protected Row(T table, Text record){
    this.table = table;
    this.map = parseRecordUsing(table, record.toString());
  }

  private static Map<Column, Object> parseRecordUsing(Table table, String record) {
    Map<Column, Object> map = new HashMap<Column, Object>(table.getColumns().size());
    RecordParser recordParser = new RecordParser(record, table.getColumnSeparator(), table.getHiveNull());
    int index = 0;

    for (Column column : table.getColumns()) {
      map.put(column, column.parseValueUsing(recordParser, index++));
    }

    for (Column column : table.getPartitions()) {
      map.put(column, column.parseValueUsing(recordParser, index++));
    }

    return map;
  }

  public Object get(Column column) {
    return map.get(column);
  }

  public String getString(Column column) {
    return (String) get(column);
  }

  public Integer getInt(Column column) {
    return (Integer) get(column);
  }

  public Float getFloat(Column column) {
    return (Float) get(column);
  }

  public Row set(Column column, Object value) {
    if (map.containsKey(column)){
      map.put(column, column.convert(value));
    }

    return this;
  }

  public String generateRecord() {
    return Row.generateRecordFor(table, this);
  }

  public static String generateRecordFor(Table table, Row row) {
    StringBuilder result = new StringBuilder();

    for (Column column : table.getColumns()) {
      Object value = row.get(column);
      result.append(value == null ? table.getHiveNull() : value.toString());
      result.append(table.getColumnSeparator());
    }

    for (Column column : table.getPartitions()) {
      Object value = row.get(column);
      result.append(value == null ? table.getHiveNull() : value.toString());
      result.append(table.getColumnSeparator());
    }

    result.delete(result.lastIndexOf(table.getColumnSeparator()), result.length());

    return result.toString();
  }
}
