package com.workerbee;

import com.workerbee.dr.selectfunction.Constant;
import org.apache.hadoop.io.Text;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row<T extends Table> {
  private Map<Column, Object> map;
  private Table table;

  public Row(Table<T> table){
    this(table, "");
  }

  public Row(Table<T> table, ResultSet resultSet){
    this.table = table;
    this.map = parseRecordUsing(table, resultSet);
  }

  public Row(Table<T> table, String record){
    this.table = table;
    this.map = parseRecordUsing(table, record);
  }

  public Row(Table<T> table, Text record){
    this(table, record.toString());
  }

  private static Map<Column, Object> parseRecordUsing(Table<? extends Table> table, String record) {
    Map<Column, Object> map = new HashMap<>(table.getColumns().size());
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

  private static Map<Column, Object> parseRecordUsing(Table<? extends Table> table, ResultSet resultSet) {
    Map<Column, Object> map = new HashMap<>(table.getColumns().size());
    int index = 1;

    for (Column column : table.getColumns()) {
      map.put(column, column.parseValueUsing(resultSet, index++));
    }

    for (Column column : table.getPartitions()) {
      map.put(column, column.parseValueUsing(resultSet, index++));
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

  public Row<T> set(Column column, Object value) {
    if (map.containsKey(column)){
      map.put(column, column.convert(value));
    }

    return this;
  }

  public String generateRecord() {
    return Row.generateRecordFor(table, this);
  }

  public Text generateTextRecord() {
    return new Text(Row.generateRecordFor(table, this));
  }

  public static String generateRecordFor(Table<? extends Table> table, Row row) {
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

  public Constant getC(Column column) {
    return new Constant(map.get(column));
  }

  public Constant[] getConstants() {
    List<Constant> constants = new ArrayList<>();

    for (Column column : (List<Column>) table.getColumns()) {
      constants.add(getC(column));
    }

    for (Column column : (List<Column>) table.getPartitions()) {
      constants.add(getC(column));
    }

    return constants.toArray(new Constant[constants.size()]);
  }
}
