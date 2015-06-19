package com.workerbee;

import java.util.*;

public class Table {
  public static final String COLUMN_SEPARATOR = ":";
  public static final String HIVE_NULL = "NULL";
  private  Database database;

  private String name;
  private String comment;
  private String location;

  private boolean external = false;

  HashMap<String, String> properties = new HashMap<String, String>();

  List<Column> columns = new ArrayList<Column>();

  public Table(String name) {
    this(null, name, null);
  }
  public Table(Database database, String name) {
    this(database, name, null);
  }

  public Table(Database database, String name, String comment) {
    this.database = database;
    this.name = name;
    this.comment = comment;
  }

  public Table havingColumn(Column column){
    columns.add(column);
    return this;
  }

  public Table havingColumn(String name, Column.Type type, String comment){
    return havingColumn(new Column(name, type, comment));
  }

  public Table havingColumn(String name, Column.Type type){
    return havingColumn(name, type, null);
  }

  public Table havingColumns(List<Column> columns) {
    this.columns.addAll(columns);
    return this;
  }

  public Table withComment(String comment){
    this.comment = comment;
    return this;
  }

  public Table havingProperty(String key, String value){
    properties.put(key, value);
    return this;
  }

  public Table onLocation(String location) {
    this.location = location;

    return this;
  }

  public Table external(){
    external = true;

    return this;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public Row getNewRow(){
    return parseRecordUsing("");
  }

  public String getDatabaseName(){
    return database.getName();
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public Set<String> getProperties(){
    return properties.keySet();
  }

  public String getProperty(String property) {
    return properties.get(property);
  }

  public String getLocation() {
    return location;
  }

  public boolean isExternal() {
    return external;
  }

  public boolean isNotTemporary() {
    return database != null;
  }

  public Row parseRecordUsing(String record) {
    Map<Column, Object> map = new HashMap<Column, Object>(columns.size());
    RecordParser recordParser = new RecordParser(record, COLUMN_SEPARATOR, HIVE_NULL);
    int index = 0;
    for (Column column : columns) {
      map.put(column, column.readValueUsing(recordParser, index++));
    }

    return new Row(map);
  }

  public String generateRecordFor(Row row) {
    StringBuilder result = new StringBuilder();

    for (Column column : columns) {
      Object value = row.get(column);
      result.append(value == null ? HIVE_NULL : value.toString());
      result.append(COLUMN_SEPARATOR);
    }

    result.delete(result.lastIndexOf(COLUMN_SEPARATOR), result.length());

    return result.toString();
  }
}
