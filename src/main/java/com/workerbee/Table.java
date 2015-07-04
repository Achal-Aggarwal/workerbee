package com.workerbee;

import org.apache.hadoop.io.Text;

import java.util.*;

public class Table {

  private  Database database;

  private String name;
  private String comment;
  private String location;

  private boolean external = false;

  HashMap<String, String> properties = new HashMap<String, String>();

  List<Column> columns = new ArrayList<Column>();
  List<Column> partitionedOn = new ArrayList<Column>();

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

  public static Column HavingColumn(Table table, String name, Column.Type type) {
    Column column = new Column(table, name, type);
    table.havingColumn(column);
    return column;
  }

  public Table havingColumn(String name, Column.Type type, String comment){
    return havingColumn(new Column(this, name, type, comment));
  }

  public Table havingColumn(String name, Column.Type type){
    return havingColumn(name, type, null);
  }

  public Table havingColumns(List<Column> columns) {
    this.columns.addAll(columns);
    return this;
  }

  public Table partitionedOnColumn(Column column){
    partitionedOn.add(column);
    return this;
  }

  public static Column PartitionedOnColumn(Table table, String name, Column.Type type) {
    Column column = new Column(table, name, type);
    table.partitionedOnColumn(column);
    return column;
  }

  public Table partitionedOnColumns(List<Column> columns) {
    partitionedOn.addAll(columns);
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

  public List<Column> getPartitions() {
    return partitionedOn;
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

  public String getHiveNull() {
    return "\\N";
  }

  public String getColumnSeparator() {
    return "\1";
  }

  public Row getNewRow(){
    return parseRecordUsing("");
  }

  public Row parseRecordUsing(String record) {
    return new Row<Table>(this, record);
  }

  public Row parseTextRecordUsing(Text record) {
    return parseRecordUsing(record.toString());
  }

  public String generateRecordFor(Row row) {
    return Row.generateRecordFor(this, row);
  }

  public Text generateTextRecordFor(Row row) {
    return new Text(generateRecordFor(row));
  }
}
