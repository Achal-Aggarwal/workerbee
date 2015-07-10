package com.workerbee;

import org.apache.hadoop.io.Text;

import java.nio.file.Path;
import java.util.*;

public class Table<T extends Table> {
  private  Database database;

  private String name;

  private String comment;
  private String location;
  private boolean external = false;

  HashMap<String, String> properties = new HashMap<>();

  List<Column> columns = new ArrayList<>();
  List<Column> partitionedOn = new ArrayList<>();

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

  public Table onLocation(Path location) {
    return onLocation(location.toAbsolutePath().toString());
  }

  public Table external(){
    external = true;

    return this;
  }

  public Database getDatabase() {
    return database;
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

  public Row<T> getNewRow(){
    return parseRecordUsing("");
  }

  public Row<T> parseRecordUsing(String record) {
    return new Row<T>(this, record);
  }

  public Row<T> parseTextRecordUsing(Text record) {
    return parseRecordUsing(record.toString());
  }

  public String generateRecordFor(Row<T> row) {
    return Row.generateRecordFor(this, row);
  }

  public Text generateTextRecordFor(Row<T> row) {
    return new Text(generateRecordFor(row));
  }
}
