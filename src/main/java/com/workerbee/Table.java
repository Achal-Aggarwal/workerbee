package com.workerbee;

import com.workerbee.ddl.create.TableCreator;
import org.apache.hadoop.io.Text;

import java.nio.file.Path;
import java.util.*;

import static com.workerbee.Column.Type.INT;
import static com.workerbee.Column.Type.STRING;

public class Table<T extends Table> {
  private Database database;

  private String name;

  private long version;

  private String comment;

  private String location;
  private boolean external = false;

  HashMap<String, String> properties = new HashMap<>();

  List<Column> columns = new ArrayList<>();

  List<Column> partitionedOn = new ArrayList<>();

  public Table(String name) {
    this(null, name, null, 0);
  }
  public Table(String name, long version) {
    this(null, name, null, version);
  }

  public Table(Database database, String name) {
    this(database, name, null, 0);
  }

  public Table(Database database, String name, long version) {
    this(database, name, null, version);
  }

  public Table(Database database, String name, String comment) {
    this(database, name, comment, 0);
  }

  public Table(Database database, String name, String comment, long version) {
    this.database = database;
    this.name = name;
    this.comment = comment;
    this.version = version;

    if (isNotTemporary()) {
      this.database.havingTable(this);
    }
  }

  public Table<T> havingColumn(Column column){
    columns.add(column);
    return this;
  }

  public static Column HavingColumn(Table table, String name, Column.Type type) {
    Column column = new Column(table, name, type);
    table.havingColumn(column);
    return column;
  }

  public Table<T> havingColumn(String name, Column.Type type, String comment){
    return havingColumn(new Column(this, name, type, comment));
  }

  public Table<T> havingColumn(String name, Column.Type type){
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

  public long getVersion() {
    return version;
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

  public String migration() {
    return new TableCreator(this).ifNotExist().generate();
  }

  public static Table<Table> DUAL = new Table<>(Database.DEFAULT, "Dual")
    .havingColumn("dummy", STRING);
}
