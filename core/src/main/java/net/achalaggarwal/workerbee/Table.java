package net.achalaggarwal.workerbee;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.Text;

import java.nio.file.Path;
import java.util.*;

import static net.achalaggarwal.workerbee.Column.Type.STRING;

public class Table<T extends Table> {
  @Getter
  private Database database;

  @Getter
  private String name;

  @Getter
  private long version;

  @Getter
  private String comment;

  @Getter
  private String location;

  @Getter
  private boolean external = false;

  @Getter
  private Class<? extends SpecificRecord> klass;

  HashMap<String, String> properties = new HashMap<>();

  Map<String, Column> columns = new LinkedHashMap<>();

  Map<String, Column> partitionedOn = new LinkedHashMap<>();

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

  public Table<T> havingColumnsFromSchema(Class<? extends SpecificRecord> klass){
    this.klass = klass;

    SpecificRecord specificRecord = null;
    try {
      specificRecord = klass.newInstance();
    } catch (Exception e) {
      new RuntimeException(e);
    }
    for (Schema.Field field : specificRecord.getSchema().getFields()) {
      havingColumn(field.name(), Column.getType(field.schema()));
    }

    return this;
  }

  public Table<T> havingColumn(Column column){
    if (columns.containsKey(column.getName().toLowerCase())) {
      throw new RuntimeException("Table " + getName() + " already has a column with name " + column.getName());
    }

    columns.put(column.getName().toLowerCase(), column);
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
    for (Column column : columns) {
      havingColumn(column);
    }

    return this;
  }

  public Table partitionedOnColumn(Column column){
    partitionedOn.put(column.getName().toLowerCase(), column);
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

  public Column getColumn(Column column) {
    return getColumn(column.getName());
  }

  public Column getColumn(String columnName) {
    Column column = columns.get(columnName.toLowerCase());
    return column == null ? partitionedOn.get(columnName.toLowerCase()) : column;
  }

  public List<Column> getColumns() {
    return new ArrayList<>(columns.values());
  }

  public List<Column> getPartitions() {
    return new ArrayList<>(partitionedOn.values());
  }

  public String getDatabaseName(){
    return database.getName();
  }

  public Set<String> getProperties(){
    return properties.keySet();
  }

  public String getProperty(String property) {
    return properties.get(property);
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
    return new Row<>(this, record);
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

  public static Table<Table> DUAL = new Table<>(Database.DEFAULT, "Dual")
    .havingColumn("dummy", STRING);
}
