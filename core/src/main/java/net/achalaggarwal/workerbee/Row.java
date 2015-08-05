package net.achalaggarwal.workerbee;

import net.achalaggarwal.workerbee.dr.selectfunction.Constant;
import org.apache.hadoop.io.Text;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Row<T extends Table> {
  private static final int ZERO_BASED = 0;
  private static final int ONE_BASED = 1;
  private Map<Column, Object> map;
  private Table<? extends Table> table;

  public Row(Table<T> table){
    this(table, "");
  }

  public Row(Table<T> table, ResultSet resultSet){
    this.table = table;
    this.map = parseRecordUsing(table, resultSet, ONE_BASED);
  }

  public Row(Table<T> table, String record){
    this.table = table;
    this.map = parseRecordUsing(table, record);
  }

  public Row(Table<T> table, Text record){
    this(table, record.toString());
  }

  public Row(Table<T> table, Row<? extends Table> record) {
    this(table);
    for (Column column : table.getColumns()) {
      set(column, record.get(column));
    }

    for (Column column : table.getPartitions()) {
      set(column, record.get(column));
    }
  }

  private static Map<Column, Object> parseRecordUsing(Table<? extends Table> table, String record) {
    return parseRecordUsing(
      table,
      new RecordParser(record, table.getColumnSeparator(), table.getHiveNull()),
      ZERO_BASED
    );
  }

  private static Map<Column, Object> parseRecordUsing(
    Table<? extends Table> table, ResultSet resultSet, int startingIndex
  ) {
    Map<Column, Object> map = new HashMap<>(table.getColumns().size());
    int index = startingIndex;

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
    List<String> result = new ArrayList<>(table.getColumns().size() + table.getPartitions().size());

    for (Column column : table.getColumns()) {
      Object value = row.get(column);
      result.add(value == null ? table.getHiveNull() : value.toString());
    }

    for (Column column : table.getPartitions()) {
      Object value = row.get(column);
      result.add(value == null ? table.getHiveNull() : value.toString());
    }

    return Utils.joinList(result, table.getColumnSeparator());
  }

  public Constant getC(Column column) {
    return new Constant(map.get(column));
  }

  public Constant[] getConstants() {
    List<Constant> constants = new ArrayList<>();

    for (Column column : table.getColumns()) {
      constants.add(getC(column));
    }

    for (Column column : table.getPartitions()) {
      constants.add(getC(column));
    }

    return constants.toArray(new Constant[constants.size()]);
  }
}
