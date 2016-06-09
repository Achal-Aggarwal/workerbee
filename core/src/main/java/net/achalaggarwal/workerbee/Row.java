package net.achalaggarwal.workerbee;

import lombok.Getter;
import net.achalaggarwal.workerbee.dr.selectfunction.Constant;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.Text;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.joinList;

public class Row<T extends Table> {
  public static final int ZERO_BASED = 0;
  public static final int ONE_BASED = 1;

  protected Map<Column, Object> map;

  @Getter
  protected T table;

  public Row(T table){
    this.table = table;
  }

  public Row(T table, ResultSet resultSet){
    this(table);
    this.map = parseRecordUsing(resultSet, ONE_BASED);
  }

  public Row(T table, Row record) {
    this(table);

    for (Column column : table.getColumns()) {
      set(column, record.get(column));
    }

    for (Column column : table.getPartitions()) {
      set(column, record.get(column));
    }
  }

  public Row(T table, String record){
    this(table);
    this.map = parseRecordUsing(
      new RecordParser(record, table.getColumnSeparator(), table.getHiveNull()),
      ZERO_BASED
    );
  }

  private Map<Column, Object> parseRecordUsing(
    ResultSet resultSet, int startingIndex
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

  public Row set(Column column, Object value) {
    if (map.containsKey(column)){
      map.put(column, column.convert(value));
    }

    return this;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Row row = (Row) o;

    if (!map.equals(row.map)) return false;
    return table.equals(row.table);

  }

  @Override
  public int hashCode() {
    int result = map.hashCode();
    result = 31 * result + table.hashCode();
    return result;
  }

  @Override
  public String toString() {
    List<String> sb = new ArrayList<>();

    for (Column column : map.keySet()) {
      sb.add(column.getName() + ":" + map.get(column));
    }

    return fqTableName(table) + "@{" + joinList(sb, ", ") + '}';
  }

  public String generateRecord() {
    return RowUtils.generateRecordFor(table, this);
  }
}
