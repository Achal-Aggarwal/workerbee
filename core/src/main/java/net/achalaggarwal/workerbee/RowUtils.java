package net.achalaggarwal.workerbee;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.achalaggarwal.workerbee.Utils.joinList;

public class RowUtils {
  public static <T extends AvroTable> Row<T> parseSpecificRecord(T table, SpecificRecord record, Column... partitions) {
    Row<T> avroRow = new Row<>(table);

    Schema schema = record.getSchema();
    for (Column column : table.getColumns()) {
      avroRow.set(column, record.get(schema.getField(column.getName()).pos()));
    }

    Map<Column, Object> partitionValueMap = new HashMap<>();

    for (Column partition : partitions) {
      partitionValueMap.put(partition, partition.getValue());
    }

    for (Column column : table.getPartitions()) {
      avroRow.set(column, partitionValueMap.get(column));
    }

    return avroRow;
  }

  public static <T extends AvroTable, A extends SpecificRecord> List<A> getSpecificRecords(List<Row<T>> rows){
    List<A> records = new ArrayList<>(rows.size());

    for (Row row : rows) {
      records.add((A) RowUtils.getSpecificRecord(row));
    }

    return records;
  }

  public static <T extends AvroTable, A extends SpecificRecord> A getSpecificRecord(Row<T> row){
    T table = row.getTable();
    Class<A> klass = (Class<A>) table.getKlass();

    A specificRecord;
    try {
      specificRecord = klass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    for (Schema.Field field : specificRecord.getSchema().getFields()) {
      specificRecord.put(field.pos(), row.get(table.getColumn(field.name())));
    }

    return specificRecord;
  }

  public static String generateRecordFor(Table table, Row row) {
    List<String> result = new ArrayList<>(table.getColumns().size() + table.getPartitions().size());

    for (Column column : table.getColumns()) {
      Object value = row.get(column);
      result.add(value == null ? table.getHiveNull() : value.toString());
    }

    for (Column column : table.getPartitions()) {
      Object value = row.get(column);
      result.add(value == null ? table.getHiveNull() : value.toString());
    }

    return joinList(result, table.getColumnSeparator());
  }
}
