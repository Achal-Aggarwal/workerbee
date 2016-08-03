package net.achalaggarwal.workerbee.ddl.misc;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.Utils;

import java.util.*;

import static net.achalaggarwal.workerbee.Utils.joinList;

public class AlterPartitionTable implements Query {
  private Table table;
  private Map<String, Column> partitionsToAdd = new HashMap<>();

  public AlterPartitionTable(Table table) {
    this.table = table;
  }

  public AlterPartitionTable addPartition(Column partition){
    partitionsToAdd.put(partition.getName(), partition);

    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    if (partitionsToAdd.size() == 0) {
      return result.toString();
    }

    result.append("USE " + table.getDatabaseName() + " ;");
    result.append(String.format("ALTER TABLE %s ADD PARTITION (%s)", table.getName(), partitionSpec(partitionsToAdd)));

    return result.toString();
  }

  private String partitionSpec(Map<String, Column> partitionsToAdd) {
    StringBuilder result = new StringBuilder();

    List<String> columnsDef = new ArrayList<>(table.getPartitions().size());

    for (Column column : table.getPartitions()) {
      String def = column.getName();
      Object value = partitionsToAdd.get(def).getValue();

      if (value instanceof String){
        def += " = " + Utils.quoteString((String) value);
      } else if(value != null) {
        def += " = " + value;
      }

      columnsDef.add(def);
    }

    result.append(joinList(columnsDef, ", "));

    return result.toString();
  }
}
