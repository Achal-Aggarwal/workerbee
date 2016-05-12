package net.achalaggarwal.workerbee.dml.insert;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.Utils;
import net.achalaggarwal.workerbee.dr.SelectQuery;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.joinList;

public class InsertQuery implements Query {
  private boolean overwrite = false;
  private Table<? extends Table> table;
  private SelectQuery selectQuery;
  private Map<Column, Object> partitionMap;
  private File directory;

  public InsertQuery intoTable(Table<? extends Table> table) {
    this.table = table;

    partitionMap = new HashMap<>(table.getPartitions().size());
    for (Column column : table.getPartitions()) {
      partitionMap.put(column, null);
    }

    return this;
  }

  public InsertQuery directory(File path){
    this.directory = path;

    return this;
  }

  public InsertQuery using(SelectQuery selectQuery) {
    this.selectQuery = selectQuery;
    return this;
  }

  public InsertQuery overwrite() {
    overwrite = true;
    return this;
  }

  public InsertQuery partitionOn(Column partitionName, Object partitionValue) {
    if (partitionMap.containsKey(partitionName)){
      partitionMap.put(partitionName, partitionValue);
    }

    return this;
  }

  public InsertQuery partitionOn(List<Column> partitions) {
    for (Column partition : partitions) {
      partitionOn(partition, partition.getValue());
    }

    return this;
  }

  public InsertQuery partitionOn(Column partition) {
    partitionOn(partition, partition.getValue());

    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("INSERT");

    if (overwrite){
      result.append(" OVERWRITE");
    } else {
      result.append(" INTO");
    }

    if(directory == null) {
      result.append(" TABLE ");

      result.append(fqTableName(table));

      if (!partitionMap.isEmpty()) {
        partitionPart(result);
      }
    } else {
      result.append(" DIRECTORY '").append(directory.getAbsolutePath()).append("'");
    }

    result.append(" " + selectQuery.generate());

    return result.toString();
  }

  private void partitionPart(StringBuilder result) {
    result.append(" PARTITION ( ");
    List<String> columnsDef = new ArrayList<>(partitionMap.size());

    for (Column column : partitionMap.keySet()) {
      String def = column.getName();
      Object value = partitionMap.get(column);

      if (value instanceof String){
        def += " = " + Utils.quoteString((String) partitionMap.get(column));
      } else if(value != null) {
        def += " = " + partitionMap.get(column);
      }

      columnsDef.add(def);
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
