package com.workerbee.dml.insert;

import com.workerbee.Column;
import com.workerbee.Query;
import com.workerbee.Table;
import com.workerbee.Utils;
import com.workerbee.dr.SelectQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.workerbee.Utils.fqTableName;
import static com.workerbee.Utils.joinList;

public class InsertQuery implements Query {
  private boolean overwrite = false;
  private Table<? extends Table> table;
  private SelectQuery selectQuery;
  private Map<Column, Object> partitionMap;

  public InsertQuery intoTable(Table<? extends Table> table) {
    this.table = table;

    partitionMap = new HashMap<>(table.getPartitions().size());
    for (Column column : table.getPartitions()) {
      partitionMap.put(column, null);
    }

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

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("INSERT");

    if (overwrite){
      result.append(" OVERWRITE");
    } else {
      result.append(" INTO");
    }

    result.append(" TABLE ");

    result.append(fqTableName(table));

    if (!partitionMap.isEmpty()){
      partitionPart(result);
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
