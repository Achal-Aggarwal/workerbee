package com.workerbee.ddl.create;

import com.workerbee.Column;
import com.workerbee.Table;
import com.workerbee.Query;
import com.workerbee.Utils;

import java.util.ArrayList;
import java.util.List;

import static com.workerbee.Utils.joinList;
import static com.workerbee.Utils.quoteString;

public class TableCreator implements Query {
  Table table;
  boolean overwrite = true;

  public TableCreator(Table table) {
    this.table = table;
  }

  public TableCreator ifNotExist(){
    overwrite = false;
    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("CREATE");

    if(table.isExternal()){
      result.append(" EXTERNAL");
    }

    result.append(" TABLE");

    if (!overwrite) {
      result.append(" IF NOT EXISTS");
    }

    result.append(" ");

    if (table.isNotTemporary()){
      result.append(table.getDatabaseName() + ".");
    }

    result.append(table.getName());

    if (!table.getColumns().isEmpty()){
      columnDefPart(result);
    }

    if (table.getComment() != null){
      result.append(" COMMENT " + quoteString(table.getComment()));
    }

    if (!table.getPartitions().isEmpty()){
      partitionedByPart(result);
    }

    if(table.getLocation() != null){
      result.append(" LOCATION " + quoteString(table.getLocation()));
    }

    if(!table.getProperties().isEmpty()){
      tablePropertiesPart(result);
    }

    result.append(" ;");

    return result.toString();
  }

  private void tablePropertiesPart(StringBuilder result) {
    result.append(" TBLPROPERTIES ( ");
    for (String property : table.getProperties()) {
      result.append(quoteString(property) + " = " + quoteString(table.getProperty(property)) + ", ");
    }
    result.delete(result.lastIndexOf(", "), result.length());
    result.append(" )");
  }

  private void partitionedByPart(StringBuilder result) {
    result.append(" PARTITIONED BY ( ");
    List<String> columnsDef = new ArrayList<String>(table.getPartitions().size());

    for (Column column : table.getPartitions()) {
      columnsDef.add(column.getName() + " " + column.getType());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }

  private void columnDefPart(StringBuilder result) {
    result.append(" ( ");
    List<String> columnsDef = new ArrayList<String>(table.getColumns().size());

    for (Column column : table.getColumns()) {
      columnsDef.add(column.getName() + " " + column.getType());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
