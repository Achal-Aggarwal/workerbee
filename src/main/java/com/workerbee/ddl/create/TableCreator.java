package com.workerbee.ddl.create;

import com.workerbee.Table;
import com.workerbee.ddl.Query;

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

    result.append(" " + table.getDatabaseName() + "." + table.getName());

    if (table.getComment() != null){
      result.append(" COMMENT " + table.getComment());
    }

    if(table.getLocation() != null){
      result.append(" LOCATION " + table.getLocation());
    }

    if(!table.getProperties().isEmpty()){
      result.append(" TBLPROPERTIES (");
      for (String property : table.getProperties()) {
        result.append(property + " = " + table.getProperty(property) + ", ");
      }
      result.delete(result.lastIndexOf(", "), result.length());
      result.append(")");
    }

    result.append(" ;");

    return result.toString();
  }
}
