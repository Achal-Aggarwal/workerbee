package com.workerbee.ddl.drop;

import com.workerbee.Query;
import com.workerbee.Table;

public class TableDropper implements Query {
  private Table table;
  private boolean ifExist;

  public TableDropper(Table table) {
    this.table = table;
  }

  public TableDropper ifExist() {
    ifExist = true;
    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("DROP TABLE");

    if (ifExist){
      result.append(" IF EXISTS");
    }

    result.append(" ");

    if (table.isNotTemporary()){
      result.append(table.getDatabaseName() + ".");
    }

    result.append(table.getName() + " ;");

    return result.toString();
  }
}
