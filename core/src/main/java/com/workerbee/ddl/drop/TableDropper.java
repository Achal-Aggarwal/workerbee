package com.workerbee.ddl.drop;

import com.workerbee.Query;
import com.workerbee.Table;
import com.workerbee.Utils;

import static com.workerbee.Utils.fqTableName;

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

    result.append(" ").append(fqTableName(table));

    result.append(" ;");

    return result.toString();
  }
}
