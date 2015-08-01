package com.workerbee.ddl.misc;

import com.workerbee.Query;
import com.workerbee.Table;

public class RecoverPartition implements Query {
  private Table<? extends Table> table;

  public RecoverPartition(Table<? extends Table> table) {
    this.table = table;
  }

  @Override
  public String generate() {
    return String.format("USE %s ; MSCK REPAIR TABLE %s ;", table.getDatabaseName(), table.getName());
  }
}
