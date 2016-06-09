package net.achalaggarwal.workerbee.ddl.misc;

import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.TextTable;

public class RecoverPartition implements Query {
  private Table table;

  public RecoverPartition(Table table) {
    this.table = table;
  }

  @Override
  public String generate() {
    return String.format("USE %s ; MSCK REPAIR TABLE %s ;", table.getDatabaseName(), table.getName());
  }
}
