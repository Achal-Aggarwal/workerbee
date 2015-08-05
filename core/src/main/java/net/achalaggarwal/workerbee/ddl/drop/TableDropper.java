package net.achalaggarwal.workerbee.ddl.drop;

import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;

import static net.achalaggarwal.workerbee.Utils.fqTableName;

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
