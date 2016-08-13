package net.achalaggarwal.workerbee;

import lombok.Getter;
import net.achalaggarwal.workerbee.dr.SelectQuery;

public class View {
  @Getter
  private Database database;

  @Getter
  private String name;

  @Getter
  private SelectQuery selectQuery;

  public View(Database database, String name) {
    this.database = database;
    this.name = name;
    this.database.havingView(this);
  }

  public View as(SelectQuery selectQuery) {
    this.selectQuery = selectQuery;

    return this;
  }

  public TextTable<TextTable> getTable() {
    return selectQuery.table(database);
  }

  public String getDatabaseName(){
    return database.getName();
  }
}
