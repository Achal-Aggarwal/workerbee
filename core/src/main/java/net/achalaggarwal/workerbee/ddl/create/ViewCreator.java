package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.*;

public class ViewCreator implements Query {
  protected View view;
  protected boolean overwrite = true;
  protected Database database;

  public ViewCreator(View view) {
    this.view = view;
  }

  public ViewCreator ifNotExist(){
    overwrite = false;
    return this;
  }

  public ViewCreator inDatabase(Database database) {
    this.database = database;

    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("CREATE VIEW ");

    if (!overwrite) {
      result.append("IF NOT EXISTS ");
    }

    result.append(Utils.fqViewName(view, database));

    result.append(" AS ").append(view.getSelectQuery().generate());

    result.append(" ;");

    return result.toString();
  }
}
