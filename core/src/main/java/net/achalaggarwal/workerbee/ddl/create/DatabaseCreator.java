package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Utils;

import java.util.ArrayList;
import java.util.List;

public class DatabaseCreator implements Query {
  private Database database;
  private boolean overwrite = true;

  public DatabaseCreator(Database database) {
    this.database = database;
  }

  public DatabaseCreator ifNotExist(){
    overwrite = false;
    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("CREATE DATABASE");

    if (!overwrite) {
      result.append(" IF NOT EXISTS");
    }

    result.append(" " + database.getName());

    if (database.getComment() != null){
      result.append(" COMMENT " + Utils.quoteString(database.getComment()));
    }

    if(database.getLocation() != null){
      result.append(" LOCATION " + Utils.quoteString(database.getLocation()));
    }

    if(!database.getProperties().isEmpty()){
      tablePropertiesPart(result);
    }

    result.append(" ;");

    return result.toString();
  }

  private void tablePropertiesPart(StringBuilder result) {
    result.append(" WITH DBPROPERTIES ( ");
    List<String> keyValues = new ArrayList<>(database.getProperties().size());

    for (String property : database.getProperties()) {
      keyValues.add(Utils.quoteString(property) + " = " + Utils.quoteString(database.getProperty(property)));
    }

    result.append(Utils.joinList(keyValues, ", "));
    result.append(" )");
  }
}
