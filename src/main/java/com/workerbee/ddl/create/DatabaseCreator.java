package com.workerbee.ddl.create;

import com.workerbee.Database;
import com.workerbee.Query;
import com.workerbee.Utils;

import java.util.ArrayList;
import java.util.List;

import static com.workerbee.Utils.quoteString;

public class DatabaseCreator implements Query {
  Database database;
  boolean overwrite = true;

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
      result.append(" COMMENT " + quoteString(database.getComment()));
    }

    if(database.getLocation() != null){
      result.append(" LOCATION " + quoteString(database.getLocation()));
    }

    if(!database.getProperties().isEmpty()){
      tablePropertiesPart(result);
    }

    result.append(" ;");

    return result.toString();
  }

  private void tablePropertiesPart(StringBuilder result) {
    result.append(" WITH DBPROPERTIES ( ");
    List<String> keyValues = new ArrayList<String>(database.getProperties().size());

    for (String property : database.getProperties()) {
      keyValues.add(quoteString(property) + " = " + quoteString(database.getProperty(property)));
    }

    result.append(Utils.joinList(keyValues, ", "));
    result.append(" )");
  }
}
