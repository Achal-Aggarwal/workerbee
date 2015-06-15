package com.workerbee.ddl.create;

import com.workerbee.Database;
import com.workerbee.ddl.Query;

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
      result.append(" COMMENT " + database.getComment());
    }

    if(database.getLocation() != null){
      result.append(" LOCATION " + database.getLocation());
    }

    if(!database.getProperties().isEmpty()){
      result.append(" WITH DBPROPERTIES (");
      for (String property : database.getProperties()) {
        result.append(property + " = " + database.getProperty(property) + ", ");
      }
      result.delete(result.lastIndexOf(", "), result.length());
      result.append(")");
    }

    result.append(" ;");

    return result.toString();
  }
}
