package com.workerbee.ddl.drop;

import com.workerbee.Database;

public class DatabaseDropper {
  private Database database;
  private boolean cascade;
  private boolean ifExist;

  public DatabaseDropper(Database database) {
    this.database = database;
  }


  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("DROP DATABASE");

    if (ifExist){
      result.append(" IF EXISTS");
    }

    result.append(" " + database.getName());

    if (cascade){
      result.append(" CASCADE");
    }

    result.append(" ;");

    return result.toString();
  }

  public DatabaseDropper cascade() {
    cascade = true;
    return this;
  }

  public DatabaseDropper ifExist() {
    ifExist = true;
    return this;
  }
}
