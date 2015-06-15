package com.workerbee.ddl;

import com.workerbee.Database;
import com.workerbee.Table;
import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;

public class QueryGenerator {
  public static DatabaseCreator create(Database database){
    return new DatabaseCreator(database);
  }

  public static TableCreator create(Table table){
    return new TableCreator(table);
  }
}
