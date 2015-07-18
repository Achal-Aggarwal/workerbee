package com.workerbee;

import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import com.workerbee.ddl.drop.DatabaseDropper;
import com.workerbee.ddl.drop.TableDropper;
import com.workerbee.ddl.misc.LoadData;
import com.workerbee.ddl.misc.RecoverPartition;
import com.workerbee.ddl.misc.TruncateTable;
import com.workerbee.dml.insert.InsertQuery;
import com.workerbee.dr.SelectFunction;
import com.workerbee.dr.SelectQuery;
import com.workerbee.dr.selectfunction.ColumnSF;

import java.util.ArrayList;
import java.util.List;

public class QueryGenerator {
  public static DatabaseCreator create(Database database){
    return new DatabaseCreator(database);
  }

  public static DatabaseDropper drop(Database database) {
    return new DatabaseDropper(database);
  }

  public static TableCreator create(Table table){
    return new TableCreator(table);
  }

  public static TableDropper drop(Table table) {
    return new TableDropper(table);
  }

  public static RecoverPartition recover(Table table) {
    return new RecoverPartition(table);
  }

  public static SelectQuery select(SelectFunction... selectFunctions) {
    return new SelectQuery(selectFunctions);
  }

  public static SelectQuery select(Column... columns) {
    List<SelectFunction> selectFunctions = new ArrayList<SelectFunction>(columns.length);

    for (Column column : columns) {
      selectFunctions.add(new ColumnSF(column));
    }

    return new SelectQuery(selectFunctions);
  }

  public static InsertQuery insert() {
    return new InsertQuery();
  }

  public static InsertQuery insert(Row<? extends Table> row) {
    return new InsertQuery().using(select(row.getConstants()).from(Table.DUAL));
  }

  public static LoadData loadData() {
    return new LoadData();
  }

  public static TruncateTable truncate(Table<? extends Table> table) {
    return new TruncateTable(table);
  }
}
