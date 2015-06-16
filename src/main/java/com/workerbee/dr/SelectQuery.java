package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.Query;
import com.workerbee.Table;
import com.workerbee.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.workerbee.Utils.fqColumnName;

public class SelectQuery implements Query {
  private Table table;
  private List<SelectFunction> selectFunctions = new ArrayList<SelectFunction>();
  private String alias;
  private Table joinTable;
  private Column tableAColumn;
  private Column tableBColumn;

  public SelectQuery(SelectFunction... selectFunctions){
    this.selectFunctions.addAll(Arrays.asList(selectFunctions));
  }

  public SelectQuery(List<SelectFunction> selectFunctions) {
    this.selectFunctions.addAll(selectFunctions);
  }

  public SelectQuery from(Table table){
    this.table = table;

    return this;
  }

  public SelectQuery join(Table table) {
    joinTable = table;
    return this;
  }

  public SelectQuery on(Column tableAColumn, Column tableBColumn){
    this.tableAColumn = tableAColumn;
    this.tableBColumn = tableBColumn;

    return this;
  }

  public SelectQuery as(String alias){
    this.alias = alias;

    return this;
  }

  public Table table(){
    Table table = new Table(alias);

    for (SelectFunction selectFunction : selectFunctions) {
      table.havingColumn(new Column(selectFunction.getAlias(), selectFunction.getType()));
    }

    return table;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("SELECT");

    if (!selectFunctions.isEmpty()){
      for (SelectFunction selectFunction : selectFunctions) {
        result.append(" " + selectFunction.generate() + ",");
      }
      result.delete(result.lastIndexOf(","), result.length());
    }

    result.append(" FROM ");

    result.append(Utils.fqTableName(table));

    if (joinTable != null && tableAColumn != null && tableBColumn != null){
      result.append(" JOIN ");

      result.append(Utils.fqTableName(joinTable));

      result.append(" ON ");
      result.append(fqColumnName(table, tableAColumn) + " = " + fqColumnName(joinTable, tableBColumn));
    }

    if (alias != null){
      result.append(" AS " + alias);
    }

    result.append(" ;");

    return result.toString();
  }
}
