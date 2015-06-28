package com.workerbee.dr;

import com.workerbee.*;
import com.workerbee.expression.BooleanExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectQuery implements Query {
  private Table table;
  private List<SelectFunction> selectFunctions = new ArrayList<SelectFunction>();
  private String alias;
  private Table joinTable;
  private BooleanExpression onBooleanExpression;
  private Integer limit;
  private List<ColumnOrder> orderBy = new ArrayList<ColumnOrder>();

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

  public SelectQuery on(BooleanExpression booleanExpression){
    onBooleanExpression = booleanExpression;
    return this;
  }

  public SelectQuery as(String alias){
    this.alias = alias;

    return this;
  }

  public SelectQuery limit(Integer limit){
    this.limit = limit;
    return this;
  }

  private SelectQuery orderBy(Column column, String order){
    this.orderBy.add(new ColumnOrder(column, order));
    return this;
  }

  public SelectQuery descOrderOf(Column column){
    return orderBy(column, ColumnOrder.DESC_ORDER);
  }

  public SelectQuery ascOrderOf(Column column){
    return orderBy(column, ColumnOrder.ASC_ORDER);
  }

  public Table table(){
    Table table = new Table(alias);

    for (SelectFunction selectFunction : selectFunctions) {
      table.havingColumn(new Column(table, selectFunction.getAlias(), selectFunction.getType()));
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

    if (joinTable != null && onBooleanExpression != null){
      result.append(" JOIN ");

      result.append(Utils.fqTableName(joinTable));

      result.append(" ON ");
      result.append(onBooleanExpression.generate());
    }

    if (alias != null){
      result.append(" AS " + alias);
    }

    if (!orderBy.isEmpty()){
      result.append(" ORDER BY ");

      List<String> orderByColumns = new ArrayList<String>(orderBy.size());
      for (ColumnOrder columnOrder : orderBy) {
        orderByColumns.add(columnOrder.column.getFqColumnName() + " " + columnOrder.order);
      }

      result.append(Utils.joinList(orderByColumns, ", "));
    }

    if (limit != null){
      result.append(" LIMIT " + limit);
    }

    result.append(" ;");

    return result.toString();
  }

  private class ColumnOrder {
    public static final String DESC_ORDER = "DESC";
    public static final String ASC_ORDER = "ASC";

    private Column column;
    private String order;

    public ColumnOrder(Column column, String order) {
      this.column = column;
      this.order = order;
    }
  }
}
