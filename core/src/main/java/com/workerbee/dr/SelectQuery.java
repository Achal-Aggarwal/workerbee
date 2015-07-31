package com.workerbee.dr;

import com.workerbee.*;
import com.workerbee.dr.selectfunction.AllStarSF;
import com.workerbee.expression.BooleanExpression;

import java.util.*;

public class SelectQuery implements Query {
  private Table<? extends Table> table;
  private List<SelectFunction> selectFunctions = new ArrayList<>();
  private String alias;
  private Table<? extends Table> joinTable;
  private BooleanExpression onBooleanExpression;
  private Integer limit;
  private List<ColumnOrder> orderBy = new ArrayList<>();
  private List<Column> groupBy = new ArrayList<>();
  private SelectQuery selectQuery;
  private BooleanExpression where;

  public SelectQuery(List<SelectFunction> selectFunctions) {
    this.selectFunctions.addAll(selectFunctions);
  }

  public SelectQuery(SelectFunction... selectFunctions){
    this(Arrays.asList(selectFunctions));
  }

  public SelectQuery from(Table<? extends Table> table){
    this.table = table;

    as(table.isNotTemporary()
      ? table.getDatabaseName() + "_" + table.getName()
      : "_" + table.getName()
    );

    return this;
  }

  public SelectQuery join(Table<? extends Table> table) {
    joinTable = table;
    return this;
  }

  public SelectQuery join(SelectQuery selectQuery) {
    this.selectQuery = selectQuery;
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

  public SelectQuery where(BooleanExpression booleanExpression){
    this.where = booleanExpression;

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

  public SelectQuery groupBy(Column column, Column... columns){
    this.groupBy.add(column);
    Collections.addAll(this.groupBy, columns);
    return this;
  }

  public SelectQuery descOrderOf(Column column){
    return orderBy(column, ColumnOrder.DESC_ORDER);
  }

  public SelectQuery ascOrderOf(Column column){
    return orderBy(column, ColumnOrder.ASC_ORDER);
  }

  public Table<Table> table(Database database){
    Table<Table> table = new Table<>(database, alias);

    for (SelectFunction selectFunction : selectFunctions) {
      if (selectFunction instanceof AllStarSF){
        for (Column column : this.table.getColumns()) {
          table.havingColumn(new Column(table, column.getName(), column.getType()));
        }
        for (Column column : this.table.getPartitions()) {
          table.havingColumn(new Column(table, column.getName(), column.getType()));
        }
      } else {
        table.havingColumn(new Column(table, selectFunction.getAlias(), selectFunction.getType()));
      }
    }

    return table;
  }

  public Table<Table> table(){
    return table(null);
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("SELECT");

    if (!selectFunctions.isEmpty()){
      selectFuncPart(result);
    }

    result.append(" FROM ");

    result.append(Utils.fqTableName(table));

    if (joinTable != null && onBooleanExpression != null){
      joinTablePart(result);
    }
    else if (selectQuery != null && onBooleanExpression != null){
      joinSelectQueryPart(result);
    }

    if (where != null){
      wherePart(result);
    }

    if (!groupBy.isEmpty()){
      groupByPart(result);
    }

    if (!orderBy.isEmpty()){
      orderByPart(result);
    }

    if (limit != null){
      result.append(" LIMIT " + limit);
    }

    return result.toString();
  }

  private void wherePart(StringBuilder result) {
    result.append(" WHERE ");
    result.append(where.generate());
  }

  private void selectFuncPart(StringBuilder result) {
    for (SelectFunction selectFunction : selectFunctions) {
      if (selectFunction instanceof AllStarSF){
        for (Column column : this.table.getColumns()) {
          result.append(" " + column.getFqColumnName() + ",");
        }
        for (Column column : this.table.getPartitions()) {
          result.append(" " + column.getFqColumnName() + ",");
        }
      } else {
        result.append(" " + selectFunction.generate() + ",");
      }
    }
    result.delete(result.lastIndexOf(","), result.length());

  }

  private void joinTablePart(StringBuilder result) {

    result.append(" AS " + table.getDatabaseName() + "_" + table.getName());

    result.append(" JOIN ");

    result.append(Utils.fqTableName(joinTable));

    result.append(" AS " + joinTable.getDatabaseName() + "_" + joinTable.getName());

    result.append(" ON ");
    result.append(onBooleanExpression.generate());
  }

  private void joinSelectQueryPart(StringBuilder result) {
    result.append(" JOIN (");

    result.append(selectQuery.generate() + ") " + selectQuery.alias);

    result.append(" ON ");
    result.append(onBooleanExpression.generate());
  }

  private void orderByPart(StringBuilder result) {
    result.append(" ORDER BY ");

    List<String> orderByColumns = new ArrayList<>(orderBy.size());
    for (ColumnOrder columnOrder : orderBy) {
      orderByColumns.add(columnOrder.column.getFqColumnName() + " " + columnOrder.order);
    }

    result.append(Utils.joinList(orderByColumns, ", "));
  }

  private void groupByPart(StringBuilder result) {
    result.append(" GROUP BY ");

    List<String> groupByColumns = new ArrayList<>(groupBy.size());
    for (Column column : groupBy) {
      groupByColumns.add(column.getFqColumnName());
    }

    result.append(Utils.joinList(groupByColumns, ", "));
  }

  private static class ColumnOrder {
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
