package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.Database;
import com.workerbee.Table;
import com.workerbee.dr.selectfunction.Constant;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.Column.Type.INT;
import static com.workerbee.QueryGenerator.select;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SelectQueryTest {

  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String ALIAS = "ALIAS";
  public static final int LIMIT = 5;
  public static final String GENERATED_TABLE_NAME = "GENERATED_TABLE_NAME";

  private Table table;
  private Column column;

  @Before
  public void setup(){
    table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
    column = new Column(table, COLUMN_NAME, INT);
    table.havingColumn(column);
  }

  @Test
  public void shouldGenerateBasicSelectQuery(){
    assertThat(select(column).from(table).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME));
  }

  @Test
  public void shouldGenerateSelectQueryWithGroupBy(){
    assertThat(select(column).from(table).groupBy(column).generate(),
      is(
        "SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
          + " GROUP BY " + TABLE_NAME + "." + COLUMN_NAME
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithWhereClause(){
    assertThat(select(column).from(table).where(column.eq(new Constant(1))).generate(),
      is(
        "SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " WHERE " + TABLE_NAME + "." + COLUMN_NAME + " = " + 1
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithLimit(){
    assertThat(select(column).from(table).limit(LIMIT).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME
        + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " LIMIT " + LIMIT
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithOutAddingAliasOnSingleTableEvenIfProvided(){
    assertThat(select(column).from(table).as(ALIAS).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME));
  }

  @Test
  public void shouldGenerateSelectQueryHavingJoinUsingAnotherTable(){
    assertThat(select(column).from(table).join(table).on(column.eq(column)).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM "
        + DATABASE_NAME + "." + TABLE_NAME + " AS " + DATABASE_NAME + "_" + TABLE_NAME
        + " JOIN " + DATABASE_NAME + "." + TABLE_NAME + " AS " + DATABASE_NAME + "_" + TABLE_NAME
        + " ON " + TABLE_NAME + "." + COLUMN_NAME + " = " + TABLE_NAME + "." + COLUMN_NAME
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryHavingJoinUsingAnotherSelectQuery(){
    SelectQuery selectQuery = select(column).from(table).as(GENERATED_TABLE_NAME);

    assertThat(
      select(column).from(table)
        .join(selectQuery)
        .on(column.eq(selectQuery.table().getColumn(column)))
        .generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " JOIN (SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME + ") "
        + GENERATED_TABLE_NAME +" ON "
        + TABLE_NAME + "." + COLUMN_NAME + " = " + GENERATED_TABLE_NAME + "." + COLUMN_NAME
      )
    );
  }

  @Test
  public void shouldGiveTableWithGivenSelectQuery(){
    Table genTable = select(column).from(table).table();

    assertThat(genTable.getName(), is(DATABASE_NAME + "_" + TABLE_NAME));
    assertThat(genTable.getColumns().size(), is(1));
    assertThat(genTable.getColumns().contains(column), is(true));
    assertThat(genTable.isNotTemporary(), is(false));

    genTable = select(column).from(table).as(GENERATED_TABLE_NAME).table();

    assertThat(genTable.getName(), is(GENERATED_TABLE_NAME));
    assertThat(genTable.getColumns().size(), is(1));
    assertThat(genTable.getColumns().contains(column), is(true));
    assertThat(genTable.isNotTemporary(), is(false));
  }
}