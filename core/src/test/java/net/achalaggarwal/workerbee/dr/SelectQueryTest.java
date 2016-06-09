package net.achalaggarwal.workerbee.dr;

import net.achalaggarwal.workerbee.*;
import net.achalaggarwal.workerbee.dr.selectfunction.Constant;
import org.junit.Before;
import org.junit.Test;

import static net.achalaggarwal.workerbee.Column.Type.INT;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SelectQueryTest {

  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String ALIAS = "ALIAS";
  public static final int LIMIT = 5;
  public static final String GENERATED_TABLE_NAME = "GENERATED_TABLE_NAME";

  private TextTable table;
  private Column column;

  @Before
  public void setup(){
    table = new TextTable(new Database(DATABASE_NAME), TABLE_NAME);
    column = new Column(table, COLUMN_NAME, INT);
    table.havingColumn(column);
  }

  @Test
  public void shouldGenerateBasicSelectQuery(){
    assertThat(QueryGenerator.select(column).from(table).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME));
  }

  @Test
  public void shouldGenerateSelectQueryWithGroupBy(){
    assertThat(QueryGenerator.select(column).from(table).groupBy(column).generate(),
      is(
        "SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
          + " GROUP BY " + TABLE_NAME + "." + COLUMN_NAME
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithWhereClause(){
    assertThat(QueryGenerator.select(column).from(table).where(column.eq(new Constant(1))).generate(),
      is(
        "SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " WHERE " + TABLE_NAME + "." + COLUMN_NAME + " = " + 1
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithLimit(){
    assertThat(QueryGenerator.select(column).from(table).limit(LIMIT).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME
        + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " LIMIT " + LIMIT
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryWithOutAddingAliasOnSingleTableEvenIfProvided(){
    assertThat(QueryGenerator.select(column).from(table).as(ALIAS).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME));
  }

  @Test
  public void shouldGenerateSelectQueryHavingJoinUsingAnotherTable(){
    assertThat(QueryGenerator.select(column).from(table).join(table).on(column.eq(column)).generate(),
      is("SELECT " + TABLE_NAME + "." + COLUMN_NAME + " FROM "
        + DATABASE_NAME + "." + TABLE_NAME + " AS " + DATABASE_NAME + "_" + TABLE_NAME
        + " JOIN " + DATABASE_NAME + "." + TABLE_NAME + " AS " + DATABASE_NAME + "_" + TABLE_NAME
        + " ON " + TABLE_NAME + "." + COLUMN_NAME + " = " + TABLE_NAME + "." + COLUMN_NAME
      )
    );
  }

  @Test
  public void shouldGenerateSelectQueryHavingJoinUsingAnotherSelectQuery(){
    SelectQuery selectQuery = QueryGenerator.select(column).from(table).as(GENERATED_TABLE_NAME);

    assertThat(
      QueryGenerator.select(column).from(table)
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
    Table genTable = QueryGenerator.select(column).from(table).table();

    assertThat(genTable.getName(), is(DATABASE_NAME + "_" + TABLE_NAME));
    assertThat(genTable.getColumns().size(), is(1));
    assertThat(genTable.getColumns().contains(column), is(true));
    assertThat(genTable.isNotTemporary(), is(false));

    genTable = QueryGenerator.select(column).from(table).as(GENERATED_TABLE_NAME).table();

    assertThat(genTable.getName(), is(GENERATED_TABLE_NAME));
    assertThat(genTable.getColumns().size(), is(1));
    assertThat(genTable.getColumns().contains(column), is(true));
    assertThat(genTable.isNotTemporary(), is(false));
  }
}