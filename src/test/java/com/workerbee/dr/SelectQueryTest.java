package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.Database;
import com.workerbee.QueryGenerator;
import com.workerbee.Table;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.Column.Type.SMALLINT;
import static com.workerbee.QueryGenerator.select;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SelectQueryTest {

  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String ALIAS = "ALIAS";

  private Table table;
  Column column = new Column(COLUMN_NAME, SMALLINT);

  @Before
  public void setup(){
    table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
    table.havingColumn(column);

  }

  @Test
  public void shouldGenerateBasicSelectQuery(){
    assertThat(select(column).from(table).generate(),
      is("SELECT " + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME + " ;"));
  }

  @Test
  public void shouldGenerateSelectQueryWithTableAlias(){
    assertThat(select(column).from(table).as(ALIAS).generate(),
      is("SELECT " + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME + " AS " + ALIAS + " ;"));
  }

  @Test
  public void shouldGenerateSelectQueryHavingJoin(){
    assertThat(select(column).from(table).join(table).on(column, column).generate(),
      is("SELECT " + COLUMN_NAME + " FROM " + DATABASE_NAME + "." + TABLE_NAME
        + " JOIN " + DATABASE_NAME + "." + TABLE_NAME
        + " ON " + TABLE_NAME + "." + COLUMN_NAME + " = " + TABLE_NAME + "." + COLUMN_NAME
        + " ;"));
  }
}