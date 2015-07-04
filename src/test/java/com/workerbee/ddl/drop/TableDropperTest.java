package com.workerbee.ddl.drop;

import com.workerbee.Database;
import com.workerbee.Table;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.QueryGenerator.drop;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class TableDropperTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";

  Table table;

  @Before
  public void setup(){
    table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
  }

  @Test
  public void shouldGenerateCorrectBasicDropHqlForTemporaryTable(){
    assertThat(drop(new Table(TABLE_NAME)).generate(), is(
      "DROP TABLE " + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectBasicDropHql(){
    assertThat(drop(table).generate(), is(
      "DROP TABLE " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectDropHqlWithIfExist(){
    assertThat(drop(table).ifExist().generate(), is(
      "DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }
}