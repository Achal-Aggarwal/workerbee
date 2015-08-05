package net.achalaggarwal.workerbee.ddl.drop;

import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.QueryGenerator;
import org.junit.Before;
import org.junit.Test;

import static net.achalaggarwal.workerbee.QueryGenerator.drop;
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
    assertThat(QueryGenerator.drop(new Table(TABLE_NAME)).generate(), is(
      "DROP TABLE " + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectBasicDropHql(){
    assertThat(QueryGenerator.drop(table).generate(), is(
      "DROP TABLE " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectDropHqlWithIfExist(){
    assertThat(QueryGenerator.drop(table).ifExist().generate(), is(
      "DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }
}