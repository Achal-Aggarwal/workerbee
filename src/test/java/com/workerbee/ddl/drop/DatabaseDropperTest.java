package com.workerbee.ddl.drop;

import com.workerbee.Database;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.QueryGenerator.drop;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DatabaseDropperTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";

  Database database;

  @Before
  public void setup(){
    database = new Database(DATABASE_NAME);
  }

  @Test
  public void shouldGenerateCorrectBasicDropHql(){
    assertThat(drop(database).generate(), is(
      "DROP DATABASE " + DATABASE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectDropHqlWithCascade(){
    assertThat(drop(database).cascade().generate(), is(
      "DROP DATABASE " + DATABASE_NAME + " CASCADE ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectDropHqlWithIfExist(){
    assertThat(drop(database).ifExist().generate(), is(
      "DROP DATABASE IF EXISTS " + DATABASE_NAME + " ;"
    ));
  }
}