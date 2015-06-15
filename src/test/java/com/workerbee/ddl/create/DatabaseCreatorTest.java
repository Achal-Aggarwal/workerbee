package com.workerbee.ddl.create;

import com.workerbee.Database;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.ddl.QueryGenerator.create;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DatabaseCreatorTest{
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String DATABASE_COMMENT = "DATABASE_COMMENT";

  Database database;

  @Before
  public void setup(){
    database = new Database(DATABASE_NAME);
  }

  @Test
  public void shouldGenerateCorrectBasicCreateHql(){
    assertThat(create(database).generate(), is(
      "CREATE DATABASE " + DATABASE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectBasicCreateHqlForDatabaseWithComment(){
    assertThat(create(database.withComment(DATABASE_COMMENT)).generate(), is(
      "CREATE DATABASE " + DATABASE_NAME + " COMMENT " + DATABASE_COMMENT + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlWithIfNotExist(){
    assertThat(create(database).ifNotExist().generate(), is(
      "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME + " ;"
    ));
  }
}