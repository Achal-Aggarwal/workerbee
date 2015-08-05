package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.Database;
import org.junit.Before;
import org.junit.Test;

import static net.achalaggarwal.workerbee.QueryGenerator.create;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DatabaseCreatorTest{
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String DATABASE_COMMENT = "DATABASE_COMMENT";
  public static final String PATH = "PATH";
  public static final String PROP_VALUE = "PROP_VALUE";
  public static final String PROP_KEY = "PROP_KEY";

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
      "CREATE DATABASE " + DATABASE_NAME + " COMMENT '" + DATABASE_COMMENT + "' ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlWithIfNotExist(){
    assertThat(create(database).ifNotExist().generate(), is(
      "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlWithSpecifiedLocation(){
    assertThat(create(database.onLocation(PATH)).generate(), is(
      "CREATE DATABASE " + DATABASE_NAME + " LOCATION '" + PATH + "' ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlWithSpecifiedProperties(){
    assertThat(create(database.havingProperty(PROP_KEY, PROP_VALUE)).generate(), is(
      "CREATE DATABASE " + DATABASE_NAME + " WITH DBPROPERTIES ( " +
        "'" + PROP_KEY + "' = '"+ PROP_VALUE + "' ) ;"
    ));
  }
}