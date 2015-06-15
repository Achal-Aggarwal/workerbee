package com.workerbee;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DatabaseTest {

  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String DATABASE_COMMENT = "DATABASE_COMMENT";
  public static final String DATABASE_LOCATION = "DATABASE_LOCATION";
  public static final String PROPERTY_VALUE_ONE = "PROPERTY_VALUE_ONE";
  public static final String PROPERTY_KEY_ONE = "PROPERTY_KEY_ONE";
  public static final String DATABASE_COMMENT_2 = "DATABASE_COMMENT_2";

  Database database;

  @Before
  public void setup(){
    database = new Database(DATABASE_NAME);
  }

  @Test
  public void shouldCreateDatabaseObjectWithGivenName(){
    assertThat(database, instanceOf(Database.class));
    assertThat(database.getName(), is(DATABASE_NAME));
  }

  @Test
  public void shouldCreateDatabaseObjectWithGivenNameAndComment(){
    Database database = new Database(DATABASE_NAME, DATABASE_COMMENT);
    assertThat(database, instanceOf(Database.class));
    assertThat(database.getName(), is(DATABASE_NAME));
    assertThat(database.getComment(), is(DATABASE_COMMENT));
    assertThat(database.withComment(DATABASE_COMMENT_2).getComment(), is(DATABASE_COMMENT_2));
  }

  @Test
  public void shouldAbleToSetLocationOfADatabase(){
    assertThat(database.onLocation(DATABASE_LOCATION).getLocation(), is(DATABASE_LOCATION));
  }

  @Test
  public void shouldAbleToSetPropertiesOfADatabase(){
    database.havingProperty(PROPERTY_KEY_ONE, PROPERTY_VALUE_ONE);
    assertThat(database.getProperties().contains(PROPERTY_KEY_ONE), is(true));
    assertThat(database.getProperty(PROPERTY_KEY_ONE), is(PROPERTY_VALUE_ONE));
  }
}
