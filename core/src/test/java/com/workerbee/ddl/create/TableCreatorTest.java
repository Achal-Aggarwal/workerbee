package com.workerbee.ddl.create;

import com.workerbee.Column;
import com.workerbee.Database;
import com.workerbee.Table;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.Column.Type.STRING;
import static com.workerbee.QueryGenerator.create;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class TableCreatorTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String TABLE_COMMENTS = "TABLE_COMMENTS";
  public static final String PATH = "PATH";
  public static final String PROP_VALUE = "PROP_VALUE";
  public static final String PROP_KEY = "PROP_KEY";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String NEW_DATABASE_NAME = "NEW_DATABASE_NAME";

  Table table;

  @Before
  public void setup(){
    table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
  }

  @Test
  public void shouldGenerateCorrectBasicCreateHql(){
    assertThat(create(table).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfExternalTable(){
    assertThat(create(table.external()).generate(), is(
      "CREATE EXTERNAL TABLE " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithIfNotExist(){
    assertThat(create(table).ifNotExist().generate(), is(
      "CREATE TABLE IF NOT EXISTS " + DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTemporaryTable(){
    assertThat(create(new Table(TABLE_NAME)).generate(), is(
      "CREATE TABLE "  + TABLE_NAME + " ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithComments(){
    assertThat(create(table.withComment(TABLE_COMMENTS)).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " COMMENT '" + TABLE_COMMENTS + "' ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithGivenLocation(){
    assertThat(create(table.onLocation(PATH)).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " LOCATION '" + PATH + "' ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithGivenProperties(){
    assertThat(create(table.havingProperty(PROP_KEY, PROP_VALUE)).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " TBLPROPERTIES ( "
        + "'" + PROP_KEY + "' = '" + PROP_VALUE + "'"
        + " ) ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithColumnInfo(){
    assertThat(create(table.havingColumn(COLUMN_NAME, STRING)).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " ( "
        + COLUMN_NAME + " " + STRING
        + " ) ;"
    ));
  }

  @Test
  public void shouldGenerateCorrectCreateHqlOfTableWithPartitionColumnInfo(){
    assertThat(create(table.partitionedOnColumn(new Column(table, COLUMN_NAME, STRING))).generate(), is(
      "CREATE TABLE " + DATABASE_NAME + "." + TABLE_NAME + " PARTITIONED BY ( "
        + COLUMN_NAME + " " + STRING
        + " ) ;"
    ));
  }

  @Test
  public void shouldAllowToCreateTableInDifferentDatabase(){
    assertThat(create(table).inDatabase(new Database(NEW_DATABASE_NAME)).generate(), is(
      "CREATE TABLE " + NEW_DATABASE_NAME + "." + TABLE_NAME + " ;"
    ));
  }
}