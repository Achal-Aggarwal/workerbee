package com.workerbee;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.workerbee.Column.Type.INT;
import static com.workerbee.Column.Type.STRING;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TableTest {

  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_COMMENT = "TABLE_COMMENT";
  public static final String PROP_VALUE = "PROP_VALUE";
  public static final String PROP_KEY = "PROP_KEY";
  public static final String TABLE_LOCATION = "TABLE_LOCATION";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final boolean EMPTY = true;
  public static final boolean NOT_EMPTY = false;

  private Table table;

  @Before
  public void setup(){
    table = new Table(TABLE_NAME);
  }

  @Test
  public void shouldCreateTemporaryTableWithName(){
    assertThat(table, instanceOf(Table.class));
    assertThat(table.getName(), is(TABLE_NAME));
    assertThat(table.isNotTemporary(), is(false));
  }

  @Test
  public void shouldCreateTableWithNameAndDatabase(){
    Table table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
    assertThat(table, instanceOf(Table.class));
    assertThat(table.getDatabaseName(), is(DATABASE_NAME));
    assertThat(table.isNotTemporary(), is(true));
  }

  @Test
  public void shouldAddCommentToTable(){
    table.withComment(TABLE_COMMENT);
    assertThat(table.getComment(), is(TABLE_COMMENT));
  }

  @Test
  public void shouldAddPropertyToTable(){
    table.havingProperty(PROP_KEY, PROP_VALUE);
    assertThat(table.getProperties(), contains(PROP_KEY));
    assertThat(table.getProperty(PROP_KEY), is(PROP_VALUE));
  }

  @Test
  public void shouldAddLocationToTable(){
    table.onLocation(TABLE_LOCATION);
    assertThat(table.getLocation(), is(TABLE_LOCATION));
  }

  @Test
  public void shouldMarkTableExternal(){
    table.external();
    assertThat(table.isExternal(), is(true));
  }

  @Test
  public void shouldAddColumnsToTable(){
    assertThat(table.getColumns().isEmpty(), is(EMPTY));
    table.havingColumn(COLUMN_NAME, STRING);

    List<Column> columns = table.getColumns();
    assertThat(columns.isEmpty(), is(NOT_EMPTY));
    assertThat(columns.get(0), instanceOf(Column.class));
    assertThat(columns.get(0).getName(), is(COLUMN_NAME));
    assertThat(columns.get(0).getType(), is(STRING));
  }

  @Test
  public void shouldAddGivenColumnToTable(){
    Column column = new Column(COLUMN_NAME, STRING);
    table.havingColumn(column);
    assertThat(table.getColumns(), contains(column));
  }

  @Test
  public void shouldGetNewRowOfTable(){
    Column column = new Column(COLUMN_NAME, STRING);
    table.havingColumn(column);
    Row newRow = table.getNewRow();
    assertThat(newRow.get(column), nullValue());
  }

  @Test
  public void shouldParseRecordAndReturnCorrespondingRowOfTable(){
    Column integer = new Column("INT_COLUMN_NAME", INT);
    Column string = new Column("STRING_COLUMN_NAME", STRING);

    table.havingColumn(integer);
    table.havingColumn(string);

    Row row = table.parseRecordUsing("1:ASD");
    assertThat((Integer) row.get(integer), is(1));
    assertThat((String) row.get(string), is("ASD"));
  }

  @Test
  public void shouldGenerateRecordStringForCorrespondingRowOfTable(){
    Column integer = new Column("INT_COLUMN_NAME", INT);
    Column string = new Column("STRING_COLUMN_NAME", STRING);

    table.havingColumn(integer);
    table.havingColumn(string);

    Row row = table.getNewRow().set(integer, 1).set(string, "ASD");
    assertThat(table.generateRecordFor(row), is("1:ASD"));
  }
}