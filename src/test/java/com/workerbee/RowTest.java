package com.workerbee;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static com.workerbee.Column.Type.INT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class RowTest {
  public static final Integer INT_VALUE = 1;
  public static final Integer ANOTHER_INT_VALUE = 2;
  private  Row row;
  private final Column column = new Column(null, "COLUMN_NAME", INT);
  @Before
  public void setup(){
    row = new Row<Table>(new Table("TABLE_NAME").havingColumn(column), "1");
  }

  @Test
  public void shouldCreateRowForATable(){
    assertThat(row, instanceOf(Row.class));
  }

  @Test
  public void shouldGetValueFromRowOfAColumn(){
    assertThat((Integer) row.get(column), is(INT_VALUE));
  }

  @Test
  public void shouldSetValueOfAnExistingColumnInTheRow(){
    assertThat(row.set(column, ANOTHER_INT_VALUE), is(row));
    assertThat(row.getInt(column), is(ANOTHER_INT_VALUE));
  }

  @Test
  public void shouldNotSetValueOfNonExistingColumnInTheRow(){
    final Column anotherColumn = new Column(null, "COLUMN_NAME", INT);
    assertThat(row.set(anotherColumn, ANOTHER_INT_VALUE), is(row));
    assertThat(row.get(anotherColumn), nullValue());
  }
}