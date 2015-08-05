package net.achalaggarwal.workerbee;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class RowTest {
  public static final Integer INT_VALUE = 1;
  public static final Integer ANOTHER_INT_VALUE = 2;
  private  Row row;
  private final Column column = new Column(null, "COLUMN_NAME", Column.Type.INT);
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
  public void shouldNotSetValueOfColumnWithDifferentCombinationOfNameAndTypeInTheRow(){
    final Column differentTypeColumn = new Column(null, "COLUMN_NAME", Column.Type.STRING);
    row.set(differentTypeColumn, "SOME_STRING_VALUE");
    assertThat(row.get(differentTypeColumn), nullValue());

    final Column differentNameColumn = new Column(null, "COLUMN_NAME_", Column.Type.INT);
    row.set(differentNameColumn, ANOTHER_INT_VALUE);
    assertThat(row.get(differentNameColumn), nullValue());

    final Column sameNameAndTypeColumn = new Column(null, "COLUMN_NAME", Column.Type.INT);
    row.set(sameNameAndTypeColumn, ANOTHER_INT_VALUE);
    assertThat(row.getInt(sameNameAndTypeColumn), is(ANOTHER_INT_VALUE));
  }
}