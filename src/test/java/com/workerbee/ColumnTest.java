package com.workerbee;

import com.workerbee.expression.*;
import org.junit.Test;

import static com.workerbee.Column.Type.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ColumnTest {
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final Integer INT_VALUE = 1;
  public static final int INDEX = 0;
  public static final String STRING_VALUE = "STRING_VALUE";
  Column column = new Column(null, COLUMN_NAME, INT);

  @Test
  public void shouldCreateColumn(){
    assertThat(column, instanceOf(Column.class));
    assertThat(column.getName(), is(COLUMN_NAME));
    assertThat(column.getType(), is(INT));
  }

  @Test
  public void shouldParseIntegerValueUsingRecordParser(){
    RecordParser mockRecordParser = mock(RecordParser.class);

    when(mockRecordParser.readInt(INDEX)).thenReturn(INT_VALUE);

    assertThat((Integer) column.parseValueUsing(mockRecordParser, INDEX), is(INT_VALUE));

    verify(mockRecordParser).readInt(INDEX);
  }

  @Test
  public void shouldParseStringValueUsingRecordParser(){
    Column column = new Column(null, COLUMN_NAME, STRING);
    RecordParser mockRecordParser = mock(RecordParser.class);

    when(mockRecordParser.readString(INDEX)).thenReturn(STRING_VALUE);

    assertThat((String) column.parseValueUsing(mockRecordParser, INDEX), is(STRING_VALUE));

    verify(mockRecordParser).readString(INDEX);
  }

  @Test
  public void shouldBeAComparableGiveFqColumnWhenOperandNameIsAskedFor(){
    assertThat(column, instanceOf(com.workerbee.expression.Comparable.class));
    assertThat(column.operandName(), is(column.getFqColumnName()));
  }
}