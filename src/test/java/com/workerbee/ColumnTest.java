package com.workerbee;

import org.junit.Test;

import static com.workerbee.Column.Type.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ColumnTest {
  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldCreateColumn(){
    Column column = new Column(COLUMN_NAME, SMALLINT);
    assertThat(column, instanceOf(Column.class));
    assertThat(column.getName(), is(COLUMN_NAME));
    assertThat(column.getType(), is(SMALLINT));
  }
}