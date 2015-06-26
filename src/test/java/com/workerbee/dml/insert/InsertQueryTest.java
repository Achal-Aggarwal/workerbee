package com.workerbee.dml.insert;

import com.workerbee.Column;
import com.workerbee.Table;
import com.workerbee.dr.SelectQuery;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.Column.Type.STRING;
import static com.workerbee.QueryGenerator.*;
import static com.workerbee.dml.insert.InsertQuery.DONT_OVERWRITE;
import static com.workerbee.dml.insert.InsertQuery.OVERWRITE;
import static com.workerbee.dr.SelectFunctionGenerator.star;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class InsertQueryTest {
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  Table table;
  SelectQuery selectQuery;

  @Before
  public void setup(){
    table = new Table(TABLE_NAME);
    selectQuery = select(star()).from(table);
  }

  @Test
  public void shouldGenerateInsertQueryForInsertingIntoUnPartitionedTable(){
    assertThat(
      new InsertQuery(DONT_OVERWRITE).intoTable(table).using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME + " " + selectQuery.generate())
    );
  }

  @Test
  public void shouldGenerateInsertQueryForOverwritingIntoUnPartitionedTable(){
    assertThat(
      new InsertQuery(OVERWRITE).intoTable(table).using(selectQuery).generate(),
      is("INSERT OVERWRITE TABLE " + TABLE_NAME + " " + selectQuery.generate())
    );
  }

  @Test
  public void shouldGenerateInsertQueryForInsertingIntoPartitionedTable(){
    Column column = new Column(table, COLUMN_NAME, STRING);
    table.partitionedOnColumn(column);

    assertThat(
      new InsertQuery(DONT_OVERWRITE).intoTable(table).using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME + " PARTITION ( " + COLUMN_NAME + " ) " + selectQuery.generate())
    );

    assertThat(
      new InsertQuery(DONT_OVERWRITE).intoTable(table).partitionOn(column, "VALUE").using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME
        + " PARTITION ( " + COLUMN_NAME + " = 'VALUE' ) "
        + selectQuery.generate())
    );
  }
}