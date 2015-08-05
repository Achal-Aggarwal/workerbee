package net.achalaggarwal.workerbee.dml.insert;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertQueryTest {
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String SELECT_QUERY_GENERATE = "SELECT_QUERY_GENERATE";
  Table table;
  SelectQuery selectQuery;

  @Before
  public void setup(){
    table = new Table(TABLE_NAME);
    selectQuery = mock(SelectQuery.class);
    when(selectQuery.generate()).thenReturn(SELECT_QUERY_GENERATE);
  }

  @Test
  public void shouldGenerateInsertQueryForInsertingIntoUnPartitionedTable(){
    assertThat(
      new InsertQuery().intoTable(table).using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME + " " + SELECT_QUERY_GENERATE)
    );
  }

  @Test
  public void shouldGenerateInsertQueryForOverwritingIntoUnPartitionedTable(){
    assertThat(
      new InsertQuery().overwrite().intoTable(table).using(selectQuery).generate(),
      is("INSERT OVERWRITE TABLE " + TABLE_NAME + " " + SELECT_QUERY_GENERATE)
    );
  }

  @Test
  public void shouldGenerateInsertQueryForInsertingIntoPartitionedTable(){
    Column column = new Column(table, COLUMN_NAME, Column.Type.STRING);
    table.partitionedOnColumn(column);

    assertThat(
      new InsertQuery().intoTable(table).using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME + " PARTITION ( " + COLUMN_NAME + " ) " + SELECT_QUERY_GENERATE)
    );

    assertThat(
      new InsertQuery().intoTable(table).partitionOn(column, "VALUE").using(selectQuery).generate(),
      is("INSERT INTO TABLE " + TABLE_NAME
        + " PARTITION ( " + COLUMN_NAME + " = 'VALUE' ) "
        + SELECT_QUERY_GENERATE)
    );
  }
}