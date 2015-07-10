package com.workerbee.ddl.misc;

import com.workerbee.Column;
import com.workerbee.Database;
import com.workerbee.Table;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static com.workerbee.Column.Type.STRING;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadDataTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String PATH = "PATH";

  private static Table table = new Table(new Database(DATABASE_NAME), TABLE_NAME);
  private static Path path = mock(Path.class, Mockito.RETURNS_DEEP_STUBS);

  static {
    when(path.toAbsolutePath().toString())
      .thenReturn(PATH);
  }

  @Test
  public void shouldGenerateCorrectStatementForTemporaryTable(){
    assertThat(
      new LoadData().into(new Table(TABLE_NAME)).fromLocal(path).generate(),
      is("LOAD DATA LOCAL INPATH 'PATH' INTO TABLE TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementForLocalPath(){
    assertThat(
      new LoadData().into(table).fromLocal(path).generate(),
      is("LOAD DATA LOCAL INPATH 'PATH' INTO TABLE DATABASE_NAME.TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementWithOverwrite(){
    assertThat(
      new LoadData().into(table).overwrite().fromLocal(path).generate(),
      is("LOAD DATA LOCAL INPATH 'PATH' OVERWRITE INTO TABLE DATABASE_NAME.TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementWithPartitionedTable(){
    Table partitionedTable = new Table(TABLE_NAME);
    Column column = new Column(partitionedTable, "PAT_COL_NAME", STRING);
    partitionedTable.partitionedOnColumn(column);

    assertThat(
      new LoadData().into(partitionedTable).overwrite().fromLocal(path).generate(),
      is("LOAD DATA LOCAL INPATH 'PATH' OVERWRITE INTO TABLE TABLE_NAME PARTITION ( PAT_COL_NAME ) ;")
    );
  }
}