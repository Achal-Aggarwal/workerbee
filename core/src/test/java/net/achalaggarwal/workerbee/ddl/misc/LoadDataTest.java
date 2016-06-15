package net.achalaggarwal.workerbee.ddl.misc;

import net.achalaggarwal.workerbee.*;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadDataTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String PATH = "/PATH";
  public static final String PAT_COL_VALUE = "PAT_COL_VALUE";
  public static final String PAT_COL_NAME = "PAT_COL_NAME";

  private static Table table = new TextTable(new Database(DATABASE_NAME), TABLE_NAME);
  private static Path path = mock(Path.class, Mockito.RETURNS_DEEP_STUBS);

  static {
    when(path.toAbsolutePath().toString())
      .thenReturn(PATH);
    try {
      when(path.toUri())
        .thenReturn(new URI("file:///PATH"));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void shouldGenerateCorrectStatementForTemporaryTable(){
    assertThat(
      new LoadData().into(new TextTable<>(TABLE_NAME)).from(path.toUri()).generate(),
      is("LOAD DATA LOCAL INPATH '/PATH' INTO TABLE TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementForLocalPath(){
    assertThat(
      new LoadData().into(table).from(path.toUri()).generate(),
      is("LOAD DATA LOCAL INPATH '/PATH' INTO TABLE DATABASE_NAME.TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementWithOverwrite(){
    assertThat(
      new LoadData().into(table).overwrite().from(path.toUri()).generate(),
      is("LOAD DATA LOCAL INPATH '/PATH' OVERWRITE INTO TABLE DATABASE_NAME.TABLE_NAME ;")
    );
  }

  @Test
  public void shouldGenerateCorrectStatementWithPartitionedTable(){
    TextTable partitionedTable = new TextTable<>(TABLE_NAME);
    Column column = new Column(partitionedTable, PAT_COL_NAME, Column.Type.STRING);
    partitionedTable.partitionedOnColumn(column);

    assertThat(
      new LoadData()
        .data(partitionedTable.getNewRow().set(column, PAT_COL_VALUE))
        .into(partitionedTable)
        .overwrite()
        .from(path.toUri())
        .generate(),
      is("LOAD DATA LOCAL INPATH '/PATH' OVERWRITE INTO TABLE TABLE_NAME PARTITION ( PAT_COL_NAME = 'PAT_COL_VALUE' ) ;")
    );
  }
}