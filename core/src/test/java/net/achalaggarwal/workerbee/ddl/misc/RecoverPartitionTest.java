package net.achalaggarwal.workerbee.ddl.misc;

import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.QueryGenerator;
import net.achalaggarwal.workerbee.TextTable;
import org.junit.Test;

import static net.achalaggarwal.workerbee.QueryGenerator.create;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RecoverPartitionTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";

  @Test
  public void shouldGenerateCorrectBasicCreateHql(){
    assertThat(QueryGenerator.recover(new TextTable(new Database(DATABASE_NAME), TABLE_NAME)).generate(), is(
      "USE " + DATABASE_NAME + " ; MSCK REPAIR TABLE " + TABLE_NAME + " ;"
    ));
  }
}