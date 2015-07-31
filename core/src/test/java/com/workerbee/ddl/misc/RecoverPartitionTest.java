package com.workerbee.ddl.misc;

import com.workerbee.Database;
import com.workerbee.Table;
import org.junit.Before;
import org.junit.Test;

import static com.workerbee.QueryGenerator.create;
import static com.workerbee.QueryGenerator.recover;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RecoverPartitionTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";

  @Test
  public void shouldGenerateCorrectBasicCreateHql(){
    assertThat(recover(new Table(new Database(DATABASE_NAME), TABLE_NAME)).generate(), is(
      "USE " + DATABASE_NAME + " ;MSCK REPAIR TABLE " + TABLE_NAME + " ;"
    ));
  }
}