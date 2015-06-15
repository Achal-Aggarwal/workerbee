package com.workerbee.ddl;

import com.workerbee.Database;
import com.workerbee.Table;
import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class QueryGeneratorTest {
  @Test
  public void shouldReturnDatabaseCreatorObjectForDatabaseObjectOnCreate(){
    assertThat(QueryGenerator.create(new Database("DatabaseName")), instanceOf(DatabaseCreator.class));
  }

  @Test
  public void shouldReturnTableCreatorObjectForTableObjectOnCreate(){
    assertThat(QueryGenerator.create(new Table(new Database("DatabaseName"),"TableName")),
      instanceOf(TableCreator.class));
  }
}