package com.workerbee;

import com.workerbee.Column;
import com.workerbee.Database;
import com.workerbee.QueryGenerator;
import com.workerbee.Table;
import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import com.workerbee.dr.SelectQuery;
import org.junit.Test;

import static com.workerbee.Column.Type.*;
import static com.workerbee.QueryGenerator.create;
import static com.workerbee.QueryGenerator.select;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class QueryGeneratorTest {

  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldReturnDatabaseCreatorObjectForDatabaseObjectOnCreate(){
    assertThat(create(new Database("DatabaseName")), instanceOf(DatabaseCreator.class));
  }

  @Test
  public void shouldReturnTableCreatorObjectForTableObjectOnCreate(){
    assertThat(create(new Table(new Database("DatabaseName"), "TableName")),
      instanceOf(TableCreator.class));
  }

  @Test
  public void shouldReturnSelectQueryObjectForSelectQuery(){
    assertThat(select(new Column(COLUMN_NAME, STRING)),
      instanceOf(SelectQuery.class));
  }
}