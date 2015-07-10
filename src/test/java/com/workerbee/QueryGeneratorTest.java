package com.workerbee;

import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import com.workerbee.ddl.drop.DatabaseDropper;
import com.workerbee.ddl.drop.TableDropper;
import com.workerbee.ddl.misc.LoadData;
import com.workerbee.ddl.misc.RecoverPartition;
import com.workerbee.dml.insert.InsertQuery;
import com.workerbee.dr.SelectQuery;
import org.junit.Test;

import static com.workerbee.Column.Type.STRING;
import static com.workerbee.QueryGenerator.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class QueryGeneratorTest {

  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldReturnDatabaseCreatorObjectForDatabaseObjectOnCreate() {
    assertThat(create(new Database("DatabaseName")), instanceOf(DatabaseCreator.class));
  }

  @Test
  public void shouldReturnDatabaseDropperObjectForDatabaseObjectOnDrop() {
    assertThat(drop(new Database("DatabaseName")), instanceOf(DatabaseDropper.class));
  }

  @Test
  public void shouldReturnTableCreatorObjectForTableObjectOnCreate() {
    assertThat(
      drop(new Table(new Database("DatabaseName"), "TableName")),
      instanceOf(TableDropper.class)
    );
  }

  @Test
  public void shouldReturnRecoverPartitionObjectForTableObjectOnRecover() {
    assertThat(
      recover(new Table(new Database("DatabaseName"), "TableName")),
      instanceOf(RecoverPartition.class)
    );
  }

  @Test
  public void shouldReturnTableDropperObjectForTableObjectOnCreate() {
    assertThat(
      create(new Table(new Database("DatabaseName"), "TableName")),
      instanceOf(TableCreator.class)
    );
  }

  @Test
  public void shouldReturnSelectQueryObjectForSelectQuery() {
    assertThat(
      select(new Column(null, COLUMN_NAME, STRING)),
      instanceOf(SelectQuery.class)
    );
  }

  @Test
  public void shouldReturnLoadDataObjectForLoadDataQuery() {
    assertThat(loadData(), instanceOf(LoadData.class));
  }
}