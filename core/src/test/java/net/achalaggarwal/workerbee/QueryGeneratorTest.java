package net.achalaggarwal.workerbee;

import net.achalaggarwal.workerbee.ddl.create.DatabaseCreator;
import net.achalaggarwal.workerbee.ddl.create.TableCreator;
import net.achalaggarwal.workerbee.ddl.drop.DatabaseDropper;
import net.achalaggarwal.workerbee.ddl.drop.TableDropper;
import net.achalaggarwal.workerbee.ddl.misc.LoadData;
import net.achalaggarwal.workerbee.ddl.misc.RecoverPartition;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class QueryGeneratorTest {

  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldReturnDatabaseCreatorObjectForDatabaseObjectOnCreate() {
    assertThat(QueryGenerator.create(new Database("DatabaseName")), instanceOf(DatabaseCreator.class));
  }

  @Test
  public void shouldReturnDatabaseDropperObjectForDatabaseObjectOnDrop() {
    assertThat(QueryGenerator.drop(new Database("DatabaseName")), instanceOf(DatabaseDropper.class));
  }

  @Test
  public void shouldReturnTableCreatorObjectForTableObjectOnCreate() {
    assertThat(
      QueryGenerator.drop(new TextTable(new Database("DatabaseName"), "TableName")),
      instanceOf(TableDropper.class)
    );
  }

  @Test
  public void shouldReturnRecoverPartitionObjectForTableObjectOnRecover() {
    assertThat(
      QueryGenerator.recover(new TextTable(new Database("DatabaseName"), "TableName")),
      instanceOf(RecoverPartition.class)
    );
  }

  @Test
  public void shouldReturnTableDropperObjectForTableObjectOnCreate() {
    assertThat(
      QueryGenerator.create(new TextTable(new Database("DatabaseName"), "TableName")),
      instanceOf(TableCreator.class)
    );
  }

  @Test
  public void shouldReturnSelectQueryObjectForSelectQuery() {
    assertThat(
      QueryGenerator.select(new Column(null, COLUMN_NAME, Column.Type.STRING)),
      instanceOf(SelectQuery.class)
    );
  }

  @Test
  public void shouldReturnLoadDataObjectForLoadDataQuery() {
    assertThat(QueryGenerator.loadData(), instanceOf(LoadData.class));
  }
}