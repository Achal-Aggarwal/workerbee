package com.workerbee;

import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import com.workerbee.ddl.misc.LoadData;
import com.workerbee.dml.insert.InsertQuery;
import com.workerbee.dr.SelectQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.logging.LogManager;

import static com.workerbee.Database.DEFAULT;
import static com.workerbee.Table.DUAL;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
  Repository.class,
  DriverManager.class,
  DatabaseCreator.class,
  TableCreator.class,
  LoadData.class,
  Utils.class,
  Row.class
})
@PowerMockIgnore({"javax.management.*"})
public class RepositoryTest {
  public static final String DEFAULT_DATABASE_CREATE_SQL = "DEFAULT_DATABASE_CREATE_SQL";
  public static final String DUAL_TABLE_CREATE_SQL = "DUAL_TABLE_CREATE_SQL";
  public static final String LOAD_DUAL_SQL = "LOAD_DUAL_SQL";
  public static final String DATABASE_CREATE_SQL = "DATABASE_CREATE_SQL";
  public static final String TABLE_CREATE_SQL = "TABLE_CREATE_SQL";
  public static final String INSERT_SQL = "INSERT_SQL";
  public static final String SELECT_SQL = "SELECT_SQL";
  private Statement mockStatement;

  private Repository repository;

  @Before
  public void setup() throws Exception {
    LogManager.getLogManager().reset();

    mockStatic(DriverManager.class);
    Connection mockConnection = mock(Connection.class);
    PowerMockito.when(DriverManager.getConnection(anyString(), any(Properties.class)))
      .thenReturn(mockConnection);

    mockStatement = mock(Statement.class);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    mockStatic(DatabaseCreator.class);
    DatabaseCreator mockDatabaseCreator = mock(DatabaseCreator.class, Mockito.RETURNS_DEEP_STUBS);
    PowerMockito.whenNew(DatabaseCreator.class)
      .withArguments(DEFAULT).thenReturn(mockDatabaseCreator);
    when(mockDatabaseCreator.ifNotExist().generate()).thenReturn(DEFAULT_DATABASE_CREATE_SQL);
    when(mockStatement.execute(DEFAULT_DATABASE_CREATE_SQL)).thenReturn(false);


    mockStatic(TableCreator.class);
    TableCreator mockTableCreator = mock(TableCreator.class, Mockito.RETURNS_DEEP_STUBS);
    PowerMockito.whenNew(TableCreator.class)
      .withArguments(DUAL).thenReturn(mockTableCreator);
    when(mockTableCreator.ifNotExist().generate()).thenReturn(DUAL_TABLE_CREATE_SQL);
    when(mockStatement.execute(DUAL_TABLE_CREATE_SQL)).thenReturn(false);


    mockStatic(Utils.class);
    Path dualTempPath = mock(Path.class);
    PowerMockito.when(Utils.writeAtTempFile(DUAL.getName(), "X")).thenReturn(dualTempPath);

    mockStatic(LoadData.class);
    LoadData mockLoadData = mock(LoadData.class, Mockito.RETURNS_DEEP_STUBS);
    PowerMockito.whenNew(LoadData.class)
      .withNoArguments().thenReturn(mockLoadData);
    when(mockLoadData.fromLocal(dualTempPath).into(DUAL).generate()).thenReturn(LOAD_DUAL_SQL);
    when(mockStatement.execute(LOAD_DUAL_SQL)).thenReturn(false);

    repository = Repository.TemporaryRepository();
  }

  @Test
  public void shouldCreateTemporaryRepositoryWithDefaultDatabaseAndDualTable() throws Exception {
    assertThat(repository, instanceOf(Repository.class));

    verify(mockStatement).execute(DEFAULT_DATABASE_CREATE_SQL);
    verify(mockStatement).execute(DUAL_TABLE_CREATE_SQL);
    verify(mockStatement).execute(LOAD_DUAL_SQL);
  }

  @Test
  public void shouldExecuteDatabaseCreateQuery() throws Exception {
    DatabaseCreator mockDatabaseCreator = mock(DatabaseCreator.class, Mockito.RETURNS_DEEP_STUBS);
    when(mockDatabaseCreator.generate()).thenReturn(DATABASE_CREATE_SQL);
    when(mockStatement.execute(DATABASE_CREATE_SQL)).thenReturn(false);

    assertThat(repository.execute(mockDatabaseCreator), is(false));
  }

  @Test
  public void shouldExecuteTableCreateQuery() throws Exception {
    TableCreator mockTableCreator = mock(TableCreator.class);
    when(mockTableCreator.generate()).thenReturn(TABLE_CREATE_SQL);
    when(mockStatement.execute(TABLE_CREATE_SQL)).thenReturn(false);

    assertThat(repository.execute(mockTableCreator), is(false));
  }

  @Test
  public void shouldExecuteInsertQuery() throws Exception {
    InsertQuery mockInsertQuery = mock(InsertQuery.class);
    when(mockInsertQuery.generate()).thenReturn(INSERT_SQL);
    when(mockStatement.execute(INSERT_SQL)).thenReturn(false);

    assertThat(repository.execute(mockInsertQuery), is(false));
  }

  @Test
  public void shouldExecuteSelectQueryAndReturnSomeResultRows() throws Exception {
    SelectQuery mockSelectQuery = mock(SelectQuery.class);
    when(mockSelectQuery.generate()).thenReturn(SELECT_SQL);

    Table mockTable = mock(Table.class);
    when(mockSelectQuery.table()).thenReturn(mockTable);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockStatement.executeQuery(SELECT_SQL)).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);

    mockStatic(Row.class);
    Row mockRow = mock(Row.class);
    whenNew(Row.class).withArguments(mockTable, mockResultSet)
    .thenReturn(mockRow);

    List<Row<Table>> resultRows = repository.execute(mockSelectQuery);

    assertThat(resultRows.size(), is(1));
    assertThat(resultRows.get(0), is(mockRow));
  }
}