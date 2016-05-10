package net.achalaggarwal.workerbee;

import com.google.common.io.Files;
import net.achalaggarwal.workerbee.ddl.create.DatabaseCreator;
import net.achalaggarwal.workerbee.ddl.create.TableCreator;
import net.achalaggarwal.workerbee.ddl.misc.LoadData;
import net.achalaggarwal.workerbee.ddl.misc.TruncateTable;
import net.achalaggarwal.workerbee.dml.insert.InsertQuery;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static net.achalaggarwal.workerbee.Database.DEFAULT;
import static net.achalaggarwal.workerbee.QueryGenerator.*;
import static net.achalaggarwal.workerbee.QueryGenerator.select;
import static net.achalaggarwal.workerbee.Utils.getRandomPositiveNumber;
import static net.achalaggarwal.workerbee.Utils.rtrim;
import static java.lang.String.valueOf;
import static net.achalaggarwal.workerbee.dr.SelectFunctionGenerator.star;

public class Repository implements AutoCloseable {
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String JDBC_HIVE2_EMBEDDED_MODE_URL = "jdbc:hive2://";
  private static final Path ROOT_DIR = Paths.get("/", "tmp", "workerbee", valueOf(getRandomPositiveNumber()));

  private static Logger LOGGER = Logger.getLogger(Repository.class.getName());

  private Connection connection;

  public static Repository TemporaryRepository() throws IOException, SQLException {
    return new Repository(
      JDBC_HIVE2_EMBEDDED_MODE_URL,
      getHiveConfiguration(ROOT_DIR)
    );
  }

  private Repository(String connectionUrl, Properties properties) throws SQLException, IOException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    LOGGER.info("Connecting to : " + connectionUrl);
    connection = DriverManager.getConnection(connectionUrl, properties);

    LOGGER.info("Initializing repository at : " + ROOT_DIR);

    execute(new DatabaseCreator(DEFAULT).ifNotExist().generate());
    setUp(Table.DUAL);
    setUp(Table.DUAL, new Row<>(Table.DUAL, "X"));
  }

  public Repository setUp(Table<? extends Table> table, Row... rows) throws SQLException, IOException {
    if (rows.length < 1) {
      execute(new TableCreator(table).ifNotExist().generate());
      clear(table);

      return this;
    }

    LoadData loadData = new LoadData();

    for (Row row : rows) {
      Path tableDirPath = Utils.writeAtTempFile(table, row);
      execute(loadData.data(row).fromLocal(tableDirPath).into(table).generate());
    }

    return this;
  }

  private Repository clear(Table<? extends Table> table) throws SQLException {
    if (table.isExternal()){
      return execute("DFS -RMR " + table.getLocation());
    }

    return execute(new TruncateTable(table).generate());
  }

  public Repository hiveVar(String var, String val) throws SQLException {
    return execute("SET hivevar:" + var + "=" + val);
  }

  public Repository execute(DatabaseCreator databaseCreator) throws SQLException {
    return execute(databaseCreator.generate());
  }

  public Repository execute(TableCreator tableCreator) throws SQLException {
    return execute(tableCreator.generate());
  }

  public Repository execute(InsertQuery insertQuery) throws SQLException {
    return execute(insertQuery.generate());
  }

  public Repository execute(LoadData loadDataQuery) throws SQLException {
    return execute(loadDataQuery.generate());
  }

  public Repository execute(File sqlScriptFile) throws IOException, SQLException {
    return execute(FileUtils.readFileToString(sqlScriptFile));
  }

  public Repository execute(String query) throws SQLException {
    Statement statement = connection.createStatement();

    for (String sqlStatement : query.split("[\\s]*;[\\s]*")) {
      if (sqlStatement.length() > 0) {
        LOGGER.info("Executing query : " + sqlStatement);
        statement.execute(sqlStatement);
      }
    }

    return this;
  }

  public List<Row<Table>> execute(SelectQuery selectQuery) throws SQLException {
    Statement statement = connection.createStatement();

    String selectHQL = rtrim(selectQuery.generate());
    LOGGER.info("Executing query : " + selectHQL);

    List<Row<Table>> rows = new ArrayList<>();
    ResultSet resultSet = statement.executeQuery(selectHQL);
    while (resultSet.next()) {
      rows.add(new Row<>(selectQuery.table(), resultSet));
    }

    return rows;
  }

  public <T extends Table> List<Row<T>> getTextRecordsOf(Table<T> table) throws SQLException, IOException {
    Statement statement = connection.createStatement();

    File tempDirectoryPath = Files.createTempDir();

    String insertHQL = insert().overwrite().directory(tempDirectoryPath).using(select(star()).from(table)).generate();
    LOGGER.info("Executing query : " + insertHQL);
    statement.execute(insertHQL);

    List<Row<T>> rows = new ArrayList<>();

    File[] files = tempDirectoryPath.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isHidden();
      }
    });

    for (File file : files) {
      for (String record : FileUtils.readLines(file)) {
        rows.add(table.parseRecordUsing(record));
      }
    }

    return rows;
  }

  public <T extends Table, A extends SpecificRecord> List<A> getSpecificRecordsOf(Table<T> table) throws SQLException, IOException {
    return Row.getSpecificRecords(getTextRecordsOf(table));
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }

  private static Properties getHiveConfiguration(Path baseDir) {
    final String basePath = baseDir.toAbsolutePath().toString();
    return new Properties(){{
      setProperty("hiveconf:fs.default.name", "file://" + basePath);
      setProperty("hiveconf:mapred.job.tracker", "local");
      setProperty("hiveconf:mapreduce.framework.name", "local");

      setProperty("hiveconf:hive.exec.scratchdir", basePath + "/scratchdir");
      setProperty("hiveconf:hive.querylog.location", basePath + "/querylog");
      setProperty("hiveconf:hive.metastore.warehouse.dir", basePath + "/warehouse");
      setProperty("hiveconf:hive.metastore.local", "true");

      setProperty("hiveconf:javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + basePath + "/metastore_db;create=true");
      setProperty("hiveconf:javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");

      setProperty("hiveconf:hive.stats.dbclass", "jdbc:derby");
      setProperty("hiveconf:hive.stats.dbconnectionstring", "jdbc:derby:;databaseName=" + basePath + "/TempStatsStore;create=true");
      setProperty("hiveconf:hive.stats.jdbcdriver", "org.apache.derby.jdbc.EmbeddedDriver");

      setProperty("hiveconf:hive.cli.print.header", "false");
      setProperty("hiveconf:hive.metastore.execute.setugi", "true");
      setProperty("hiveconf:hive.exec.dynamic.partition.mode", "nonstrict");
    }};
  }
}
