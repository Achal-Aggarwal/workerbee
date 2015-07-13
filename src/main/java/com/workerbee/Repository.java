package com.workerbee;

import com.workerbee.ddl.create.DatabaseCreator;
import com.workerbee.ddl.create.TableCreator;
import com.workerbee.ddl.misc.LoadData;
import com.workerbee.dml.insert.InsertQuery;
import com.workerbee.dr.SelectQuery;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import static com.workerbee.Table.DUAL;

public class Repository implements AutoCloseable {
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static Logger LOGGER = Logger.getLogger(Repository.class.getName());

  private Connection connection;

  public static Repository TemporaryRepository() throws IOException, SQLException {
    return new Repository(
      "jdbc:hive2://",
      getHiveConfiguration(Paths.get("/", "workerbee", String.valueOf(Utils.getRandomPositiveNumber())))
    );
  }

  private Repository(String connectionUrl, Properties properties) throws SQLException, IOException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    LOGGER.info("Connection to : " + connectionUrl);
    connection = DriverManager.getConnection(connectionUrl, properties);

    execute(new DatabaseCreator(Database.DEFAULT).ifNotExist().generate());
    setUp(DUAL, new Row<>(DUAL, "X"));
  }

  public Repository setUp(Table<? extends Table> table, Row... rows) throws SQLException, IOException {
    List<String> records = new ArrayList<>();

    for (Row row : rows) {
      records.add(row.generateRecord());
    }

    execute(new TableCreator(table).ifNotExist().generate());
    Path tableDirPath = Utils.writeAtTempFile(table.getName(), records);
    execute(new LoadData().overwrite().fromLocal(tableDirPath).into(table).generate());

    return this;
  }

  public Repository hiveVar(String var, String val) throws SQLException {
    execute("SET hiveva:" + var + "=" + val);
    return this;
  }

  public boolean execute(DatabaseCreator databaseCreator) throws SQLException {
    return execute(databaseCreator.generate());
  }

  public boolean execute(TableCreator tableCreator) throws SQLException {
    return execute(tableCreator.generate());
  }

  public boolean execute(InsertQuery insertQuery) throws SQLException {
    return execute(insertQuery.generate());
  }

  public boolean execute(LoadData loadDataQuery) throws SQLException {
    return execute(loadDataQuery.generate());
  }

  public boolean execute(File sqlScriptFile) throws IOException, SQLException {
    String sqlScript = FileUtils.readFileToString(sqlScriptFile);
    for (String statment : sqlScript.split(";")) {
      execute(statment);
    }

    return false;
  }

  private boolean execute(String query) throws SQLException {
    Statement statement = connection.createStatement();
    if (query.endsWith(";")){
      query = query.substring(0, query.length()-1);
    }
    LOGGER.info("Executing query : " + query);
    return statement.execute(query);
  }

  public List<Row<Table>> execute(SelectQuery selectQuery) throws SQLException {
    Statement statement = connection.createStatement();

    String selectHQL = selectQuery.generate();
    if (selectHQL.endsWith(";")){
      selectHQL = selectHQL.substring(0, selectHQL.length()-1);
    }
    LOGGER.info("Executing query : " + selectHQL);

    List<Row<Table>> rows = new ArrayList<>();
    ResultSet resultSet = statement.executeQuery(selectHQL);
    while (resultSet.next()) {
      rows.add(new Row<>(selectQuery.table(), resultSet));
    }

    return rows;
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
