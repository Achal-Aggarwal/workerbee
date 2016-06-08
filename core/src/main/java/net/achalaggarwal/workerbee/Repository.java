package net.achalaggarwal.workerbee;

import com.google.common.io.Files;
import net.achalaggarwal.workerbee.ddl.create.DatabaseCreator;
import net.achalaggarwal.workerbee.ddl.create.TableCreator;
import net.achalaggarwal.workerbee.ddl.misc.LoadData;
import net.achalaggarwal.workerbee.ddl.misc.TruncateTable;
import net.achalaggarwal.workerbee.dml.insert.InsertQuery;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import net.achalaggarwal.workerbee.dr.selectfunction.Constant;
import net.achalaggarwal.workerbee.expression.BooleanExpression;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import static net.achalaggarwal.workerbee.Database.DEFAULT;
import static net.achalaggarwal.workerbee.QueryGenerator.*;
import static net.achalaggarwal.workerbee.QueryGenerator.select;
import static net.achalaggarwal.workerbee.Utils.getRandomPositiveNumber;
import static net.achalaggarwal.workerbee.Utils.rtrim;
import static java.lang.String.valueOf;
import static net.achalaggarwal.workerbee.Utils.variableSubstituter;
import static net.achalaggarwal.workerbee.dr.SelectFunctionGenerator.star;
import static net.achalaggarwal.workerbee.expression.BooleanExpression.EQUALS;

public class Repository implements AutoCloseable {
  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String JDBC_HIVE2_EMBEDDED_MODE_URL = "jdbc:hive2://";

  public static final Path ROOT_DIR = Paths.get("/", "tmp", "workerbee", valueOf(getRandomPositiveNumber()));

  private Map<String, String> hiveVarMap = new HashMap<>();

  private static Logger LOGGER = Logger.getLogger(Repository.class.getName());

  private Connection connection;

  public static Repository TemporaryRepository() throws IOException, SQLException {
    return TemporaryRepository(ROOT_DIR);
  }

  public static Repository TemporaryRepository(Path rootDir) throws IOException, SQLException {
    LOGGER.info("Initializing repository at : " + rootDir);
    return new Repository(
      JDBC_HIVE2_EMBEDDED_MODE_URL,
      getHiveConfiguration(rootDir)
    );
  }

  public Repository(String connectionUrl, Properties properties) throws SQLException, IOException {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    LOGGER.info("Connecting to : " + connectionUrl);
    connection = DriverManager.getConnection(connectionUrl, properties);

    execute(new DatabaseCreator(DEFAULT).ifNotExist().generate());
    create(Table.DUAL);
    load(Table.DUAL, new Row<>(Table.DUAL, "X"));
  }

  public Repository create(Table<? extends Table> table) throws SQLException, IOException {
    execute(new TableCreator(table).ifNotExist().generate());
    clear(table);

    return this;
  }

  public <T extends Table>  Repository load(Pair<Table<T>, List<Row<T>>> tableData) throws SQLException, IOException {
    LoadData loadData = new LoadData();

    Table<T> table = tableData.getLeft();

    for (Row<T> row : tableData.getRight()) {
      Path tableDirPath = Utils.writeAtTempFile(table, row);
      execute(loadData.data(row).fromLocal(tableDirPath).into(table).generate());
    }
    return this;
  }

  @SafeVarargs
  public final <T extends Table> Repository load(Table<T> table, Row<T> firstRow, Row<T>... rows) throws SQLException, IOException {
    List<Row<T>> rowList = new ArrayList<>();
    rowList.add(firstRow);
    rowList.addAll(Arrays.asList(rows));
    return load(Pair.of(table, rowList));
  }

  private Repository clear(Table<? extends Table> table) throws SQLException {
    if (table.isExternal()){
      return execute("dfs -rmr " + table.getLocation());
    }

    return execute(new TruncateTable(table).generate());
  }

  public Repository hiveVar(String var, String val) throws SQLException {
    hiveVarMap.put(var, val);
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
        statement.execute(interpolateQuery(sqlStatement));
      }
    }

    return this;
  }

  public ResultSet executeForResult(String query) throws SQLException {
    return connection.createStatement().executeQuery(interpolateQuery(query));
  }

  private String interpolateQuery(String query) {
    LOGGER.info("Executing query [Before interpolation]: " + query);
    String interpolatedStatement = variableSubstituter(query, hiveVarMap);
    LOGGER.info("Executing query [After interpolation]: " + interpolatedStatement);
    return interpolatedStatement;
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

  public <T extends Table> List<Row<T>> unload(Table<T> table, Column... partitions) throws SQLException, IOException {
    File tempDirectoryPath = takeoutRecordsInFile(table, partitions);

    File[] files = tempDirectoryPath.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isHidden();
      }
    });

    List<Row<T>> rows = new ArrayList<>();
    for (File file : files) {
      for (String record : FileUtils.readLines(file)) {
        rows.add(table.parseRecordUsing(record));
      }
    }

    return rows;
  }

  @Deprecated
  public <T extends Table> List<Row<T>> getTextRecordsOf(Table<T> table, Column... partitions) throws SQLException, IOException {
    return unload(table, partitions);
  }

  private <T extends Table> File takeoutRecordsInFile(Table<T> table, Column[] partitions) throws SQLException {
    File tempDirectoryPath = Files.createTempDir();

    BooleanExpression expression = new BooleanExpression(new Constant(1), EQUALS, new Constant(1));
    for (Column partition : partitions) {
      expression = expression.and(new BooleanExpression(partition, EQUALS, new Constant(partition.getValue())));
    }

    execute("USE " + table.getDatabaseName() + "; MSCK REPAIR TABLE " + table.getName());

    execute(insert().overwrite().directory(tempDirectoryPath)
      .using(select(star()).from(table).where(expression)));

    return tempDirectoryPath;
  }

  public <T extends Table, A extends SpecificRecord> List<A> getSpecificRecordsOf(Table<T> table, Column... partitions) throws SQLException, IOException {
    return Row.getSpecificRecords(getTextRecordsOf(table, partitions));
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
