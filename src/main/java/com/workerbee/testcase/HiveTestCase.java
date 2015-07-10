package com.workerbee.testcase;

import com.workerbee.Database;
import com.workerbee.Row;
import com.workerbee.Table;
import com.workerbee.Utils;
import com.workerbee.dr.SelectQuery;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

import static com.workerbee.QueryGenerator.*;

public class HiveTestCase {
  public static final String DATABASE_NAME = "WorkerBee";
  public static final String OUTPUT_TABLE_NAME = "OutputTable";
  public static final String SCRIPT_FILE_NAME = "QueryStatements.hql";
  public static final String WORKERBEE_DIR = "/tmp/workerbee";

  public Logger LOGGER = Logger.getLogger(HiveTestCase.class.getName());

  private Database workerBeeDb = new Database(DATABASE_NAME);
  private Map<Table, List<Row>> tables = new HashMap<>();

  private File testFilesDir;
  private File tempTableDataDir;

  public HiveTestCase(String testCaseName) throws IOException {
    testFilesDir = Paths.get(
        WORKERBEE_DIR, testCaseName, String.valueOf(Utils.getRandomPositiveNumber())
      )
      .toFile();

    testFilesDir.mkdirs();

    tempTableDataDir = new File(testFilesDir.getAbsolutePath(), OUTPUT_TABLE_NAME);
    tempTableDataDir.mkdir();
  }

  public HiveTestCase setUp(Table table, Row... rows) {
    if(!tables.containsKey(table)) {
      this.tables.put(table, new ArrayList<Row>());
    }

    Collections.addAll(tables.get(table), rows);
    return this;
  }

  public List<Row> execute(SelectQuery selectQuery) throws IOException, InterruptedException {
    List<String> sqlStatements = new ArrayList<>();

    Table tempTable = selectQuery.as(OUTPUT_TABLE_NAME)
      .table(workerBeeDb).external().onLocation(tempTableDataDir.toPath());

    sqlStatements.add(drop(workerBeeDb).ifExist().cascade().generate());
    sqlStatements.add(drop(tempTable).ifExist().generate());
    sqlStatements.add(create(workerBeeDb).ifNotExist().generate());
    sqlStatements.add(create(tempTable).ifNotExist().generate());

    sqlStatements.addAll(getDDLStatements());
    sqlStatements.addAll(writeDataAndGetLoadDataStatements());

    sqlStatements.add(insert().overwrite().intoTable(tempTable).using(selectQuery).generate());

    LOGGER.info("Testing query : " + selectQuery.generate());

    Path queryScriptPath = new File(testFilesDir.getAbsolutePath(), SCRIPT_FILE_NAME).toPath();
    Files.write(queryScriptPath, sqlStatements, Charset.defaultCharset());

    Utils.execHiveCLI(queryScriptPath, getHiveConfiguration(), LOGGER);

    return getRowsOfTable(tempTable, tempTableDataDir.toPath());
  }

  private List<Row> getRowsOfTable(Table table, Path dataDir) {
    List<Row> rows = new ArrayList<>();

    FileFilter onlyVisibleFiles = new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isFile() && !file.isHidden();
      }
    };

    for (File file : dataDir.toFile().listFiles(onlyVisibleFiles)) {
        try (FileReader fileReader = new FileReader(file)) {
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          String line;
          while ((line = bufferedReader.readLine()) != null) {
            rows.add(table.parseRecordUsing(line));
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
    }

    return rows;
  }

  private List<String> getDDLStatements() {
    List<String> statements = new ArrayList<>(tables.keySet().size());

    for (Table table : tables.keySet()) {
      statements.add(drop(table.getDatabase()).ifExist().cascade().generate());
      statements.add(create(table.getDatabase()).ifNotExist().generate());
      statements.add(drop(table).ifExist().generate());
      statements.add(create(table).generate());
    }

    return statements;
  }


  private List<String> writeDataAndGetLoadDataStatements() throws IOException {
    List<String> loadDataStatements = new ArrayList<>(tables.keySet().size());

    for (Table table : tables.keySet()) {
      Path tableDataFilePath = writeDataForTable(table);
      if (tableDataFilePath != null){
        tableDataFilePath = tableDataFilePath.toAbsolutePath();

        LOGGER.info("Writing data for table " + table.getName() + " at " + tableDataFilePath.toString());

        loadDataStatements.add(
          loadData().fromLocal(tableDataFilePath).overwrite().into(table).generate()
        );
      }
    }

    return loadDataStatements;
  }

  private Path writeDataForTable(Table table) throws IOException {

    List<Row> rows = tables.get(table);

    if (rows.size() < 1) {
      return null;
    }

    LOGGER.info("Data for table " + table.getName());

    List<String> records = new ArrayList<>(rows.size());

    for (Row row : rows) {
      String record = row.generateRecord();
      LOGGER.info(record);
      records.add(record);
    }

    File tableDataDir = new File(testFilesDir.getAbsolutePath(), table.getName());
    tableDataDir.mkdir();

    Files.write(new File(tableDataDir, "00000-00").toPath(), records, Charset.defaultCharset());

    return tableDataDir.toPath();
  }

  private HashMap<String, String> getHiveConfiguration() {
    return new HashMap<String, String>(){{
      put("fs.default.name", "file://" + WORKERBEE_DIR);
      put("mapred.job.tracker", "local");
      put("mapreduce.framework.name", "local");

      put("hive.exec.scratchdir", WORKERBEE_DIR + "/scratchdir");
      put("hive.querylog.location", WORKERBEE_DIR + "/querylog");
      put("hive.metastore.warehouse.dir", WORKERBEE_DIR + "/warehouse");
      put("hive.metastore.local", "true");

      put("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + WORKERBEE_DIR + "/metastore_db;create=true");
      put("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");

      put("hive.stats.dbclass", "jdbc:derby");
      put("hive.stats.dbconnectionstring", "jdbc:derby:;databaseName=" + WORKERBEE_DIR + "/TempStatsStore;create=true");
      put("hive.stats.jdbcdriver", "org.apache.derby.jdbc.EmbeddedDriver");

      put("hive.cli.print.header", "false");
      put("hive.metastore.execute.setugi", "true");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
    }};
  }
}
