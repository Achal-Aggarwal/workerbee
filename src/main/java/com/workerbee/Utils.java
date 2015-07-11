package com.workerbee;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Logger;

import static java.lang.String.format;

public class Utils {
  private static final String SILENT = "--silent";
  private static final String VERBOSE = "--verbose";
  private static final String QUERY_FILENAME = "-f";
  private static final String HIVE_CLI_BIN_NAME = "hive";
  private static final String HIVECONF = "-hiveconf";

  public static String escapeQuote(String string){
    return string.replaceAll("'","''");
  }

  public static String quoteString(String string){
    return "'" + escapeQuote(string) + "'";
  }

  public static String fqTableName(Table table){
    return fqTableName(table, null);
  }

  public static String fqTableName(Table table, Database database){
    StringBuilder result = new StringBuilder();

    if (database != null) {
      result.append(format("%s.%s", database.getName(), table.getName()));
    } else if (table.isNotTemporary()){
      result.append(format("%s.%s", table.getDatabaseName(), table.getName()));
    } else {
      result.append(format("%s",table.getName()));
    }

    return result.toString();
  }

  public static String fqColumnName(Table table, Column column){
    StringBuilder result = new StringBuilder();

    if (table != null){
      result
        .append(table.getName())
        .append(".");
    }

    result.append(column.getName());

    return result.toString();
  }

  public static String joinList(List list, String separator){
    StringBuilder result = new StringBuilder();

    for (Object o : list) {
      if (o == null)
        continue;
      result.append(o.toString() + separator);
    }

    result.delete(result.lastIndexOf(separator), result.length());

    return result.toString();
  }

  public static int execHiveCLI(
    final Path queryFile, final Map<String, String> hiveConf, Logger LOGGER
  )
    throws IOException, InterruptedException
  {
    List<String> commands = makeHiveCLICommand(
      new HashMap<String, String>() {{
        put(QUERY_FILENAME, queryFile.toAbsolutePath().toString());
      }}, hiveConf
    );

    return execCommand(commands, LOGGER
    );
  }

  private static List<String> makeHiveCLICommand(Map<String, String> config, final Map<String, String> hiveConf){
    List<String> result = new ArrayList<>();

    result.add(HIVE_CLI_BIN_NAME);

    for (String hiveVar : hiveConf.keySet()) {
      result.add(HIVECONF);
      result.add(hiveVar + "=" + hiveConf.get(hiveVar));
    }

    result.add(getConfig(config, QUERY_FILENAME, false));

    result.add(getConfig(config, SILENT, true));
    result.add(getConfig(config, VERBOSE, true));

    return result;
  }

  private static String getConfig(Map<String, String> config, final String property, boolean isBoolean){
    if (config.containsKey(property)){
      return isBoolean ? property : property + config.get(property);
    }

    return "";
  }

  private static int execCommand(List<String> commands, Logger LOGGER) throws InterruptedException, IOException {
    String command = joinList(commands, " ");
    LOGGER.info("Running command : " + command);

    Process p = new ProcessBuilder(commands).start();
    p.waitFor();

    BufferedReader reader =
      new BufferedReader(new InputStreamReader(p.getErrorStream()));

    String line = reader.readLine();
    while (line != null) {
      LOGGER.info(line);
      line = reader.readLine();
    }

    return p.exitValue();
  }

  private static Random random = new Random();

  public static int getRandomPositiveNumber() {
    return (random.nextInt() & Integer.MAX_VALUE);
  }
}
