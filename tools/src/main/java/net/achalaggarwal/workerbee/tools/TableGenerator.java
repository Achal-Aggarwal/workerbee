package net.achalaggarwal.workerbee.tools;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Utils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableGenerator {

  public static final Pattern COLUMN_PATTERN = Pattern.compile(
    "(?:\\s*(?:`([\\w\\d_]+)` ([\\w\\d_]+)(\\([\\d,]+\\))?)( COMMENT 'from deserializer')?,?)"
  );
  public static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
    "(?:CREATE(?: EXTERNAL)? TABLE `([\\w\\d_]+)`)"
  );

  private static Logger LOGGER = Logger.getLogger(TableGenerator.class.getName());

  public static String generateTable(String packageName, String databaseName, String schemaStr){
    String[] schemaParts = schemaStr.split("PARTITIONED BY");

    List<Column> columns = extractColumnsAsMap(schemaParts[0]);
    List<Column> partitions =
      schemaParts.length > 1
      ? extractColumnsAsMap(schemaParts[1])
      : new ArrayList<Column>();


    String tableName = extractTableName(schemaStr);

    LOGGER.finest("TableName: " + tableName);
    LOGGER.finest("Columns: " + columns.toString());
    LOGGER.finest("Partitions: " + partitions.toString());

    return tableTemplate(packageName, databaseName, tableName, columns, partitions);
  }

  private static String tableTemplate(
    String packageName, String databaseName, String tableName,
    List<Column> columns,
    List<Column> partitions
  ){
    String className = snakeCaseToFirstUpperCase(tableName) + "Table";
    return
      "package " + packageName + ";\n" +
      "\n" +
      "import net.achalaggarwal.workerbee.Column;\n" +
      "import net.achalaggarwal.workerbee.Table;\n" +
      "\n" +
      "public class " + className + " extends Table<" + className + "> {\n" +
      "  public static final " + className + " tb = new " + className + "();\n" +
      "\n" +
      columnSectionTemplate("HavingColumn", columns) +
      "\n" +
      columnSectionTemplate("PartitionedOnColumn", partitions) +
      "\n" +
      "  private " + className + "() {\n" +
      "    super(" + databaseName + ", \"" + tableName + "\", \"" + className + "\", 1);\n" +
      "  }\n" +
      "}"
      ;
  }

  private static String columnSectionTemplate(String method, List<Column> columns){
    StringBuilder sb = new StringBuilder();

    for (Column colEntry : columns) {
      sb.append("  ")
        .append(columnTemplate(method, colEntry))
        .append("\n");
    }

    return sb.toString();
  }

  private static String columnTemplate(String method, Column column) {
    return "public static final " +
      "Column " + column.getName() + " = "
      + method + "(" +
        "tb, \""
        + column.getName() + "\"" +
        ", Column.Type." + column.getType().name() + ")"
      + (column.getParams() == null ? "" : ".withParams(\"" + column.getParams() + "\")")
      + ";";
  }

  private static String snakeCaseToFirstUpperCase(String snakeCaseStr){
    String[] splits = snakeCaseStr.split("_");

    List<String> splitsList = new ArrayList<>(splits.length);

    for (String split : splits) {
      splitsList.add(split.substring(0,1).toUpperCase() + split.substring(1));
    }

    return Utils.joinList(splitsList, "");
  }

  private static String extractTableName(String schemaStr) {
    Matcher matcher = TABLE_NAME_PATTERN.matcher(schemaStr);
    matcher.find();
    return matcher.group(1);
  }

  private static List<Column> extractColumnsAsMap(String schemaPart) {
    ArrayList<Column> columns = new ArrayList<>();

    Matcher matcher = COLUMN_PATTERN.matcher(schemaPart);
    while(matcher.find()) {
      columns.add(
        new Column(
          null,
          matcher.group(1),
          Column.Type.valueOf(matcher.group(2).toUpperCase())
        ).withParams(matcher.group(3))
      );
    }
    return columns;
  }

  private static void generateJavaFileForTable(String packageName, String databaseName, File outputDir, File inputFile) throws IOException {
    String schemaStr = FileUtils.readFileToString(inputFile);
    FileUtils.writeStringToFile(
      new File(
        outputDir,
        snakeCaseToFirstUpperCase(extractTableName(schemaStr)) + "Table.java"
      ),
      generateTable(packageName, databaseName, schemaStr)
    );
  }

  public static void main(String[] args) throws Exception {

    String inputDir = args[0];
    String packageName = args[1];
    String databaseName = args[2];
    String outputDir = args[3];


    File inputFile = new File(inputDir);
    File outputFile = new File(outputDir);

    if(inputFile.isFile()){
      generateJavaFileForTable(packageName, databaseName, outputFile, inputFile);
    } else {
      for (File file : inputFile.listFiles()) {
        generateJavaFileForTable(packageName, databaseName, outputFile, file);
      }
    }
  }
}