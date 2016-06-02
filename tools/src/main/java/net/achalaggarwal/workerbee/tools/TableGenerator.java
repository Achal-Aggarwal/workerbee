package net.achalaggarwal.workerbee.tools;

import net.achalaggarwal.workerbee.Utils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableGenerator {

  public static final Pattern COLUMN_PATTERN = Pattern.compile(
    "(?:\\s*(?:`([a-zA-Z]+)` ([\\w\\d_]+))( COMMENT 'from deserializer')?,?)"
  );
  public static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
    "(?:CREATE(?: EXTERNAL)? TABLE `([\\w\\d_]+)`)"
  );

  private static Logger LOGGER = Logger.getLogger(TableGenerator.class.getName());

  public static String generateTable(String packageName, String databaseName, String schemaStr){
    String[] schemaParts = schemaStr.split("PARTITIONED BY");

    LinkedHashMap<String, String> columns = extractColumnsAsMap(schemaParts[0]);
    LinkedHashMap<String, String> partitions =
      schemaParts.length > 1
      ? extractColumnsAsMap(schemaParts[1])
      : new LinkedHashMap<String, String>();


    String tableName = extractTableName(schemaStr);

    LOGGER.finest("TableName: " + tableName);
    LOGGER.finest("Columns: " + columns.toString());
    LOGGER.finest("Partitions: " + partitions.toString());

    return tableTemplate(packageName, databaseName, tableName, columns, partitions);
  }

  private static String tableTemplate(
    String packageName, String databaseName, String tableName,
    LinkedHashMap<String, String> columns,
    LinkedHashMap<String, String> partitions
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

  private static String columnSectionTemplate(String method, LinkedHashMap<String, String> columns){
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, String> colEntry : columns.entrySet()) {
      sb.append("  ")
        .append(columnTemplate(method, colEntry.getKey(), colEntry.getValue()))
        .append("\n");
    }

    return sb.toString();
  }

  private static String columnTemplate(String method, String name, String type) {
    return "public static final " +
      "Column " + name + " = " + method + "(" +
      "tb, \"" + name + "\", " + getColumnType(type) +
      ");";
  }

  private static String getColumnType(String type){
    switch (type.toLowerCase()){
      case "string":
        return "Column.Type.STRING";
      case "int":
        return "Column.Type.INT";
      case "float":
        return "Column.Type.FLOAT";
      case "double":
        return "Column.Type.DOUBLE";
      case "boolean":
        return "Column.Type.BOOLEAN";
      case "timestamp":
        return "Column.Type.TIMESTAMP";
    }

    return null;
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

  private static LinkedHashMap<String, String> extractColumnsAsMap(String schemaPart) {
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();

    Matcher matcher = COLUMN_PATTERN.matcher(schemaPart);
    while(matcher.find()) {
        columns.put(matcher.group(1), matcher.group(2));
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