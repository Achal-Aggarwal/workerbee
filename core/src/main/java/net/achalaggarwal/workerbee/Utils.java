package net.achalaggarwal.workerbee;

import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.lang.String.format;

public class Utils {
  public static String escapeQuote(String string){
    return string.replaceAll("'","''");
  }

  public static String quoteString(String string){
    return "'" + escapeQuote(string) + "'";
  }

  public static String rtrim(String string, String end) {
    if (string.endsWith(end)){
      string = string.substring(0, string.length()-end.length());
    }

    return string;
  }

  public static String rtrim(String string) {
    return rtrim(string, ";");
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
      result.append(format("%s", table.getName()));
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
      if (o == null || o.equals(""))
        continue;
      result.append(o.toString()).append(separator);
    }

    int i = result.lastIndexOf(separator);
    if (i >=0){
      result.delete(i, result.length());
    }

    return result.toString();
  }

  private static Random random = new Random();

  public static int getRandomPositiveNumber() {
    return (random.nextInt() & Integer.MAX_VALUE);
  }

  public static String[] head(String first, String... other){
    Object[] objects = _row(first, other);
    return Arrays.copyOf(objects, objects.length, String[].class);
  }

  public static Object[] _row(Object first, Object... other){
    List<Object> values = Lists.asList(first, other);
    return values.toArray(new Object[values.size()]);
  }

  public static <T extends Table> List<Row<T>> table(T table, String[] head, Object[]... rowValues){
    List<Row<T>> rows = new ArrayList<>();

    for (Object[] rowValue : rowValues) {
      Row<T> newRow = table.getNewRow();
      for (int i = 0; i < head.length; i++) {
        newRow.set(table.getColumn(head[i]), rowValue[i]);
      }

      rows.add(newRow);
    }

    return rows;
  }

  public static String variableSubstituter(String value, Map<String, String> variables){
    for (String var : variables.keySet()) {
      value = value.replaceAll("\\$\\{"+var+"\\}", variables.get(var));
    }
    return value;
  }
}
