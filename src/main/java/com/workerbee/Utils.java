package com.workerbee;

import java.util.List;

public class Utils {
  public static String escapeQuote(String string){
    return string.replaceAll("'","''");
  }

  public static String quoteString(String string){
    return "'" + escapeQuote(string) + "'";
  }

  public static String fqTableName(Table table){
    StringBuilder result = new StringBuilder();

    if (table.isNotTemporary()){
      result.append(table.getDatabaseName()).append(".");
    }

    result.append(table.getName());

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
      result.append(o.toString() + separator);
    }

    result.delete(result.lastIndexOf(separator), result.length());

    return result.toString();
  }
}
