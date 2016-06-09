package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;

import java.util.ArrayList;
import java.util.List;

import static net.achalaggarwal.workerbee.Utils.joinList;
import static net.achalaggarwal.workerbee.Utils.quoteString;

abstract public class TableCreator implements Query {
  protected Table table;
  protected boolean overwrite = true;
  protected Database database;

  public TableCreator(Table table) {
    this.table = table;
  }

  public TableCreator ifNotExist(){
    overwrite = false;
    return this;
  }

  public TableCreator inDatabase(Database database) {
    this.database = database;

    return this;
  }

  protected void tablePropertiesPart(StringBuilder result) {
    result.append(" TBLPROPERTIES ( ");
    for (String property : table.getProperties()) {
      result.append(quoteString(property) + " = " + quoteString(table.getProperty(property)) + ", ");
    }
    result.delete(result.lastIndexOf(", "), result.length());
    result.append(" )");
  }

  protected void columnDefPart(StringBuilder result) {
    result.append(" ( ");
    List<String> columnsDef = new ArrayList<>(table.getColumns().size());

    for (Column column : table.getColumns()) {
      columnsDef.add(column.getName() + " " + column.getTypeRepresentation());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }

  protected void partitionedByPart(StringBuilder result) {
    result.append(" PARTITIONED BY ( ");
    List<String> columnsDef = new ArrayList<>(table.getPartitions().size());

    for (Column column : table.getPartitions()) {
      columnsDef.add(column.getName() + " " + column.getType());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
