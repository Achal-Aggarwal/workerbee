package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.Query;
import net.achalaggarwal.workerbee.Table;

import java.util.ArrayList;
import java.util.List;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.joinList;
import static net.achalaggarwal.workerbee.Utils.quoteString;

public class TableCreator implements Query {
  private Table<? extends Table> table;
  private boolean overwrite = true;
  private Database database;

  public TableCreator(Table<? extends Table> table) {
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

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("CREATE");

    if(table.isExternal()){
      result.append(" EXTERNAL");
    }

    result.append(" TABLE");

    if (!overwrite) {
      result.append(" IF NOT EXISTS");
    }

    result.append(" ").append(fqTableName(table, database));

    if (!table.getColumns().isEmpty()){
      columnDefPart(result);
    }

    if (table.getComment() != null){
      result.append(" COMMENT " + quoteString(table.getComment()));
    }

    if (!table.getPartitions().isEmpty()){
      partitionedByPart(result);
    }

    if(table.getLocation() != null){
      result.append(" LOCATION " + quoteString(table.getLocation()));
    }

    if(!table.getProperties().isEmpty()){
      tablePropertiesPart(result);
    }

    result.append(" ;");

    return result.toString();
  }

  private void tablePropertiesPart(StringBuilder result) {
    result.append(" TBLPROPERTIES ( ");
    for (String property : table.getProperties()) {
      result.append(quoteString(property) + " = " + quoteString(table.getProperty(property)) + ", ");
    }
    result.delete(result.lastIndexOf(", "), result.length());
    result.append(" )");
  }

  private void columnDefPart(StringBuilder result) {
    result.append(" ( ");
    List<String> columnsDef = new ArrayList<>(table.getColumns().size());

    for (Column column : table.getColumns()) {
      columnsDef.add(column.getName() + " " + column.getTypeRepresentation());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }

  private void partitionedByPart(StringBuilder result) {
    result.append(" PARTITIONED BY ( ");
    List<String> columnsDef = new ArrayList<>(table.getPartitions().size());

    for (Column column : table.getPartitions()) {
      columnsDef.add(column.getName() + " " + column.getType());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
