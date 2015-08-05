package net.achalaggarwal.workerbee.ddl.misc;

import net.achalaggarwal.workerbee.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.joinList;

public class LoadData implements Query {
  private static final boolean LOCAL = true;

  private boolean pathType;
  private Path filePath;
  private boolean overwrite = false;
  private Table<? extends Table> table;
  private Row<? extends Table> row;

  public LoadData data(Row<? extends Table> row) {
    this.row = row;
    return this;
  }

  public LoadData fromLocal(Path filePath) {
    this.filePath = filePath;
    pathType = LOCAL;

    return this;
  }

  public LoadData overwrite() {
    overwrite = true;
    return this;
  }

  public LoadData into(Table<? extends Table> table) {
    this.table = table;
    return this;
  }

  @Override
  public String generate() {
    StringBuilder result = new StringBuilder();

    result.append("LOAD DATA");

    result.append(pathType ? " LOCAL INPATH " : "INPATH ");

    result.append(Utils.quoteString(filePath.toAbsolutePath().toString()));

    if (overwrite) {
      result.append(" OVERWRITE");
    }

    result.append(" INTO TABLE ");

    result.append(fqTableName(table));

    if (!table.getPartitions().isEmpty()){
      partitionedByPart(result);
    }

    result.append(" ;");

    return result.toString();
  }

  private void partitionedByPart(StringBuilder result) {
    result.append(" PARTITION ( ");

    List<String> columnsDef = new ArrayList<>(table.getPartitions().size());

    for (Column column : table.getPartitions()) {
      String def = column.getName();
      Object value = row.get(column);

      if (value instanceof String){
        def += " = " + Utils.quoteString((String) row.get(column));
      } else if(value != null) {
        def += " = " + row.get(column);
      }

      columnsDef.add(def);
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
