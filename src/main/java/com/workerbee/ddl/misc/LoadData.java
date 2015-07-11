package com.workerbee.ddl.misc;

import com.workerbee.Column;
import com.workerbee.Query;
import com.workerbee.Table;
import com.workerbee.Utils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.workerbee.Utils.fqTableName;
import static com.workerbee.Utils.joinList;

public class LoadData implements Query {
  private static final boolean LOCAL = true;
  private static final boolean HDFS = false;

  private boolean pathType;
  private Path filePath;
  private boolean overwrite = false;
  private Table table;

  public LoadData fromLocal(Path filePath) {
    this.filePath = filePath;
    pathType = LOCAL;

    return this;
  }

  public LoadData overwrite() {
    overwrite = true;
    return this;
  }

  public LoadData into(Table table) {
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
    List<Column> partitions = table.getPartitions();

    List<String> columnsDef = new ArrayList<>(partitions.size());

    for (Column column : partitions) {
      columnsDef.add(column.getName());
    }

    result.append(joinList(columnsDef, ", "));
    result.append(" )");
  }
}
