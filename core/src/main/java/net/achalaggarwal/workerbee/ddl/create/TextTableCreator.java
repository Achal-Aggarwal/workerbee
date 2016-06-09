package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.TextTable;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.quoteString;

public class TextTableCreator extends TableCreator {
  public TextTableCreator(TextTable<? extends TextTable> table) {
    super(table);
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
}
