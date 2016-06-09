package net.achalaggarwal.workerbee.ddl.create;

import net.achalaggarwal.workerbee.AvroTable;
import net.achalaggarwal.workerbee.Table;

import static net.achalaggarwal.workerbee.Utils.fqTableName;
import static net.achalaggarwal.workerbee.Utils.quoteString;

public class AvroTableCreator extends TableCreator {
  public AvroTableCreator(AvroTable<? extends Table> table) {
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

    if (table.getComment() != null){
      result.append(" COMMENT " + quoteString(table.getComment()));
    }

    if (!table.getPartitions().isEmpty()){
      partitionedByPart(result);
    }

    result
      .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'")
      .append("STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'")
      .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'");

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
