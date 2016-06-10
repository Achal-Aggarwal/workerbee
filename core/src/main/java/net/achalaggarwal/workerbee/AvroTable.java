package net.achalaggarwal.workerbee;

import net.achalaggarwal.workerbee.ddl.create.AvroTableCreator;
import net.achalaggarwal.workerbee.ddl.create.TableCreator;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import static java.lang.String.format;

public class AvroTable<T extends AvroTable> extends Table {
  public static final String AVRO_SCHEMA_URL_PATH = "avro.schema.url.path";
  private Class<? extends SpecificRecord> klass;

  public AvroTable(String name) {
    this(null, name, null, 0);
  }

  public AvroTable(String name, long version) {
    this(null, name, null, version);
  }

  public AvroTable(Database database, String name) {
    this(database, name, null, 0);
  }

  public AvroTable(Database database, String name, long version) {
    this(database, name, null, version);
  }

  public AvroTable(Database database, String name, String comment) {
    this(database, name, comment, 0);
  }

  public AvroTable(Database database, String name, String comment, long version) {
    super(database, name, comment, version);
    havingProperty(
      "avro.schema.url",
      getSchemaPath("${" + AVRO_SCHEMA_URL_PATH + "}")
    );
  }

  public String getSchemaPath(String avroSchemaPath) {
    return format("%s/%s-%s.avsc", avroSchemaPath, getDatabaseName(), getName());
  }

  public AvroTable<T> readSchema(Class<? extends SpecificRecord> klass){
    this.klass = klass;

    for (Schema.Field field : getSchema().getFields()) {
      havingColumn(new Column(this, field.name(), Column.getType(field.schema())));
    }

    return this;
  }

  public Schema getSchema() {
    return createSpecificRecord(klass).getSchema();
  }

  private SpecificRecord createSpecificRecord(Class<? extends SpecificRecord> klass) {
    SpecificRecord specificRecord = null;
    try {
      specificRecord = klass.newInstance();
    } catch (Exception e) {
      new RuntimeException(e);
    }
    return specificRecord;
  }

  public Class<? extends SpecificRecord> getKlass() {
    return klass;
  }

  @Override
  public TableCreator create() {
    return new AvroTableCreator(this);
  }
}
