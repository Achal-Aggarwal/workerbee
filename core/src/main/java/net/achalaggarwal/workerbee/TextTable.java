package net.achalaggarwal.workerbee;

import net.achalaggarwal.workerbee.ddl.create.TableCreator;
import net.achalaggarwal.workerbee.ddl.create.TextTableCreator;

import static net.achalaggarwal.workerbee.Column.Type.STRING;

public class TextTable<T extends TextTable> extends Table {
  public static class Dual extends TextTable<Dual> {
    public static Dual tb = new Dual();

    private Dual() {
      super(Database.DEFAULT, "Dual");
      havingColumn("dummy", STRING);
    }
  }

  public TextTable(String name) {
    super(name);
  }

  public TextTable(String name, long version) {
    super(name, version);
  }

  public TextTable(Database database, String name) {
    super(database, name);
  }

  public TextTable(Database database, String name, long version) {
    super(database, name, version);
  }

  public TextTable(Database database, String name, String comment) {
    super(database, name, comment);
  }

  public TextTable(Database database, String name, String comment, long version) {
    super(database, name, comment, version);
  }

  public T havingColumn(Column column){
    super.havingColumn(column);

    return (T) this;
  }

  public static Column HavingColumn(TextTable table, String name, Column.Type type) {
    Column column = new Column(table, name, type);
    table.havingColumn(column);
    return column;
  }

  public T havingColumn(String name, Column.Type type, String comment){
    return havingColumn(new Column(this, name, type, comment));
  }

  public T havingColumn(String name, Column.Type type){
    return havingColumn(name, type, null);
  }

  public String generateRecordFor(Row<T> row) {
    return RowUtils.generateRecordFor(this, row);
  }

  @Override
  public TableCreator create() {
    return new TextTableCreator(this);
  }
}
