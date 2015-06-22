package com.workerbee;

public class Column {
  public static enum Type {
    INT {
      @Override
      public Object parseValue(RecordParser recordParser, int index) {
        return recordParser.readInt(index);
      }
    },
    STRING {
      @Override
      public Object parseValue(RecordParser recordParser, int index) {
        return recordParser.readString(index);
      }
    };

    public abstract Object parseValue(RecordParser rowParser, int index);
  }

  private String name;
  private Type type;
  private String comment;

  public Column(String name, Type type) {
    this(name, type, null);
  }
  public Column(String name, Type type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public Object parseValueUsing(RecordParser recordParser, int index) {
    return type.parseValue(recordParser, index);
  }
}
