package com.workerbee;

import java.util.Objects;

public class Column extends com.workerbee.expression.Comparable {
  public static enum Type {
    INT {
      @Override
      public Object parseValue(RecordParser recordParser, int index) {
        return recordParser.readInt(index);
      }

      @Override
      public Integer convert(Object value) {
        return value == null ? null : Integer.parseInt(String.valueOf(value));
      }
    },
    FLOAT {
      @Override
      public Object parseValue(RecordParser recordParser, int index) {
        return recordParser.readFloat(index);
      }

      @Override
      public Float convert(Object value) {
        return value == null ? null : Float.valueOf(String.valueOf(value));
      }
    },
    STRING {
      @Override
      public Object parseValue(RecordParser recordParser, int index) {
        return recordParser.readString(index);
      }

      @Override
      public String convert(Object value) {
        return value == null ? null : (String) value;
      }
    };

    public abstract Object parseValue(RecordParser rowParser, int index);

    public abstract Object convert(Object value);
  }
  private final String name;

  private final Type type;
  private final String comment;
  private final Table belongsTo;
  public Column(Table belongsTo, String name, Type type) {
    this(belongsTo, name, type, null);
  }

  public Column(Table belongsTo, String name, Type type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.belongsTo = belongsTo;
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

  public String getFqColumnName(){
    return Utils.fqColumnName(belongsTo, this);
  }

  public Object parseValueUsing(RecordParser recordParser, int index) {
    try{
      return type.parseValue(recordParser, index);
    } catch (NumberFormatException nfe){
      throw new RuntimeException(
        "Couldn't parse value '" + recordParser.readString(index) + "' for "
          + getFqColumnName() + " of type '" + type + "'.");
    }
  }

  public Object convert(Object value) {
    return type.convert(value);
  }

  @Override
  public String operandName() {
    return getFqColumnName();
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Column)){
      return false;
    }

    Column column = (Column) obj;

    return getName().equals(column.getName()) && getType() == column.getType();
  }
}
