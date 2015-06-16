package com.workerbee;

public class Column {
  public static enum Type {
    TINYINT,
    SMALLINT,
    STRING
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
}
