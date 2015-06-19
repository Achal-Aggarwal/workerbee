package com.workerbee.dr;

import com.workerbee.Column.Type;

public abstract class SelectFunction {
  protected String alias;
  protected Type type;

  public abstract String generate();

  public SelectFunction as(String alias){
    this.alias = alias;

    return this;
  }

  public String getAlias() {
    return alias;
  }

  public Type getType() {
    return type;
  }
}
