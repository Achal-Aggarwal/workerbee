package com.workerbee.dr;

import com.workerbee.Column.Type;
import lombok.Getter;

public abstract class SelectFunction {
  @Getter
  protected String alias;

  @Getter
  protected Type type;

  public abstract String generate();

  public SelectFunction as(String alias){
    this.alias = alias;

    return this;
  }
}
