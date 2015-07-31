package com.workerbee.dr;

import com.workerbee.Column.Type;
import com.workerbee.expression.*;
import lombok.Getter;

public abstract class SelectFunction extends com.workerbee.expression.Comparable {
  @Getter
  protected String alias;

  @Getter
  protected Type type;

  public abstract String generate();

  public SelectFunction as(String alias){
    this.alias = alias;

    return this;
  }

  @Override
  public String operandName() {
    return null;
  }
}
