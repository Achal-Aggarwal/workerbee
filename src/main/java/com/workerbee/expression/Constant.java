package com.workerbee.expression;

import com.workerbee.*;

public class Constant extends Comparable {
  private Object value;

  public Constant(Object value) {
    this.value = value;
  }

  @Override
  public String operandName() {
    if (value instanceof String){
      return Utils.quoteString(String.valueOf(value));
    }

    return String.valueOf(value);
  }
}
