package com.workerbee.dr.selectfunction;

import com.workerbee.Utils;
import com.workerbee.dr.SelectFunction;

public class Constant extends SelectFunction {
  private Object value;

  public Constant(Object value) {
    this.value = value;
  }

  @Override
  public String generate() {
    if (value instanceof String){
      return Utils.quoteString(String.valueOf(value));
    }

    return String.valueOf(value);
  }

  @Override
  public String operandName() {
    return generate();
  }
}
