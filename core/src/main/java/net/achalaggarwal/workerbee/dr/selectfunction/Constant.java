package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.Utils;
import net.achalaggarwal.workerbee.dr.SelectFunction;

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
