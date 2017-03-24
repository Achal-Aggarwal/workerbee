package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.dr.SelectFunction;

public final class NULL extends SelectFunction {
  public static NULL i = new NULL();

  private NULL(){}

  @Override
  public String generate() {
    return "NULL";
  }

  @Override
  public String operandName() {
    return generate();
  }
}