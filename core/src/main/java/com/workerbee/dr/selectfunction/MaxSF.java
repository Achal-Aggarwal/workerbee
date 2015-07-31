package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

import static com.workerbee.Column.Type.INT;

public class MaxSF extends SelectFunction {
  private String columnName;

  public MaxSF(Column column){
    columnName = column.getFqColumnName();
    alias = column.getName();
    type = INT;
  }

  @Override
  public String generate() {
    if (alias == null){
      return operandName();
    }

    return operandName() + " AS " + alias;
  }

  @Override
  public String operandName() {
    return "MAX(" + columnName + ")";
  }
}
