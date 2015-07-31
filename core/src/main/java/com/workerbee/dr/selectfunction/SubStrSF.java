package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

import static com.workerbee.Column.Type.STRING;

public class SubStrSF extends SelectFunction {
  private String columnName;
  private int start;
  private int end;

  public SubStrSF(Column column, int start, int end){
    columnName = column.getFqColumnName();
    alias = column.getName();
    type = STRING;
    this.start = start;
    this.end = end;
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
    return "SUBSTR(" + columnName + ", " + start + ", " + end + ")";
  }
}