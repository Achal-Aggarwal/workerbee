package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

import static com.workerbee.Column.Type.STRING;

public class SubStrSF extends SelectFunction {
  private String columnName;
  private int start;
  private int end;

  public SubStrSF(Column column, int start, int end){
    columnName = column.getName();
    type = STRING;
    this.start = start;
    this.end = end;
  }

  @Override
  public String generate() {
    String result = "SUBSTR(" + columnName + ", " + start + ", " + end + ")";

    if (alias == null){
      return result;
    }

    return result + " AS " + alias;
  }
}