package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

public class ColumnSF extends SelectFunction {
  private String columnName;
  public ColumnSF(Column column){
    columnName = column.getName();
    type = column.getType();
  }

  @Override
  public String generate() {
    if (alias == null){
      return columnName;
    }

    return columnName + " AS " + alias;
  }
}
