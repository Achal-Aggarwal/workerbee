package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

public class ColumnSF extends SelectFunction {
  private Column column;

  public ColumnSF(Column column){
    this.column = column;
    alias = column.getName();
    type = column.getType();
  }

  @Override
  public String generate() {
    if (alias == column.getName()){
      return column.getFqColumnName();
    }

    return column.getFqColumnName() + " AS " + alias;
  }
}
