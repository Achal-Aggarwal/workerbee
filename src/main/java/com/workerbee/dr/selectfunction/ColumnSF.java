package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

public class ColumnSF extends SelectFunction {
  private String name;
  public ColumnSF(Column column){
    name = column.getName();
    type = column.getType();
  }

  @Override
  public String generate() {
    if (alias == null){
      return name;
    }

    return name + " AS " + alias;
  }
}
