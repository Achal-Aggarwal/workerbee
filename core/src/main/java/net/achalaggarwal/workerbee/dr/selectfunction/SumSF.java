package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.dr.SelectFunction;

public class SumSF extends SelectFunction {
  private String columnName;

  public SumSF(Column column){
    columnName = column.getFqColumnName();
    alias = column.getName();
    type = Column.Type.INT;
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
    return "SUM(" + columnName + ")";
  }
}
