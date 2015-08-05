package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.dr.SelectFunction;

public class SubStrSF extends SelectFunction {
  private String columnName;
  private int start;
  private int end;

  public SubStrSF(Column column, int start, int end){
    columnName = column.getFqColumnName();
    alias = column.getName();
    type = Column.Type.STRING;
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