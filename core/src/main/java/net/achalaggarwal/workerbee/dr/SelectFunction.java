package net.achalaggarwal.workerbee.dr;

import lombok.Getter;
import net.achalaggarwal.workerbee.Column;

public abstract class SelectFunction extends net.achalaggarwal.workerbee.expression.Comparable {
  @Getter
  protected String alias;

  @Getter
  protected Column.Type type;

  public abstract String generate();

  public SelectFunction as(String alias){
    this.alias = alias;

    return this;
  }

  @Override
  public String operandName() {
    return null;
  }
}
