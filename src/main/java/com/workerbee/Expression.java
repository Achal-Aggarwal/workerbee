package com.workerbee;

public class Expression {
  private Operand left, right;
  private String operator;

  public Expression(Operand left, String operator, Operand right) {
    this.left = left;
    this.right = right;
    this.operator = operator;
  }

  public String generate(){
    return left.operandName() + " " + operator + " " + right.operandName();
  }
}
