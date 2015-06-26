package com.workerbee;

public class BooleanExpression {
  private Comparable left, right;
  private String operator;

  public BooleanExpression(Comparable left, String operator, Comparable right) {
    this.left = left;
    this.right = right;
    this.operator = operator;
  }

  public String generate(){
    return left.operandName() + " " + operator + " " + right.operandName();
  }
}
