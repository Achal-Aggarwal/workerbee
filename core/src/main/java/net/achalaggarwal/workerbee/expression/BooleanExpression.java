package net.achalaggarwal.workerbee.expression;

import java.lang.*;

public class BooleanExpression {
  private static String AND = "AND";
  private static String OR = "OR";

  public static final String EQUALS = "=";
  public static final String NOT_EQUALS = "<>";
  public static final String GREATER_THAN = ">";
  public static final String LESSER_THAN = "<";
  public static final String GREATER_THAN_EQUAL_TO = ">=";
  public static final String LESSER_THAN_EQUAL_TO = "<=";

  private Comparable left, right;
  private String operator;
  private BooleanExpression expression;
  private String expressionType;

  public BooleanExpression(Comparable left, String operator, Comparable right) {
    this.left = left;
    this.right = right;
    this.operator = operator;
  }

  private BooleanExpression(BooleanExpression leftExpression, String expressionType, BooleanExpression rightExpression) {
    this(leftExpression.left, leftExpression.operator, leftExpression.right);
    this.expression = rightExpression;
    this.expressionType = expressionType;
  }

  public String generate(){
    StringBuilder result = new StringBuilder();


    if(expression != null){
      result.append("( " + expression.generate() + " " + expressionType + " ");
    }

    result.append(left.operandName() + " " + operator + " " + right.operandName());

    if(expression != null){
      result.append(" )");
    }

    return result.toString();
  }

  public BooleanExpression and(BooleanExpression booleanExpression){
    return new BooleanExpression(booleanExpression, AND, this);
  }

  public BooleanExpression or(BooleanExpression booleanExpression){
    return new BooleanExpression(booleanExpression, OR, this);
  }
}
