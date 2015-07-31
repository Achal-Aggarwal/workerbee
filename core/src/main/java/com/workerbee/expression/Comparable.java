package com.workerbee.expression;

import static com.workerbee.expression.BooleanExpression.*;

public abstract class Comparable {

  public BooleanExpression eq(Comparable rightComparable) {
    return new BooleanExpression(this, EQUALS, rightComparable);
  }

  public BooleanExpression notEq(Comparable rightComparable) {
    return new BooleanExpression(this, NOT_EQUALS, rightComparable);
  }

  public BooleanExpression gt(Comparable rightComparable) {
    return new BooleanExpression(this, GREATER_THAN, rightComparable);
  }

  public BooleanExpression lt(Comparable rightComparable) {
    return new BooleanExpression(this, LESSER_THAN, rightComparable);
  }

  public BooleanExpression gte(Comparable rightComparable) {
    return new BooleanExpression(this, GREATER_THAN_EQUAL_TO, rightComparable);
  }

  public BooleanExpression lte(Comparable rightComparable) {
    return new BooleanExpression(this, LESSER_THAN_EQUAL_TO, rightComparable);
  }

  public abstract String operandName();
}
