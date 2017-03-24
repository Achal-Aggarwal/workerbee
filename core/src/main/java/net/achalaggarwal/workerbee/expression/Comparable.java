package net.achalaggarwal.workerbee.expression;

import net.achalaggarwal.workerbee.dr.selectfunction.NULL;

public abstract class Comparable {

  public BooleanExpression eq(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.EQUALS, rightComparable);
  }

  public BooleanExpression notEq(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.NOT_EQUALS, rightComparable);
  }

  public BooleanExpression gt(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.GREATER_THAN, rightComparable);
  }

  public BooleanExpression lt(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.LESSER_THAN, rightComparable);
  }

  public BooleanExpression gte(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.GREATER_THAN_EQUAL_TO, rightComparable);
  }

  public BooleanExpression isNULL() {
    return new BooleanExpression(this, "IS", NULL.i);
  }

  public BooleanExpression isNotNULL() {
    return new BooleanExpression(this, "IS NOT", NULL.i);
  }

  public abstract String operandName();
}
