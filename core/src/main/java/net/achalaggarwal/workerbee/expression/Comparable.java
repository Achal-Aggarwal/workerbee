package net.achalaggarwal.workerbee.expression;

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

  public BooleanExpression lte(Comparable rightComparable) {
    return new BooleanExpression(this, BooleanExpression.LESSER_THAN_EQUAL_TO, rightComparable);
  }

  public abstract String operandName();
}
