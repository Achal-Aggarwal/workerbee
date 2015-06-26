package com.workerbee;

public interface Comparable {
  BooleanExpression eq(Comparable rightComparable);
  BooleanExpression gt(Comparable rightComparable);
  BooleanExpression lt(Comparable rightComparable);
  BooleanExpression gte(Comparable rightComparable);
  BooleanExpression lte(Comparable rightComparable);
  BooleanExpression notEq(Comparable rightComparable);
  String operandName();
}
