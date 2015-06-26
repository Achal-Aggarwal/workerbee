package com.workerbee;

public interface Operand {
  Expression eq(Operand rightOperand);
  String operandName();
}
