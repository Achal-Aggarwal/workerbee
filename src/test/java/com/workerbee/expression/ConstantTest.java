package com.workerbee.expression;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ConstantTest {
  @Test
  public void shouldBeAComparableGiveItsValueWhenOperandNameIsAskedFor(){
    Constant constant = new Constant(1);
    assertThat(constant, instanceOf(com.workerbee.expression.Comparable.class));
    assertThat(constant.operandName(), is("1"));
  }

  @Test
  public void shouldGiveQuotedString(){
    Constant constant = new Constant("1");
    assertThat(constant, instanceOf(com.workerbee.expression.Comparable.class));
    assertThat(constant.operandName(), is("'1'"));
  }
}