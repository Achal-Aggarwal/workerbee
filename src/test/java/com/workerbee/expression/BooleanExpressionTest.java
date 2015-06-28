package com.workerbee.expression;

import org.junit.Before;
import org.junit.Test;

import static com.workerbee.expression.BooleanExpression.EQUALS;
import static com.workerbee.expression.BooleanExpression.NOT_EQUALS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BooleanExpressionTest {

  public static final String LEFT_OPERAND = "LEFT_OPERAND";
  public static final String RIGHT_OPERAND = "RIGHT_OPERAND";

  private BooleanExpression booleanExpression;
  private BooleanExpression anotherBooleanExpression;

  @Before
  public void setup(){
    Comparable leftOperand = mock(Comparable.class);
    when(leftOperand.operandName()).thenReturn(LEFT_OPERAND);
    Comparable rightOperand = mock(Comparable.class);
    when(rightOperand.operandName()).thenReturn(RIGHT_OPERAND);
    booleanExpression = new BooleanExpression(leftOperand, EQUALS, rightOperand);
    anotherBooleanExpression = new BooleanExpression(rightOperand, NOT_EQUALS, leftOperand);
  }

  @Test
  public void shouldGenerateBooleanExpression(){
    assertThat(booleanExpression.generate(), is(LEFT_OPERAND + " = " + RIGHT_OPERAND));
  }

  @Test
  public void shouldGenerateBooleanExpressionWithConnectorAnd(){
    assertThat(booleanExpression.and(anotherBooleanExpression).generate(),
      is("( " + LEFT_OPERAND + " = " + RIGHT_OPERAND + " AND " + RIGHT_OPERAND + " <> " + LEFT_OPERAND + " )"));
  }

  @Test
  public void shouldGenerateBooleanExpressionWithConnectorOR(){
    assertThat(anotherBooleanExpression.or(booleanExpression).generate(),
      is("( " + RIGHT_OPERAND + " <> " + LEFT_OPERAND + " OR " + LEFT_OPERAND + " = " + RIGHT_OPERAND + " )"));
  }
}