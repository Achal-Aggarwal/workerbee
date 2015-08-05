package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.expression.Comparable;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConstantTest {
  @Test
  public void shouldBeAComparableGiveItsValueWhenOperandNameIsAskedFor(){
    Constant constant = new Constant(1);
    assertThat(constant, instanceOf(net.achalaggarwal.workerbee.expression.Comparable.class));
    assertThat(constant.operandName(), is("1"));
  }

  @Test
  public void shouldGiveQuotedString(){
    Constant constant = new Constant("1");
    assertThat(constant, instanceOf(Comparable.class));
    assertThat(constant.operandName(), is("'1'"));
  }
}