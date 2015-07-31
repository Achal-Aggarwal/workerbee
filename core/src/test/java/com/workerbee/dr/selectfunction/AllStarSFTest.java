package com.workerbee.dr.selectfunction;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AllStarSFTest {
  @Test
  public void shouldGenerateStar(){
    assertThat(new AllStarSF().generate(), is("*"));
  }
}