package com.workerbee.dr.selectfunction;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AllStartSFTest {
  @Test
  public void shouldGenerateStar(){
    assertThat(new AllStartSF().generate(), is("*"));
  }
}