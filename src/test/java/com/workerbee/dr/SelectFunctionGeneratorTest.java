package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.dr.selectfunction.AllStartSF;
import com.workerbee.dr.selectfunction.SubStrSF;
import org.junit.Test;

import static com.workerbee.Column.Type.STRING;
import static com.workerbee.dr.SelectFunctionGenerator.star;
import static com.workerbee.dr.SelectFunctionGenerator.substr;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class SelectFunctionGeneratorTest {
  @Test
  public void shouldReturnAllStarSFWhenStarIsUsed(){
    assertThat(star(), instanceOf(AllStartSF.class));
  }

  @Test
  public void shouldReturnSunStrSFWhenSubstrIsUsed(){
    assertThat(substr(new Column("", STRING), 1 ,2), instanceOf(SubStrSF.class));
  }
}