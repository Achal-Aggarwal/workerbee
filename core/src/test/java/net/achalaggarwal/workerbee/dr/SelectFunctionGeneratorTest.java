package net.achalaggarwal.workerbee.dr;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.dr.selectfunction.AllStarSF;
import net.achalaggarwal.workerbee.dr.selectfunction.MaxSF;
import net.achalaggarwal.workerbee.dr.selectfunction.SubStrSF;
import net.achalaggarwal.workerbee.dr.selectfunction.SumSF;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

public class SelectFunctionGeneratorTest {
  @Test
  public void shouldReturnAllStarSFWhenStarIsUsed(){
    assertThat(SelectFunctionGenerator.star(), instanceOf(AllStarSF.class));
  }

  @Test
  public void shouldReturnSunStrSFWhenSubstrIsUsed(){
    assertThat(SelectFunctionGenerator.substr(new Column(null, "", Column.Type.STRING), 1, 2), instanceOf(SubStrSF.class));
  }

  @Test
  public void shouldReturnMaxSFWhenMaxIsUsed(){
    assertThat(SelectFunctionGenerator.max(new Column(null, "", Column.Type.INT)), instanceOf(MaxSF.class));
  }

  @Test
  public void shouldReturnSumSFWhenSumIsUsed(){
    assertThat(SelectFunctionGenerator.sum(new Column(null, "", Column.Type.INT)), instanceOf(SumSF.class));
  }
}