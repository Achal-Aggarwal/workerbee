package net.achalaggarwal.workerbee.dr.selectfunction;

import net.achalaggarwal.workerbee.Column;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class SubStrSFTest {

  public static final String COLUMN_NAME = "COLUMN_NAME";
  public static final String ALIAS_NAME = "ALIAS_NAME";

  @Test
  public void shouldGenerateSubStr(){
    assertThat(new SubStrSF(new Column(null, COLUMN_NAME, Column.Type.STRING), 1,2).generate(),
      is("SUBSTR(" + COLUMN_NAME + ", 1, 2) AS " + COLUMN_NAME));

    assertThat(new SubStrSF(new Column(null, COLUMN_NAME, Column.Type.STRING), 1,2).as(ALIAS_NAME).generate(),
      is("SUBSTR(" + COLUMN_NAME + ", 1, 2) AS " + ALIAS_NAME));
  }
}