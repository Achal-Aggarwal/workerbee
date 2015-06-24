package com.workerbee;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.workerbee.Utils.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


public class UtilsTest {

  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldEscapeQuotes(){
    assertThat(escapeQuote("AString"), is("AString"));
    assertThat(escapeQuote("AStringWith'SingleQuote"), is("AStringWith''SingleQuote"));
  }

  @Test
  public void shouldAddQuotesInString(){
    assertThat(quoteString("AString"), is("'AString'"));
    assertThat(quoteString("AStringWith'SingleQuote"), is("'AStringWith''SingleQuote'"));
  }

  @Test
  public void shouldReturnFullyQualifiedTableName(){
    assertThat(fqTableName(
        new Table(new Database(DATABASE_NAME), TABLE_NAME)),
      is(DATABASE_NAME + "." + TABLE_NAME));

    assertThat(fqTableName(new Table(TABLE_NAME)), is(TABLE_NAME));
  }

  @Test
  public void shouldReturnFullyQualifiedColumnName(){
    Column aColumn = new Column(null, COLUMN_NAME, Column.Type.STRING);

    assertThat(fqColumnName(null, aColumn), is(COLUMN_NAME));
    assertThat(fqColumnName(new Table(TABLE_NAME), aColumn), is(TABLE_NAME + "." + COLUMN_NAME));
  }

  @Test
  public void shouldJoinListUsingSeparator(){
    List<String> stringList = new ArrayList<String>(){{add("1"); add("2"); add("3");}};
    assertThat(joinList(stringList, ", "), is("1, 2, 3"));
  }
}