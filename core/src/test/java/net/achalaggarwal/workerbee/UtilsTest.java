package net.achalaggarwal.workerbee;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class UtilsTest {
  public static final String DATABASE_NAME = "DATABASE_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String COLUMN_NAME = "COLUMN_NAME";

  @Test
  public void shouldEscapeQuotes(){
    assertThat(Utils.escapeQuote("AString"), is("AString"));
    assertThat(Utils.escapeQuote("AStringWith'SingleQuote"), is("AStringWith''SingleQuote"));
  }

  @Test
  public void shouldAddQuotesInString(){
    assertThat(Utils.quoteString("AString"), is("'AString'"));
    assertThat(Utils.quoteString("AStringWith'SingleQuote"), is("'AStringWith''SingleQuote'"));
  }

  @Test
  public void shouldReturnFullyQualifiedTableName(){
    assertThat(Utils.fqTableName(
        new TextTable(new Database(DATABASE_NAME), TABLE_NAME)),
      is(DATABASE_NAME + "." + TABLE_NAME));

    assertThat(Utils.fqTableName(new TextTable(TABLE_NAME)), is(TABLE_NAME));
  }

  @Test
  public void shouldReturnFullyQualifiedColumnName(){
    Column aColumn = new Column(null, COLUMN_NAME, Column.Type.STRING);

    assertThat(Utils.fqColumnName(null, aColumn), is(COLUMN_NAME));
    assertThat(Utils.fqColumnName(new TextTable(TABLE_NAME), aColumn), is(TABLE_NAME + "." + COLUMN_NAME));
  }

  @Test
  public void shouldJoinListUsingSeparator(){
    List<String> stringList = new ArrayList<String>(){{add("1"); add("2"); add("3");}};
    assertThat(Utils.joinList(stringList, ", "), is("1, 2, 3"));
  }
}