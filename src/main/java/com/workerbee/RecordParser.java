package com.workerbee;


import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.parseInt;

public class RecordParser {

  private List<String> values;
  private String hiveNull;

  public RecordParser(String row, String columnSeparator, String hiveNull) {
    this(Arrays.asList(row.split(columnSeparator)), hiveNull);
  }

  public RecordParser(List<String> values, String hiveNull) {
    this.hiveNull = hiveNull;
    this.values = values;
  }

  public Integer readInt(int index) {
    String value = at(index);
    return isValid(value) ? parseInt(value) : null;
  }

  public String readString(int index) {
    String value = at(index);
    return isValid(value) ? value : null;
  }

  private String at(int index) {
    return (values.size() > index) ? values.get(index) : null;
  }

  private boolean isValid(String value) {
    return (value != null && !value.equals("") && !value.equals(hiveNull));
  }
}
