package com.workerbee;

import java.util.Map;

public class Row {
  Map<Column, Object> map;

  public Row(Map<Column, Object> map){
    this.map = map;
  }

  public Object get(Column column) {
    return map.get(column);
  }

  public String getString(Column column) {
    return (String) get(column);
  }

  public Integer getInt(Column column) {
    return (Integer) get(column);
  }

  public Row set(Column column, Object value) {
    if (map.containsKey(column)){
      map.put(column, value);
    }

    return this;
  }
}
