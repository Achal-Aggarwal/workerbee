package com.workerbee;

import java.util.HashMap;
import java.util.Set;

public class Table {
  private  Database database;

  private String name;
  private String comment;
  private String location;

  private boolean external = false;

  HashMap<String, String> properties = new HashMap<String, String>();

  public Table(Database database, String name) {
    this(database, name, null);
  }

  public Table(Database database, String name, String comment) {
    this.database = database;
    this.name = name;
    this.comment = comment;
  }

  public String getDatabaseName(){
    return database.getName();
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public Table withComment(String comment){
    this.comment = comment;
    return this;
  }

  public Table havingProperty(String key, String value){
    properties.put(key, value);
    return this;
  }

  public Set<String> getProperties(){
    return properties.keySet();
  }

  public String getProperty(String property) {
    return properties.get(property);
  }

  public String getLocation() {
    return location;
  }

  public Table onLocation(String location) {
    this.location = location;

    return this;
  }

  public boolean isExternal() {
    return external;
  }
}
