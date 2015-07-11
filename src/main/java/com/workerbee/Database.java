package com.workerbee;

import java.util.HashMap;
import java.util.Set;

public class Database {
  private String name;
  private String comment;
  private String location;

  HashMap<String, String> properties = new HashMap<String, String>();

  public Database(String name) {
    this(name, null);
  }

  public Database(String name, String comment) {
    this.name = name;
    this.comment = comment;
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public Database withComment(String comment){
    this.comment = comment;
    return this;
  }

  public Database havingProperty(String key, String value){
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

  public Database onLocation(String location) {
    this.location = location;

    return this;
  }

  public static Database DEFAULT = new Database("Default");
}
