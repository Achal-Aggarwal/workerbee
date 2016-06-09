package net.achalaggarwal.workerbee;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Database {
  @Getter
  private String name;

  @Getter
  private String comment;

  @Getter
  private String location;

  private HashMap<String, String> properties = new HashMap<>();

  private HashSet<Table> tables = new HashSet<>();

  public Database(String name) {
    this(name, null);
  }

  public Database(String name, String comment) {
    this.name = name;
    this.comment = comment;
  }

  public Database withComment(String comment){
    this.comment = comment;
    return this;
  }

  public Database havingTable(Table table) {
    tables.add(table);

    return this;
  }

  public Table[] getTables() {
    return tables.toArray(new Table[tables.size()]);
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

  public Database onLocation(String location) {
    this.location = location;

    return this;
  }

  public static Database DEFAULT = new Database("Default");
}
