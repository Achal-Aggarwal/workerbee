package net.achalaggarwal.workerbee;

import lombok.Getter;
import net.achalaggarwal.workerbee.dr.SelectFunction;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class Column extends SelectFunction {
  public static Type getType(Schema schema) {

    switch (schema.getType()) {
      case STRING:
        return Type.STRING;
      case BOOLEAN:
        return Type.BOOLEAN;
      case INT:
        return Type.INT;
      case LONG:
        return Type.LONG;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case UNION:
        return getType(schema.getTypes().get(1));
    }

    return null;
  }

  public enum Type {
    BOOLEAN {
      @Override
      public Boolean parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getBoolean(index);
      }

      @Override
      public Boolean convert(Object value) {
        return value == null ? null : Boolean.parseBoolean(String.valueOf(value));
      }
    },
    INT {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getInt(index);
      }

      @Override
      public Integer convert(Object value) {
        return value == null ? null : Integer.parseInt(String.valueOf(value));
      }
    },
    LONG {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getLong(index);
      }

      @Override
      public Long convert(Object value) {
        return value == null ? null : Long.parseLong(String.valueOf(value));
      }
    },
    BIGINT {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getBigDecimal(index);
      }

      @Override
      public BigDecimal convert(Object value) {
        return value == null ? null : new BigDecimal(String.valueOf(value));
      }
    },
    FLOAT {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getFloat(index);
      }

      @Override
      public Float convert(Object value) {
        return value == null ? null : Float.valueOf(String.valueOf(value));
      }
    },
    DOUBLE {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getDouble(index);
      }

      @Override
      public Double convert(Object value) {
        return value == null ? null : Double.valueOf(String.valueOf(value));
      }
    },
    STRING {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getString(index);
      }

      @Override
      public String convert(Object value) {
        return value == null ? null : (String) value;
      }
    },
    TIMESTAMP {
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getTimestamp(index);
      }

      @Override
      public Timestamp convert(Object value) {
        return value == null ? null : Timestamp.valueOf(String.valueOf(value));
      }
    },
    DECIMAL{
      @Override
      public Object parseValue(ResultSet resultSet, int index) throws SQLException {
        return resultSet.getBigDecimal(index);
      }

      @Override
      public BigDecimal convert(Object value) {
        return value == null ? null : new BigDecimal(String.valueOf(value));
      }
    };

    public abstract Object parseValue(ResultSet resultSet, int index) throws SQLException;

    public abstract Object convert(Object value);
  }

  @Getter
  private final String name;

  @Getter
  private final Type type;

  @Getter
  private final String comment;

  @Getter
  private Object value;

  @Getter
  private String params;

  private final Table belongsTo;

  public Column(Table belongsTo, String name, Type type) {
    this(belongsTo, name, type, null);
  }

  public Column(Table belongsTo, String name, Type type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.belongsTo = belongsTo;
    this.alias = name;
  }

  public String getFqColumnName(){
    return Utils.fqColumnName(belongsTo, this);
  }

  public Object parseValueUsing(RecordParser recordParser, int index) throws SQLException {
    try{
      return type.parseValue(recordParser, index);
    } catch (NumberFormatException nfe){
      throw new RuntimeException(
        "Couldn't parse value '" + recordParser.getString(index) + "' for "
          + getFqColumnName() + " of type '" + type + "'.");
    }
  }

  public Object parseValueUsing(ResultSet resultSet, int index) {
    try{
      return type.parseValue(resultSet, index);
    } catch (SQLException  e){
      throw new RuntimeException(
        "Couldn't parse value for " + getFqColumnName() + " of type '" + type + "'."
      );
    }
  }

  public Object convert(Object value) {
    return type.convert(value);
  }

  @Override
  public String generate() {
    if (alias.equals(name)) {
      return getFqColumnName();
    }

    return getFqColumnName() + " AS " + alias;
  }

  @Override
  public String operandName() {
    return getFqColumnName();
  }

  @Override
  public int hashCode() {
    return getName().hashCode() * getType().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Column)){
      return false;
    }

    Column column = (Column) obj;

    return getName().toLowerCase().equals(column.getName().toLowerCase())
      && getType() == column.getType();
  }

  public Column withValue(Object value) {
    Column column = new Column(belongsTo, name, type);
    column.value = value;
    return column;
  }

  public Column withParams(String params){
    this.params = params;
    return this;
  }

  public String getTypeRepresentation(){
    return type.name() + (params == null ? "" : params);
  }
}
