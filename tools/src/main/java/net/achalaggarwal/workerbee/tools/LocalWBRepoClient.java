package net.achalaggarwal.workerbee.tools;

import net.achalaggarwal.workerbee.Repository;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class LocalWBRepoClient {
  public static void main(String[] args) throws IOException, SQLException, URISyntaxException {
    Repository repo = Repository.TemporaryRepository(Paths.get(args[0]));

    String query;
    BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));

    while(!"quit;".equals(query = bufferRead.readLine())) {

      if(query.endsWith(";")) {
        query = query.substring(0, query.length()-1);
      }

      try {
        ResultSet resultSet = repo.executeForResult(query);
        while (resultSet.next()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            System.out.print(resultSet.getString(i));
          }
          System.out.println();
        }
      } catch (SQLException ex) {
        System.out.println(ex.getMessage());
      }
    }
  }
}
