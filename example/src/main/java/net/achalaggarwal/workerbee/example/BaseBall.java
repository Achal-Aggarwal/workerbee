package net.achalaggarwal.workerbee.example;

import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.MigrationGenerator;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.example.baseball.Batting;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import net.achalaggarwal.workerbee.QueryGenerator;
import net.achalaggarwal.workerbee.dr.SelectFunctionGenerator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class BaseBall extends Database {
  public static final BaseBall db = new BaseBall();

  static {
    db.havingTable(Batting.tb);
  }

  private BaseBall() {
    super("BaseBall", "BaseBall database");
  }

  public static SelectQuery highestScoreForEachYear() {
    return QueryGenerator.select(Batting.year, SelectFunctionGenerator.max(Batting.runs))
      .from(Batting.tb)
      .groupBy(Batting.year)
      .ascOrderOf(Batting.year);
  }

  public static SelectQuery playerWithHighestScoreForEachYear() {
    SelectQuery selectQuery = highestScoreForEachYear().as("MaxRunsForEachYear");

    Table<Table> maxRunsForEachYear = selectQuery.table();

    return QueryGenerator.select(Batting.playerId, Batting.year, Batting.runs).from(Batting.tb)
      .join(selectQuery)
      .on(Batting.year.eq(maxRunsForEachYear.getColumn(Batting.year))
          .and(Batting.runs.eq(maxRunsForEachYear.getColumn(Batting.runs)))
      );
  }

  public static void main(String[] args) throws IOException, SQLException {
    if (args.length < 1){
      System.out.println("Please provide first argument as migration directory.");
      return;
    }

    File migrationDirectory = new File(args[0]);

    if (!migrationDirectory.exists() || !migrationDirectory.isDirectory()){
      System.out.println("Please provide first argument as migration directory.");
      return;
    }

    MigrationGenerator.generateFilesFor(BaseBall.db, migrationDirectory);
  }
}
