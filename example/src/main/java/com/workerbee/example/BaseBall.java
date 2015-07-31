package com.workerbee.example;

import com.workerbee.Database;
import com.workerbee.MigrationGenerator;
import com.workerbee.Table;
import com.workerbee.annotation.WBDatabase;
import com.workerbee.example.baseball.Batting;
import com.workerbee.dr.SelectQuery;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static com.workerbee.QueryGenerator.select;
import static com.workerbee.dr.SelectFunctionGenerator.max;

@WBDatabase
public class BaseBall extends Database {
  public static final BaseBall db = new BaseBall();

  private BaseBall() {
    super("BaseBall", "BaseBall database");
  }

  public static SelectQuery highestScoreForEachYear() {
    return select(Batting.year, max(Batting.runs))
      .from(Batting.tb)
      .groupBy(Batting.year)
      .ascOrderOf(Batting.year);
  }

  public static SelectQuery playerWithHighestScoreForEachYear() {
    SelectQuery selectQuery = highestScoreForEachYear().as("MaxRunsForEachYear");

    Table<Table> maxRunsForEachYear = selectQuery.table();

    return select(Batting.playerId, Batting.year, Batting.runs).from(Batting.tb)
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

    Table t = Batting.tb;
    MigrationGenerator.generateFilesFor(BaseBall.db, migrationDirectory);
  }
}
