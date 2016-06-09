package net.achalaggarwal.workerbee.example;

import net.achalaggarwal.workerbee.Database;
import net.achalaggarwal.workerbee.TextTable;
import net.achalaggarwal.workerbee.tools.MigrationGenerator;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.dml.insert.InsertQuery;
import net.achalaggarwal.workerbee.example.baseball.BattingTable;
import net.achalaggarwal.workerbee.dr.SelectQuery;
import net.achalaggarwal.workerbee.dr.SelectFunctionGenerator;
import net.achalaggarwal.workerbee.example.baseball.PlayerTable;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static net.achalaggarwal.workerbee.QueryGenerator.insert;
import static net.achalaggarwal.workerbee.QueryGenerator.select;
import static net.achalaggarwal.workerbee.dr.SelectFunctionGenerator.sum;

public class BaseBall extends Database {
  public static final BaseBall db = new BaseBall();

  static {
    db.havingTable(BattingTable.tb);
    db.havingTable(PlayerTable.tb);
  }

  private BaseBall() {
    super("BaseBall", "BaseBall database");
  }

  public static SelectQuery highestScoreForEachYear() {
      return select(BattingTable.year, SelectFunctionGenerator.max(BattingTable.runs))
      .from(BattingTable.tb)
      .groupBy(BattingTable.year)
      .ascOrderOf(BattingTable.year);
  }

  public static SelectQuery playerWithHighestScoreForEachYear() {
    SelectQuery selectQuery = highestScoreForEachYear().as("MaxRunsForEachYear");

    TextTable<TextTable> maxRunsForEachYear = selectQuery.table();

    return select(BattingTable.playerId, BattingTable.year, BattingTable.runs).from(BattingTable.tb)
      .join(selectQuery)
      .on(BattingTable.year.eq(maxRunsForEachYear.getColumn(BattingTable.year))
          .and(BattingTable.runs.eq(maxRunsForEachYear.getColumn(BattingTable.runs)))
      );
  }

  public static InsertQuery insertPlayerWithTotalRunsOverAllYears() {
    return insert()
      .intoTable(PlayerTable.tb)
      .partitionOn(PlayerTable.timestamp, 0)
      .using(
        select(BattingTable.playerId, sum(BattingTable.runs)).from(BattingTable.tb).groupBy(BattingTable.playerId)
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
