package net.achalaggarwal.workerbee.example;

import net.achalaggarwal.workerbee.*;
import net.achalaggarwal.workerbee.example.baseball.BattingTable;
import net.achalaggarwal.workerbee.example.baseball.PlayerTable;
import net.achalaggarwal.workerbee.example.baseball.domain.Player;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class BaseBallTest {
  public static final String PLAYER_1_ID = "PLAYER1_ID";
  public static final String PLAYER_2_ID = "PLAYER2_ID";
  public static final String PLAYER_3_ID = "PLAYER3_ID";
  private static Repository repo;

  @SuppressWarnings("unchecked")
  private static Row<BattingTable> lowestRun
    = (Row<BattingTable>) BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_1_ID)
    .set(BattingTable.year, 1990)
    .set(BattingTable.runs, 10)
    .set(BattingTable.timestamp, 0);

  @SuppressWarnings("unchecked")
  private static Row<BattingTable> mediumRuns
    = (Row<BattingTable>) BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_2_ID)
    .set(BattingTable.year, 1990)
    .set(BattingTable.runs, 100)
    .set(BattingTable.timestamp, 0);

  @SuppressWarnings("unchecked")
  private static Row<BattingTable> maximumRun
    = (Row<BattingTable>) BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_3_ID)
    .set(BattingTable.year, 2000)
    .set(BattingTable.runs, 50)
    .set(BattingTable.timestamp, 0);

  @BeforeClass
  public static void BeforeClass() throws IOException, SQLException {
    repo = localHs2Repo();
    repo.execute(QueryGenerator.create(BaseBall.db).ifNotExist());
  }

  @Before
  public void setUp() throws IOException, SQLException {
    repo
      .create(BattingTable.tb)
      .create(PlayerTable.tb);
  }

  @Test
  public void shouldReturnHighestScoreForEachYear() throws IOException, SQLException {
    repo.load(BattingTable.tb, Arrays.asList(lowestRun, mediumRuns, maximumRun));

    List<Row<TextTable>> years = repo.execute(BaseBall.highestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getInt(BattingTable.year), is(1990));
    assertThat(years.get(0).getInt(BattingTable.runs), is(100));

    assertThat(years.get(1).getInt(BattingTable.year), is(2000));
    assertThat(years.get(1).getInt(BattingTable.runs), is(50));
  }

  @Test
  public void shouldReturnPlayerWithHighestScoreForEachYear() throws IOException, SQLException {
    repo.load(BattingTable.tb, Arrays.asList(lowestRun, mediumRuns, maximumRun));

    List<Row<TextTable>> years = repo.execute(BaseBall.playerWithHighestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getString(BattingTable.playerId), is(PLAYER_2_ID));
    assertThat(years.get(0).getInt(BattingTable.year), is(1990));
    assertThat(years.get(0).getInt(BattingTable.runs), is(100));

    assertThat(years.get(1).getString(BattingTable.playerId), is(PLAYER_3_ID));
    assertThat(years.get(1).getInt(BattingTable.year), is(2000));
    assertThat(years.get(1).getInt(BattingTable.runs), is(50));
  }

  @Test
  public void shouldInsertPlayerWithTotalRunsOverAllYearsCorrectly() throws IOException, SQLException {
    repo.load(BattingTable.tb, Arrays.asList(lowestRun, lowestRun, maximumRun));

    repo.execute(BaseBall.insertPlayerWithTotalRunsOverAllYears());

    List<Player> players = repo.getSpecificRecordsOf(PlayerTable.tb, PlayerTable.timestamp.withValue(0));

    assertThat(players.size(), is(2));

    assertThat(players.get(0).getPlayerId(), is(PLAYER_1_ID));
    assertThat(players.get(0).getTotalRuns(), is(new BigDecimal(20)));

    assertThat(players.get(1).getPlayerId(), is(PLAYER_3_ID));
    assertThat(players.get(1).getTotalRuns(), is(new BigDecimal(50)));
  }

  private static Repository localHs2Repo() throws SQLException, IOException {
    return new Repository(
      "jdbc:hive2://localhost:20103/default",
      new Properties(){{
        put("user", "user");
        put("password", "pass");
      }},
      new Configuration(){{
        set("fs.defaultFS", "hdfs://localhost:20112");
      }}
    );
  }
}