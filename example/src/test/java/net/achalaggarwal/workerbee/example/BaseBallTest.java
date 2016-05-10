package net.achalaggarwal.workerbee.example;

import net.achalaggarwal.workerbee.Repository;
import net.achalaggarwal.workerbee.Row;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.example.baseball.BattingTable;
import net.achalaggarwal.workerbee.QueryGenerator;
import net.achalaggarwal.workerbee.example.baseball.PlayerTable;
import net.achalaggarwal.workerbee.example.baseball.domain.Player;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class BaseBallTest {
  public static final String PLAYER_1_ID = "PLAYER1_ID";
  public static final String PLAYER_2_ID = "PLAYER2_ID";
  public static final String PLAYER_3_ID = "PLAYER3_ID";
  private static Repository repo;

  private static Row<BattingTable> lowestRun
    = BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_1_ID)
    .set(BattingTable.year, 1990)
    .set(BattingTable.runs, 10);

  private static Row<BattingTable> mediumRuns
    = BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_2_ID)
    .set(BattingTable.year, 1990)
    .set(BattingTable.runs, 100);

  private static Row<BattingTable> maximumRun
    = BattingTable.tb.getNewRow()
    .set(BattingTable.playerId, PLAYER_3_ID)
    .set(BattingTable.year, 2000)
    .set(BattingTable.runs, 50);

  @BeforeClass
  public static void BeforeClass() throws IOException, SQLException {
    repo = Repository.TemporaryRepository();
    repo.execute(QueryGenerator.create(BaseBall.db).ifNotExist());
  }

  @Before
  public void setUp() throws IOException, SQLException {
    repo.setUp(BattingTable.tb);
  }

  @Test
  public void shouldReturnHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(BattingTable.tb)
      .setUp(BattingTable.tb, lowestRun, mediumRuns, maximumRun);

    List<Row<Table>> years = repo.execute(BaseBall.highestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getInt(BattingTable.year), is(1990));
    assertThat(years.get(0).getInt(BattingTable.runs), is(100));

    assertThat(years.get(1).getInt(BattingTable.year), is(2000));
    assertThat(years.get(1).getInt(BattingTable.runs), is(50));
  }

  @Test
  public void shouldReturnPlayerWithHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(BattingTable.tb)
      .setUp(BattingTable.tb, lowestRun, mediumRuns, maximumRun);

    List<Row<Table>> years = repo.execute(BaseBall.playerWithHighestScoreForEachYear());

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
    repo.setUp(BattingTable.tb)
      .setUp(BattingTable.tb, lowestRun, lowestRun, maximumRun)
      .setUp(PlayerTable.tb);

    repo.execute(BaseBall.insertPlayerWithTotalRunsOverAllYears());

    List<Player> players = repo.getSpecificRecordsOf(PlayerTable.tb);

    assertThat(players.size(), is(2));

    assertThat(players.get(0).getPlayerId(), is(PLAYER_1_ID));
    assertThat(players.get(0).getTotalRuns(), is(new BigDecimal(20)));

    assertThat(players.get(1).getPlayerId(), is(PLAYER_3_ID));
    assertThat(players.get(1).getTotalRuns(), is(new BigDecimal(50)));
  }
}