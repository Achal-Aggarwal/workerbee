package net.achalaggarwal.workerbee.example;

import net.achalaggarwal.workerbee.Repository;
import net.achalaggarwal.workerbee.Row;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.dr.SelectFunction;
import net.achalaggarwal.workerbee.example.baseball.Batting;
import net.achalaggarwal.workerbee.QueryGenerator;
import net.achalaggarwal.workerbee.example.baseball.Player;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static net.achalaggarwal.workerbee.dr.SelectFunctionGenerator.max;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class BaseBallTest {
  public static final String PLAYER_1_ID = "PLAYER1_ID";
  public static final String PLAYER_2_ID = "PLAYER2_ID";
  public static final String PLAYER_3_ID = "PLAYER3_ID";
  private static Repository repo;

  private static Row<Batting> lowestRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_1_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 10);

  private static Row<Batting> mediumRuns
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_2_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 100);

  private static Row<Batting> maximumRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_3_ID)
    .set(Batting.year, 2000)
    .set(Batting.runs, 50);

  @BeforeClass
  public static void BeforeClass() throws IOException, SQLException {
    repo = Repository.TemporaryRepository();
    repo.execute(QueryGenerator.create(BaseBall.db).ifNotExist());
  }

  @Before
  public void setUp() throws IOException, SQLException {
    repo.setUp(Batting.tb);
  }

  @Test
  public void shouldReturnHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(Batting.tb)
      .setUp(Batting.tb, lowestRun, mediumRuns, maximumRun);

    List<Row<Table>> years = repo.execute(BaseBall.highestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getInt(Batting.year), is(1990));
    assertThat(years.get(0).getInt(Batting.runs), is(100));

    assertThat(years.get(1).getInt(Batting.year), is(2000));
    assertThat(years.get(1).getInt(Batting.runs), is(50));
  }

  @Test
  public void shouldReturnPlayerWithHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(Batting.tb)
      .setUp(Batting.tb, lowestRun, mediumRuns, maximumRun);

    List<Row<Table>> years = repo.execute(BaseBall.playerWithHighestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getString(Batting.playerId), is(PLAYER_2_ID));
    assertThat(years.get(0).getInt(Batting.year), is(1990));
    assertThat(years.get(0).getInt(Batting.runs), is(100));

    assertThat(years.get(1).getString(Batting.playerId), is(PLAYER_3_ID));
    assertThat(years.get(1).getInt(Batting.year), is(2000));
    assertThat(years.get(1).getInt(Batting.runs), is(50));
  }

  @Test
  public void shouldInsertPlayerWithTotalRunsOverAllYearsCorrectly() throws IOException, SQLException {
    repo.setUp(Batting.tb)
      .setUp(Batting.tb, lowestRun, lowestRun, maximumRun)
      .setUp(Player.tb);

    repo.execute(BaseBall.insertPlayerWithTotalRunsOverAllYears());

    List<Row<Player>> rows = repo.getRowsOf(Player.tb);

    assertThat(rows.size(), is(2));

    assertThat(rows.get(0).getString(Player.playerId), is(PLAYER_1_ID));
    assertThat(rows.get(0).getInt(Player.totalRuns), is(20));

    assertThat(rows.get(1).getString(Player.playerId), is(PLAYER_3_ID));
    assertThat(rows.get(1).getInt(Player.totalRuns), is(50));
  }
}