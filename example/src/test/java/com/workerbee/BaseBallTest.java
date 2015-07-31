package com.workerbee;

import com.workerbee.baseball.Batting;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static com.workerbee.BaseBall.highestScoreForEachYear;
import static com.workerbee.BaseBall.playerWithHighestScoreForEachYear;
import static com.workerbee.QueryGenerator.create;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BaseBallTest {
  public static final String PLAYER_1_ID = "PLAYER1_ID";
  public static final String PLAYER_2_ID = "PLAYER2_ID";
  public static final String PLAYER_3_ID = "PLAYER3_ID";
  private Repository repo;
  private Row<Batting> lowestRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_1_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 10);

  private Row<Batting> mediumRuns
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_2_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 100);

  private Row<Batting> maximumRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_3_ID)
    .set(Batting.year, 2000)
    .set(Batting.runs, 50);

  public BaseBallTest() throws IOException, SQLException {
    repo = Repository.TemporaryRepository();
    repo.execute(create(BaseBall.db).ifNotExist());
  }

  @Before
  public void setUp() throws IOException, SQLException {
    repo.setUp(Batting.tb);
  }

  @Test
  public void shouldReturnHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(Batting.tb)
      .setUp(Batting.tb, lowestRun, mediumRuns, maximumRun);

    List<Row<Table>> years = repo.execute(highestScoreForEachYear());

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

    List<Row<Table>> years = repo.execute(playerWithHighestScoreForEachYear());

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getString(Batting.playerId), is(PLAYER_2_ID));
    assertThat(years.get(0).getInt(Batting.year), is(1990));
    assertThat(years.get(0).getInt(Batting.runs), is(100));

    assertThat(years.get(1).getString(Batting.playerId), is(PLAYER_3_ID));
    assertThat(years.get(1).getInt(Batting.year), is(2000));
    assertThat(years.get(1).getInt(Batting.runs), is(50));
  }
}