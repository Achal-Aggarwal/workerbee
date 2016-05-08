package net.achalaggarwal.workerbee.example.baseball;


import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.example.BaseBall;

public class Player extends Table<Player> {
  public static final Player tb = new Player();

  public static final Column playerId     = HavingColumn(tb, "playerId",      Column.Type.STRING);
  public static final Column totalRuns     = HavingColumn(tb, "totalRuns",      Column.Type.INT);

  private Player() {
    super(BaseBall.db, "Player", "Player table", 1);
  }
}
