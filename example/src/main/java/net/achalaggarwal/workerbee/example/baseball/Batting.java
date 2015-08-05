package net.achalaggarwal.workerbee.example.baseball;


import net.achalaggarwal.workerbee.example.BaseBall;
import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Table;

public class Batting extends Table<Batting> {
  public static final Batting tb = new Batting();

  public static final Column playerId = HavingColumn(tb, "player_id", Column.Type.STRING);
  public static final Column year     = HavingColumn(tb, "year",      Column.Type.INT);
  public static final Column runs     = HavingColumn(tb, "runs",      Column.Type.INT);

  private Batting() {
    super(BaseBall.db, "Batting", "Batting table", 1);
  }
}
