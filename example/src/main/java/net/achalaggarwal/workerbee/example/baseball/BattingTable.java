package net.achalaggarwal.workerbee.example.baseball;


import net.achalaggarwal.workerbee.example.BaseBall;
import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Table;

public class BattingTable extends Table<BattingTable> {
  public static final BattingTable tb = new BattingTable();

  public static final Column playerId = HavingColumn(tb, "player_id", Column.Type.STRING);
  public static final Column year     = HavingColumn(tb, "year",      Column.Type.INT);
  public static final Column runs     = HavingColumn(tb, "runs",      Column.Type.INT);

  private BattingTable() {
    super(BaseBall.db, "Batting", "Batting table", 1);
  }
}
