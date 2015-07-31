package com.workerbee.example.baseball;


import com.workerbee.example.BaseBall;
import com.workerbee.Column;
import com.workerbee.Table;

public class Batting extends Table<Batting> {
  public static final Batting tb = new Batting();

  public static final Column playerId = HavingColumn(tb, "player_id", Column.Type.STRING);
  public static final Column year     = HavingColumn(tb, "year",      Column.Type.INT);
  public static final Column runs     = HavingColumn(tb, "runs",      Column.Type.INT);

  private Batting() {
    super(BaseBall.db, "Batting", "Batting table", 1);
  }
}
