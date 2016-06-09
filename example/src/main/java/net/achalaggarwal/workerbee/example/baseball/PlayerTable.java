package net.achalaggarwal.workerbee.example.baseball;


import net.achalaggarwal.workerbee.AvroTable;
import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.Table;
import net.achalaggarwal.workerbee.example.BaseBall;

public class PlayerTable extends AvroTable<PlayerTable> {
  public static final PlayerTable tb = new PlayerTable();

  public static final Column timestamp = PartitionedOnColumn(tb, "timestamp", Column.Type.INT);

  private PlayerTable() {
    super(BaseBall.db, "Player", "Player table", 1);
    readSchema(net.achalaggarwal.workerbee.example.baseball.domain.Player.class);
  }
}
