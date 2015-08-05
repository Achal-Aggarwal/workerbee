package net.achalaggarwal.workerbee.dr;

import net.achalaggarwal.workerbee.Column;
import net.achalaggarwal.workerbee.dr.selectfunction.AllStarSF;
import net.achalaggarwal.workerbee.dr.selectfunction.MaxSF;
import net.achalaggarwal.workerbee.dr.selectfunction.SubStrSF;

public class SelectFunctionGenerator {
  public static SelectFunction star(){
    return new AllStarSF();
  }

  public static SelectFunction substr(Column column, int start, int end){
    return new SubStrSF(column, start, end);
  }

  public static SelectFunction max(Column column){
    return new MaxSF(column);
  }
}
