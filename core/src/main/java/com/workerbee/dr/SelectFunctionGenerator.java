package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.dr.selectfunction.AllStarSF;
import com.workerbee.dr.selectfunction.MaxSF;
import com.workerbee.dr.selectfunction.SubStrSF;

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
