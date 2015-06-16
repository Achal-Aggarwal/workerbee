package com.workerbee.dr;

import com.workerbee.Column;
import com.workerbee.dr.selectfunction.AllStartSF;
import com.workerbee.dr.selectfunction.SubStrSF;

public class SelectFunctionGenerator {
  public static SelectFunction star(){
    return new AllStartSF();
  }
  public static SelectFunction substr(Column column, int start, int end){
    return new SubStrSF(column, start, end);
  }
}
