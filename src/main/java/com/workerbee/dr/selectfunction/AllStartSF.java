package com.workerbee.dr.selectfunction;

import com.workerbee.Column;
import com.workerbee.dr.SelectFunction;

public class AllStartSF extends SelectFunction {
  @Override
  public String generate() {
    return "*";
  }
}