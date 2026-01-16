package model;

import javax.crypto.spec.DHParameterSpec;

public class DHParams {

  private DHParameterSpec params;
  private byte[] publicParam;

  public DHParams(DHParameterSpec params, byte[] publicParam) {
    this.params = params;
    this.publicParam = publicParam;
  }

  public DHParameterSpec getParams() {
    return params;
  }

  public void setParams(DHParameterSpec params) {
    this.params = params;
  }

  public byte[] getPublicParam() {
    return publicParam;
  }

  public void setPublicParam(byte[] publicParam) {
    this.publicParam = publicParam;
  }
}