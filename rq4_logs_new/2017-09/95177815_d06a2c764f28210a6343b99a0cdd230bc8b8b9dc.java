package Atomix;

import io.atomix.copycat.Query;

public class GetQuery implements Query<Object> {
	private final Object key;
	
  public GetQuery(Object key) {
	  System.out.println("*******GetQuery*******");
    this.key = key;
  }

  public Object key() {
    return key;
  }
}