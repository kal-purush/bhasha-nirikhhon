package myorganizer.organizer.models;

public class TokenEntity {
  private String token;

  @Override
  public String toString() {
    return "{\"token\":" + "\"" + token + "\"}";
  }

  public TokenEntity(String token) {
    this.token = token;
  }
}