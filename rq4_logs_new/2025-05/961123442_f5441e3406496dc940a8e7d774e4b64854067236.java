package util;

import java.io.*;
import java.util.Objects;

public class ParIdId implements RegistroHashExtensivel<ParIdId> {

  protected int id1; // Ex: ID da série
  protected int id2; // Ex: ID do ator

  public ParIdId() {
    this(-1, -1);
  }

  public ParIdId(int id1, int id2) {
    this.id1 = id1;
    this.id2 = id2;
  }

  public int getId1() {
    return id1;
  }

  public int getId2() {
    return id2;
  }

  public void setId1(int id1) {
    this.id1 = id1;
  }

  public void setId2(int id2) {
    this.id2 = id2;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id1);
  }

  @Override
  public short size() {
    return 8;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(ba);
    out.writeInt(id1);
    out.writeInt(id2);
    return ba.toByteArray();
  }

  @Override
  public void fromByteArray(byte[] ba) throws IOException {
    ByteArrayInputStream bb = new ByteArrayInputStream(ba);
    DataInputStream in = new DataInputStream(bb);
    id1 = in.readInt();
    id2 = in.readInt();
  }

  @Override
  public String toString() {
    return "(" + id1 + ", " + id2 + ")";
  }
}