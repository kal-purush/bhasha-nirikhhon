package exclude_test;

public class Binary {
  public static void main(String[] args) {
    LibraryUsingWanted lib1 = new LibraryUsingWanted();
    LibraryUsingUnwanted lib2 = new LibraryUsingUnwanted();
    boolean dep_result = lib1.doit() && lib2.doit();
    try {
      boolean runtime_dep_result = lib1.runtime_doit() && lib2.runtime_doit();
      if (runtime_dep_result != dep_result) {
        System.exit(17);
      }
    } catch (Exception e) {
      System.exit(42);
    }

    System.exit(dep_result ? 0 : 1);
  }
}