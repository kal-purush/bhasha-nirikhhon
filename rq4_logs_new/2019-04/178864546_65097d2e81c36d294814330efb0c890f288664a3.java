package net.jk.app.commons.boot;

/** Common ThreadLocal variables, usually for performance reasons */
public class ThreadLocals {

  /**
   * Thread-local StringBuilder, removes the overhead of creating a new one for every need Gets
   * reset to empty on every get() for easy usage in ad-hoc String concatenations.
   *
   * <p>Methods utilizing this string builder *MUST NOT* call other methods while building their
   * string as the called methods could potentially reset the state of this builder by utilizing it
   * themselves.
   */
  public static final ThreadLocal<StringBuilder> STRINGBUILDER =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder(1024);
        }

        @Override
        public StringBuilder get() {
          StringBuilder bld = super.get();
          bld.setLength(0);
          return bld;
        }
      };
}