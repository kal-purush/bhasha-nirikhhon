package com.treasureisland;

import java.util.logging.Logger;
import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OurLogger {
  private static final Logger logger = Logger.getLogger("com.treasureisland");

  /*
   * =============================================
   * ============= Constructors ==================
   * =============================================
   */
  public static void configLogger() {
    Handler consoleHandler = null;
    Handler fileHandler = null;
  }

  public static Logger getLogger() {
    return Logger.getLogger("com.treasureisland");
  };

  /*
   * =============================================
   * =========== Business Methods ================
   * =============================================
   */

  /*
   * =============================================
   * =========== Accessor Methods ================
   * =============================================
   */


}