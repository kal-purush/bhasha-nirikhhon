// CHECKSTYLE.OFF: JavadocParagraph
/**
 * SolveMeFirst.
 *
 * ////////////////////////////////////////////////////////////////////////////
 * Result found:
 * ////////////////////////////////////////////////////////////////////////////
 */

package ae.hackerrank;

import java.text.MessageFormat;

/**
 * SolveMeFirst.
 */
public class SolveMeFirst {

  private SolveMeFirst() {}

  static java.util.logging.Logger logger = ae.projecteuler.util.CustomLogger.getLogger();

  /**
   * solveMeFirst.
   */
  public static Integer solveMeFirst(Integer[] inputs) {

    int total = 0;

    for (Integer name : inputs) {
      total += name;
    }

    String log = MessageFormat.format("solveMeFirst solved: {0}", total);
    logger.info(log);

    return total;
  }
}

//CHECKSTYLE.ON: JavadocParagraph