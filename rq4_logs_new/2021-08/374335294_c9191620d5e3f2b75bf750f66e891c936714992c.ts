/**
 * Unit deployment status value.
 *
 * Available in {@link UnitContext.readStatus unit context}.
 */
export const enum UnitStatus {

  /**
   * The unit is available in formation, but its {@link Unit.instruct instructions} are not applied yet.
   */
  Available,

  /**
   * The unit is instructed when its {@link Unit instruct instructions} applied, but its tasks
   * {@link OrderSubject.execute execution} is not completed yet.
   */
  Instructed,

  /**
   * The unit is executed when its its tasks {@link OrderSubject.execute execution} completed.
   */
  Executed,

  /**
   * The unit is ready for use when its startup sequence completed.
   */
  Ready

}