package com.sweng894.GetVaccinated.schedule;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Locale;

/**
 * A repository that stores state in-memory.
 */
public final class MemoryScheduleRepository implements ScheduleRepository {
  public ScheduleConfirmation saveRequest(ScheduleRequest request) {
    var confirmationNumber = RandomStringUtils.randomAlphanumeric(16).toUpperCase(Locale.ROOT);
    var confirmation = new ScheduleConfirmation();
    confirmation.setConfirmationNumber(confirmationNumber);
    return confirmation;
  }
}