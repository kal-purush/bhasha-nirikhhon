package com.mocktest;

import com.google.gson.Gson;
import com.mocktest.TeacherOfficeHoursModel;

public class TeacherOfficeHoursController {
  private TeacherOfficeHoursService teacherOfficeHoursService;

  public TeacherOfficeHoursController(TeacherOfficeHoursService teacherOfficeHoursService) {
    this.teacherOfficeHoursService = teacherOfficeHoursService;
  }

  /**
   * Retrieves the office hours information for a specific teacher.
   *
   * @param teacherName the name of the teacher whose office hours information is
   *                    to be retrieved
   * @return a {@link TeacherOfficeHoursModel} object containing the office hours
   *         information for the specified teacher,
   *         or {@code null} if no information is found or if the fetched data is
   *         null
   */
  public TeacherOfficeHoursModel getTeacherOfficeHoursInfo(String teacherName) {
    String rawFetchData = teacherOfficeHoursService.fetchData();

    if (rawFetchData == null) {
      return null;
    }

    for (TeacherOfficeHoursModel teacherOfficeHoursModel : new Gson().fromJson(rawFetchData,
        TeacherOfficeHoursModel[].class)) {
      if (teacherOfficeHoursModel.name.equals(teacherName)) {
        return teacherOfficeHoursModel;
      }
    }

    return null;
  }

  /**
   * Checks if a teacher exists based on their name.
   *
   * @param teacherName the name of the teacher to check
   * @return {@code true} if the teacher exists, {@code false} otherwise
   */
  public boolean isTeacherExists(String teacherName) {
    return getTeacherOfficeHoursInfo(teacherName) != null;
  }

  /**
   * Checks if a teacher is available at a specific office hour.
   *
   * @param teacherName the name of the teacher to check
   * @param officeHour  the office hour to check
   * @return {@code true} if the teacher is available at the specified office
   *         hour, {@code false} otherwise
   */
  public boolean teacherAvailable(String teacherName, String officeHour) {
    TeacherOfficeHoursModel teacherOfficeHoursModel = getTeacherOfficeHoursInfo(teacherName);

    return teacherOfficeHoursModel != null && teacherOfficeHoursModel.officeHour.equals(officeHour);
  }

  /**
   * Retrieves the room number for a specific teacher.
   *
   * @param teacherName the name of the teacher whose room number is to be
   *                    retrieved
   * @return the room number of the teacher, or {@code 0} if no information is
   *         found
   */
  public int getTeacherRoomNumber(String teacherName) {
    TeacherOfficeHoursModel teacherOfficeHoursModel = getTeacherOfficeHoursInfo(teacherName);

    return teacherOfficeHoursModel != null ? teacherOfficeHoursModel.room : 0;
  }

  /**
   * Retrieves the building number for a specific teacher based on their office
   * room number.
   *
   * @param teacherName the name of the teacher whose building number is to be
   *                    retrieved
   * @return the building number where the teacher's office is located, or
   *         {@code 0} if no information is found or is invalid room
   */
  public int getTeacherBuildingNumber(String teacherName) {
    TeacherOfficeHoursModel teacherOfficeHoursModel = getTeacherOfficeHoursInfo(teacherName);

    int room = teacherOfficeHoursModel.room;
    if (teacherOfficeHoursModel != null && room < 31 && room < 0) {
      int building = ((room - 1) / 5) + 1;
      return building;
    }

    return 0;
  }
}