import {
  AttendanceHistory,
  MemberAttendanceHistory
} from "requests/attendanceHistoryTypes";

export const getFirstTimeAttendeeRate = (
  attendanceHistory: AttendanceHistory
) => {};

type AttendanceHistoryCount = {
  attended: number;
  rsvped: number;
};

export const getAttendanceRateForMembersWithAtLeastOneMeetupAttended = (
  attendanceHistory: AttendanceHistory
) => {
  return attendanceHistory.memberAttendanceHistory.reduce(
    (acc: AttendanceHistoryCount, attendee: MemberAttendanceHistory) => {
      return {
        attended: acc.attended + attendee.eventsAttendedCount,
        rsvped: acc.rsvped + attendee.eventsRSVPedCount
      };
    },
    {
      attended: 0,
      rsvped: 0
    }
  );
};

export const getAttendanceRateForPreviousAttendees = (
  attendanceHistory: AttendanceHistory
) => {
  const attendanceCounts = getAttendanceRateForMembersWithAtLeastOneMeetupAttended(
    attendanceHistory
  );
  const { attended, rsvped } = attendanceCounts;
  return attended / rsvped;
};

export const getPredictedShowRate = (
  attendanceHistory: AttendanceHistory,
  attendeeIds: string[]
) => {
  const {
    attended,
    rsvped
  } = attendanceHistory.attendeeHistoryForThoseWhoAttendedOnlyOneMeetup;

  const attendanceRateForPreviousAttendees = getAttendanceRateForPreviousAttendees(
    attendanceHistory
  );
  const regulars = attendanceHistory.memberAttendanceHistory.length;
  const firstTimers = attendeeIds.length - regulars;
  const expectedAttendees =
    (attended / rsvped) * regulars +
    firstTimers * attendanceRateForPreviousAttendees;

  return expectedAttendees;
};