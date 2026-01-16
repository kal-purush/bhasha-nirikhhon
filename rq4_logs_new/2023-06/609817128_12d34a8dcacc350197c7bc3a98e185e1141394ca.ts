import { HOURS } from '../../components/Calendar/Calendar.constants';
import { CalendarTileConfigType } from '../../components/Calendar/CalendarTile/CalendarTile.types';
import { TrainingDay } from './Workouts.types';

/**
 *
 * @param trainingWeek Array of training days from a specific training plan
 */
export const generateReservedTilesFromTrainings = (
  trainingWeek: Array<TrainingDay>
): CalendarTileConfigType[][] => {
  const config: CalendarTileConfigType[][] = [];
  let tile = {};

  for (let i = 0; i < HOURS.length; i++) {
    config[i] = [];

    let dayNumber = 0;

    for (const trainingDay of trainingWeek) {
      const matchingSession = trainingDay.trainingSessions.find(session => session.startHour === i);
      if (matchingSession) {
        tile = {
          children: matchingSession.name,
          isReserved: true,
          key: matchingSession.documentId,
        };
      } else {
        tile = {
          children: undefined,
          isReserved: false,
          key: crypto.randomUUID(),
        };
      }
      config[i][dayNumber++] = tile;
    }
  }
  return config;
};