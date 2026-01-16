import { selector } from 'recoil';
import amountOfCurrentWaterAtom from './amountOfCurrentWaterAtom';
import amountOfGoalAtom from './amountOfGoalAtom';

const achievementRateSelector = selector({
  key: 'achievementRateSelector',
  get: ({ get }) => {
    const amountOfCurrentWater = get(amountOfCurrentWaterAtom);
    const amountOfGoal = get(amountOfGoalAtom);

    const achievementRate = Math.round(
      (amountOfCurrentWater / amountOfGoal) * 100,
    );

    return achievementRate;
  },
});

export default achievementRateSelector;