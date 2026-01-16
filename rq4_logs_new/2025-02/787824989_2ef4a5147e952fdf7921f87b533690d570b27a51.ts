import Match from '@/models/Match';
import { useMatchContext } from '../context';

type PropsType = { matchs: ReturnType<Match['toPlainObject']>[] };

export const useMatchs = ({ matchs }: Readonly<PropsType>) => {
  const { currentFilter, currentMonth } = useMatchContext();
  const championnats = Array.from(new Set(matchs.map((match) => match.championnat)));

  const filteredMatchs = matchs.filter((match) => {
    switch (currentFilter) {
      case 'tous':
        return true;
      case 'domicile':
        return match.isHome;
      case 'extérieur':
        return !match.isHome;
    }
  });
  const matchsByMonth = filteredMatchs.filter((match) => match.date.getMonth() === currentMonth);

  const matchsByChampionnat = championnats.map((champ) => matchsByMonth.filter((match) => match.championnat === champ));
  return { matchsByChampionnat, matchsByMonth };
};