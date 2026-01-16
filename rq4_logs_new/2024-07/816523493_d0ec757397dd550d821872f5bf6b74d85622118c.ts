export interface Cohort {
  id: number;
  year: number; // 西暦
  era: string; // 元号
  eraYear?: number; // 元号の年数
}

export const cohorts: Cohort[] = [{ id: 1, year: 1999, era: '平成', eraYear: 11 }];

cohorts.unshift({ id: 0, year: 2000, era: '2000年以前' });

let nextId = 2;

function getEra(year: number): { era: string; eraYear: number } {
  if (year >= 1989 && year < 2019) {
    return { era: '平成', eraYear: year - 1988 };
  } else if (year >= 2019) {
    return { era: '令和', eraYear: year - 2018 };
  } else {
    return { era: '昭和', eraYear: year - 1925 };
  }
}

for (let year = 2000; year <= new Date().getFullYear(); year++) {
  const { era, eraYear } = getEra(year);
  cohorts.push({ id: nextId++, year, era, eraYear });
}