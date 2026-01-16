import { VacancyStatus } from "data/types"

export const statusTitles: Record<VacancyStatus, string> = {
  [VacancyStatus.created]: "Создана",
  [VacancyStatus.testTask]: "Добавление тестового задания",
  [VacancyStatus.moderating]: "На модерации",
  [VacancyStatus.rejected]: "Модерация не пройдена",
  [VacancyStatus.active]: "Активна",
  [VacancyStatus.archived]: "В архиве",
}

export const getVacancyStatuses = (activeStatus: string) => {
  const statuses = [
    VacancyStatus.created,
    VacancyStatus.testTask,
    VacancyStatus.moderating,
  ]

  if (activeStatus === VacancyStatus.rejected) {
    statuses.push(VacancyStatus.rejected)
  } else {
    statuses.push(VacancyStatus.active, VacancyStatus.archived)
  }

  return statuses
}