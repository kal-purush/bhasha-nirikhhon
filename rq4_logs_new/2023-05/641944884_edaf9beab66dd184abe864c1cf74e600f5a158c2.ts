import { CandidateStatus } from "data/types"

export const statusTitles: Record<CandidateStatus, string> = {
  [CandidateStatus.accepted]: "Заявка принята",
  [CandidateStatus.moderation]: "На модерации",
  [CandidateStatus.education]: "Проходит обучение",
  [CandidateStatus.tested]: "Прошел тестирование",
  [CandidateStatus.hackathon]: "Проходит кейс-чемпионат",
  [CandidateStatus.passed]: "Отобран на стажировку",
  [CandidateStatus.rejected]: "Отклонен",
}

const baseStatusList = [
  CandidateStatus.accepted,
  CandidateStatus.moderation,
  CandidateStatus.education,
  CandidateStatus.tested,
  CandidateStatus.hackathon,
  CandidateStatus.passed,
]

export const getCandidateStatuses = (
  activeStatus: CandidateStatus,
  previousStatus: CandidateStatus
) => {
  if (activeStatus === CandidateStatus.rejected) {
    const statusBeforeRejecting = baseStatusList.indexOf(previousStatus)

    const statusList = baseStatusList.slice(0, statusBeforeRejecting + 1)
    statusList.push(CandidateStatus.rejected)

    return statusList
  }

  return baseStatusList
}