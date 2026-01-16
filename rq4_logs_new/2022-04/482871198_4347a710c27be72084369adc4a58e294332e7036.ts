import { createContext } from "react"
import { Diary, ReqDiary } from "../types"

export const DiaryStateContext = createContext<Diary[]>([])

export const DiaryDispatchContext = createContext<{
  onCreate: ({ author, content, emotion }: ReqDiary) => void
  onRemove: (targetId: number) => void
  onEdit: (targetId: number, newContent: string) => void
} | null>(null)