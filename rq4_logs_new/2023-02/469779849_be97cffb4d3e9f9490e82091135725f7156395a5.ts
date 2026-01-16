
export type DraggableCourseChapter = {
  isChapter: boolean,
  chapterName?: string,
  children?: DraggableCourseChapter[]
  id?: string
}