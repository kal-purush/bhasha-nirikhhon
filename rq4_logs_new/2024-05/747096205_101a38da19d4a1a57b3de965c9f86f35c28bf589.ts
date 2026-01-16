import * as z from 'zod'
import { addCourseValidation } from '../addCourse/validationSchema'

export const addCourseData = (data: z.infer<typeof addCourseValidation>) => {
  const tagsUa = [data.tagsUa1, data.tagsUa2, data.tagsUa3, data.tagsUa4].filter(
    (item) => item !== ''
  )
  const tagsEn = [data.tagsEn1, data.tagsEn2, data.tagsEn3, data.tagsEn4].filter(
    (item) => item !== ''
  )
  const themesUa = [
    data.themesUa1,
    data.themesUa2,
    data.themesUa3,
    data.themesUa4,
    data.themesUa5,
    data.themesUa6,
    data.themesUa7,
    data.themesUa8,
    data.themesUa9,
    data.themesUa10,
    data.themesUa11,
    data.themesUa12,
    data.themesUa13,
    data.themesUa14,
    data.themesUa15,
    data.themesUa16,
    data.themesUa17,
    data.themesUa18
  ].filter((item) => item !== '')
  const themesEn = [
    data.themesEn1,
    data.themesEn2,
    data.themesEn3,
    data.themesEn4,
    data.themesEn5,
    data.themesEn6,
    data.themesEn7,
    data.themesEn8,
    data.themesEn9,
    data.themesEn10,
    data.themesEn11,
    data.themesEn12,
    data.themesEn13,
    data.themesEn14,
    data.themesEn15,
    data.themesEn16,
    data.themesEn17,
    data.themesEn18
  ].filter((item) => item !== '')
  const teacherIds = [data.teacherId.value]
  const fagUa = [
    { questionUa: data.faqUa1, answerUa: data.answerUa1 },
    { questionUa: data.faqUa2, answerUa: data.answerUa2 },
    { questionUa: data.faqUa3, answerUa: data.answerUa3 },
    { questionUa: data.faqUa4, answerUa: data.answerUa4 },
    { questionUa: data.faqUa5, answerUa: data.answerUa5 }
  ].filter(({ questionUa, answerUa }) => questionUa !== '' && answerUa !== '')
  const fagEn = [
    { questionEn: data.faqEn1, answerEn: data.answerEn1 },
    { questionEn: data.faqEn2, answerEn: data.answerEn2 },
    { questionEn: data.faqEn3, answerEn: data.answerEn3 },
    { questionEn: data.faqEn4, answerEn: data.answerEn4 },
    { questionEn: data.faqEn5, answerEn: data.answerEn5 }
  ].filter(({ questionEn, answerEn }) => questionEn !== '' && answerEn !== '')
  return {
    title: data.title,
    color: data.color,
    descriptionUa: data.descriptionUa,
    descriptionEn: data.descriptionEn,
    tagsUa: tagsUa,
    tagsEn: tagsEn,
    courseDescriptionUa1: data.courseDescriptionUa1,
    courseDescriptionEn1: data.courseDescriptionEn1,
    startDateUa: data.startDateUa,
    startDateEn: data.startDateEn,
    courseDurationUa: data.courseDurationUa,
    courseDurationEn: data.courseDurationEn,
    priceUa: data.priceUa,
    priceEn: data.priceEn,
    fullpriceUa: data.fullpriceUa,
    fullpriceEn: data.fullpriceEn,
    numberOfPlacesUa: data.numberOfPlacesUa,
    numberOfPlacesEn: data.numberOfPlacesEn,
    experienceUa: data.experienceUa,
    experienceEn: data.experienceEn,
    languageLevelUa: data.languageLevelUa,
    languageLevelEn: data.languageLevelEn,
    timeForLearningUa: data.timeForLearningUa,
    timeForLearningEn: data.timeForLearningEn,
    forWhomTitleUa1: data.forWhomTitleUa1,
    forWhomTitleUa2: data.forWhomTitleUa2,
    forWhomDescriptionUa1: data.forWhomDescriptionUa1,
    forWhomDescriptionUa2: data.forWhomDescriptionUa2,
    forWhomTitleEn1: data.forWhomTitleEn1,
    forWhomTitleEn2: data.forWhomTitleEn2,
    forWhomDescriptionEn1: data.forWhomDescriptionEn1,
    forWhomDescriptionEn2: data.forWhomDescriptionEn2,
    themeTitleUa: data.themeTitleUa,
    themeTitleEn: data.themeTitleEn,
    themesUa: themesUa,
    themesEn: themesEn,
    faqUa: fagUa,
    faqEn: fagEn,
    teacherIds: teacherIds
  }
}