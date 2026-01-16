import {
  CLIENT_NOT_RECOMMEND,
  CLIENT_RECOMMEND,
  NORMAL,
  NOT_RECOMMEND,
  RECOMMEND,
} from "@/constants/recommand";
import axiosInstance from "./axiosInstance";
import { RatingQuestionId } from "@/pages/WriteReviewPage/WriteReviewPage.const";
import { convertSemesterToString } from "@/utils";

export const getRecentEvaluation = () => {
  return axiosInstance.get(`/record?take=4&type=recent`);
};
export const getLectureEachEvaluation = (
  lectureId: number,
  sectionId: number | null
) => {
  const params = {
    lectureId: lectureId,
    sectionId: sectionId,
    type: "evaluation",
  };

  return axiosInstance.get(`/record`, {
    params: params,
  });
};
//type이 user일때 유저의 강의평을 조회하는 구현이 되어있지 않음
export const postLectureEvaluation = (
  review: string,
  lectureId: number,
  sectionId: number | null,
  semesterId: number,
  year: string,
  recommend: number | null,
  ratings: { [key: number]: number | null }
) => {
  // 현재 추천이 1, 비추천이 0, 보통이 2로 서버에 저장되나, 비추천 0, 보통 1, 추천 2로 저장되어 있음. 따라서 보통과 추천을 바꿔줘야 함

  const fixedRecommend =
    recommend === CLIENT_NOT_RECOMMEND
      ? NOT_RECOMMEND
      : recommend === CLIENT_RECOMMEND
      ? RECOMMEND
      : NORMAL;

  const payload = {
    difficulty: ratings[RatingQuestionId.Difficulty],
    skill: ratings[RatingQuestionId.TeachingSkills],
    helpfulness: ratings[RatingQuestionId.ContentUsefulness],
    interest: ratings[RatingQuestionId.LectureEnjoyment],
    load: ratings[RatingQuestionId.AssignmentAmount],
    generosity: ratings[RatingQuestionId.GradeSatisfaction],
    review: review,
    lectureId: lectureId,
    sectionId: sectionId,
    semester: convertSemesterToString(semesterId),
    year: parseInt(year),
    recommendation: fixedRecommend,
  };

  return axiosInstance.post(`/record`, payload);
};

/**강의평의 좋아요를 남깁니다*/
export const postRecordLikeNum = (id: number) => {
  return axiosInstance.get(`/record/${id}/like`);
};
//질문 따로 좋아요의 number를 payload형식으로 db에 post를 보내지 않아도 되는가?
//api문서에는 별다른 내용이 나와있지 않아 가능할 것 같지만 그러면 좋아요의 수를 어떻게 처리하는지 모르겠다.
//db의 구조를 알아야할것으로 보인다.현재 docs에는 관련 내용이 업데이트 되어있지 않은 듯
/**강의평의 좋아요를 삭제합니다.*/
export const deleteRecordLikeNum = (id: number) => {
  return axiosInstance.delete(`/record/${id}/like`);
};