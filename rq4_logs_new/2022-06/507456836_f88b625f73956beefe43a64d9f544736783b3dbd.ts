import request from "../utils/request";

export function findAll() {
  return request({
    url: `universities/year/2021/subject/history`,
    method: "get",
    baseURL: "http://127.0.0.1:3000/",
  });
}

export function recommend(subject: string, score: number) {
  return request({
    url: `/universities/subject/${subject}/score/${score}/recommend/100`,
    method: "get",
    baseURL: "http://127.0.0.1:3000/",
  });
}