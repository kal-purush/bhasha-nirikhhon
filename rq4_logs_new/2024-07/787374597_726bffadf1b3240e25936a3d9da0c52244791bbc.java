package com.example.moreveiw.domain.project.constant;

public class ProjectResponseConstant {

    public static final String getProjectList = """
            {
              "projects": [
                {
                  "projectId": 1,
                  "roomId": "hash 값",
                  "name": "게시글 이름입니다.",
                  "thumbnailUrl": "프로젝트 썸네일 링크",
                  "createdAt": "2024-07-05T07:51:52.396Z"
                }
              ],
              "totalPage": 0,
              "totalElements": 0,
              "isFirst": true,
              "isLast": true
            }
            """;
}