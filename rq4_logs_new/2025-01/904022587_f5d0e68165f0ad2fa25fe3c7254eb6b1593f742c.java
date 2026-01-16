package com.green.paging.vo;

public class Pagination {

    private int 	totalRecordCount;     // 검색된 전체 데이터 수 
      									  // menu_id 에 해당하는 + 검색된
    private int 	totalPageCount;       // 전체 페이지 수
    private int 	startPage;            // 첫 페이지 번호
    private int 	endPage;              // 끝 페이지 번호
    private int 	limitStart;           // LIMIT 시작 위치
    private boolean existPrevPage;        // 이전 페이지 존재 여부
    private boolean existNextPage;        // 다음 페이지 존재 여부

    public Pagination(int totalRecordCount, SearchVo params) {
        if (totalRecordCount > 0) {
            this.totalRecordCount = totalRecordCount;
            calculation(params);
        }
    }

    private void calculation(SearchVo params) {

        // 전체 페이지 수 계산
        // dtotalPageCount = ((totalRecordCount - 1) / params.getRecordSize()) + 1;
    	totalPageCount = (int)Math.ceil((double)totalRecordCount / params.getRecordSize());

        // 현재 페이지 번호가 전체 페이지 수보다 큰 경우, 현재 페이지 번호에 전체 페이지 수 저장
        if (params.getPage() > totalPageCount) {
            params.setPage(totalPageCount);
        }

        // 첫 페이지 번호 계산
        startPage = ((params.getPage() - 1) / params.getPageSize()) * params.getPageSize() + 1;

        // 끝 페이지 번호 계산
        endPage = startPage + params.getPageSize() - 1;

        // 끝 페이지가 전체 페이지 수보다 큰 경우, 끝 페이지 전체 페이지 수 저장
        if (endPage > totalPageCount) {
            endPage = totalPageCount;
        }

        // LIMIT 시작 위치 계산
        limitStart = (params.getPage() - 1) * params.getRecordSize();

        // 이전 페이지 존재 여부 확인
        existPrevPage = startPage != 1;

        // 다음 페이지 존재 여부 확인
        existNextPage = (endPage * params.getRecordSize()) < totalRecordCount;
    }

}