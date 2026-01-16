// 페이지 번호를 유지한 스토리 목록 페이지로 이동하는 함수
function goToStoryList() {
    const pageNumber = sessionStorage.getItem('currentPage') || 1;
    const currentURL = new URL(window.location.href);
    console.log(pageNumber);
    console.log(currentURL);

    // 스토리 ID(숫자) 제거
    const pathname = currentURL.pathname.replace(/\/\d+$/, '');

    currentURL.pathname = pathname;
    currentURL.searchParams.set('page', pageNumber);
    currentURL.searchParams.set('userId', userId);

    window.location.href = currentURL.toString();
}