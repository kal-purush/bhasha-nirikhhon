package com.j30n.stoblyx.domain.port.in.book;

import com.j30n.stoblyx.application.dto.book.BookResponse;
import com.j30n.stoblyx.common.exception.book.BookNotFoundException;
import com.j30n.stoblyx.domain.model.book.BookId;

import java.util.List;

public interface FindBookUseCase {
    /**
     * ID로 책을 조회합니다.
     *
     * @param id 책 ID
     * @return 책 정보
     * @throws BookNotFoundException 해당 ID의 책을 찾을 수 없는 경우
     */
    BookResponse findById(BookId id);

    /**
     * 모든 책을 조회합니다.
     *
     * @return 책 목록
     */
    List<BookResponse> findAll();

    /**
     * ISBN으로 책을 조회합니다.
     *
     * @param isbn ISBN
     * @return 책 정보
     * @throws BookNotFoundException 해당 ISBN의 책을 찾을 수 없는 경우
     */
    BookResponse findByIsbn(String isbn);
}