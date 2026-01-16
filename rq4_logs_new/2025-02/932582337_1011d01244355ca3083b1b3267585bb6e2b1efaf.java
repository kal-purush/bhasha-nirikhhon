package com.j30n.stoblyx.adapter.in.web;

import com.j30n.stoblyx.application.dto.book.BookResponse;
import com.j30n.stoblyx.application.dto.book.RegisterBookCommand;
import com.j30n.stoblyx.application.dto.book.UpdateBookCommand;
import com.j30n.stoblyx.common.dto.ApiResponse;
import com.j30n.stoblyx.domain.model.book.BookId;
import com.j30n.stoblyx.domain.port.in.book.DeleteBookUseCase;
import com.j30n.stoblyx.domain.port.in.book.FindBookUseCase;
import com.j30n.stoblyx.domain.port.in.book.RegisterBookUseCase;
import com.j30n.stoblyx.domain.port.in.book.UpdateBookUseCase;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/books")
@RequiredArgsConstructor
public class BookController {
    private final RegisterBookUseCase registerBookUseCase;
    private final UpdateBookUseCase updateBookUseCase;
    private final FindBookUseCase findBookUseCase;
    private final DeleteBookUseCase deleteBookUseCase;

    @PostMapping
    public ResponseEntity<ApiResponse<BookResponse>> registerBook(@RequestBody RegisterBookCommand command) {
        try {
            BookResponse response = registerBookUseCase.registerBook(command);
            return ResponseEntity.ok(ApiResponse.success("책이 성공적으로 등록되었습니다.", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<BookResponse>> findBook(@PathVariable Long id) {
        try {
            BookResponse response = findBookUseCase.findById(new BookId(id));
            return ResponseEntity.ok(ApiResponse.success("책 조회에 성공했습니다.", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<BookResponse>>> findAllBooks() {
        try {
            List<BookResponse> response = findBookUseCase.findAll();
            return ResponseEntity.ok(ApiResponse.success("책 목록 조회에 성공했습니다.", response));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<Void>> updateBook(@PathVariable Long id, @RequestBody UpdateBookCommand command) {
        try {
            updateBookUseCase.updateBook(command);
            return ResponseEntity.ok(ApiResponse.success("책이 성공적으로 수정되었습니다.", null));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<Void>> deleteBook(@PathVariable Long id) {
        try {
            deleteBookUseCase.deleteBook(new BookId(id));
            return ResponseEntity.ok(ApiResponse.success("책이 성공적으로 삭제되었습니다.", null));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        }
    }
}