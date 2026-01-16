package com.ssu.commerce.book.grpc;

import com.ssu.commerce.book.dto.BookDto;
import com.ssu.commerce.book.dto.param.GetBookListParamDto;
import com.ssu.commerce.book.service.BookService;
import com.ssu.commerce.order.grpc.BookListRequest;
import com.ssu.commerce.order.grpc.BookListResponse;
import com.ssu.commerce.order.grpc.GetBookListGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.util.ArrayList;
import java.util.List;

@GrpcService
@Slf4j
public class GrpcServerService extends GetBookListGrpc.GetBookListImplBase {

    @Autowired
    private BookService bookService;

    @Override
    public void getBookList(BookListRequest request, StreamObserver<BookListResponse> responseObserver) {

        Page<BookDto> dto = bookService.getBookList(
                GetBookListParamDto.builder()
                        .pageable(
                                PageRequest.of(request.getPageNumber(), request.getPageSize())
                        ).build()
        );

        List<com.ssu.commerce.order.grpc.BookDto> grpcDtoList = new ArrayList<>();

        for (BookDto list : dto.getContent()) {
            grpcDtoList.add(
                    com.ssu.commerce.order.grpc.BookDto.newBuilder()
                            .setId(list.getId())
                            .setTitle(list.getTitle())
                            .setContent(list.getContent())
                            .setWriter(list.getWriter())
                            .setPrice(list.getPrice())
                            .setIsbn(list.getIsbn())
                            .setMaxBorrowDay(list.getMaxBorrowDay())
                            .setCategoryId(list.getCategoryId())
                            .build()
            );
        }

        responseObserver.onNext(
                BookListResponse.newBuilder()
                        .addAllBookList(
                                grpcDtoList
                        )
                .build()
        );
        responseObserver.onCompleted();
    }
}