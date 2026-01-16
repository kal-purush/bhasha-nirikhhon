package ru.practicum.shareit.common.pagination;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public class CustomPageRequest implements Pageable {

    private final int offset;
    private final int size;
    private final Sort sort;

    private CustomPageRequest(int offset, int size, Sort sort) {
        this.offset = offset;
        this.size = size;
        this.sort = sort;
    }

    public static CustomPageRequest of(int offset, int size, Sort sort) {
        return new CustomPageRequest(offset, size, sort);
    }

    @Override
    public int getPageNumber() {
        return offset / size;
    }

    @Override
    public int getPageSize() {
        return size;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public Sort getSort() {
        return sort;
    }

    @Override
    public Pageable next() {
        return new CustomPageRequest(offset + size, size, sort);
    }

    @Override
    public Pageable previousOrFirst() {
        return hasPrevious() ? of(offset - size, size, sort) : first();
    }

    @Override
    public Pageable first() {
        return new CustomPageRequest(offset - getPageNumber() * getPageSize(), size, sort);
    }

    @Override
    public Pageable withPage(int pageNumber) {
        return CustomPageRequest.of((int) (getOffset() + pageNumber * getPageSize()), getPageSize(), getSort());
    }

    @Override
    public boolean hasPrevious() {
        return offset - size > 0;
    }
}