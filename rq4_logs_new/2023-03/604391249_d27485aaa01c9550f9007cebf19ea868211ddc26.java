package com.abneco.delivery.user.mock;

import com.abneco.delivery.user.entity.Seller;
import com.abneco.delivery.user.repository.SellerRepository;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;

public class MockSellerRepositoryData implements SellerRepository {
    @Override
    public Optional<Seller> findByEmail(String email) {
        return Optional.empty();
    }

    @Override
    public List<Seller> findAll() {
        return null;
    }

    @Override
    public List<Seller> findAll(Sort sort) {
        return null;
    }

    @Override
    public Page<Seller> findAll(Pageable pageable) {
        return null;
    }

    @Override
    public List<Seller> findAllById(Iterable<String> iterable) {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void deleteById(String s) {

    }

    @Override
    public void delete(Seller seller) {

    }

    @Override
    public void deleteAll(Iterable<? extends Seller> iterable) {

    }

    @Override
    public void deleteAll() {

    }

    @Override
    public <S extends Seller> S save(S s) {
        return null;
    }

    @Override
    public <S extends Seller> List<S> saveAll(Iterable<S> iterable) {
        return null;
    }

    @Override
    public Optional<Seller> findById(String s) {
        return Optional.of(new Seller());
    }

    @Override
    public boolean existsById(String s) {
        return false;
    }

    @Override
    public void flush() {

    }

    @Override
    public <S extends Seller> S saveAndFlush(S s) {
        return null;
    }

    @Override
    public void deleteInBatch(Iterable<Seller> iterable) {

    }

    @Override
    public void deleteAllInBatch() {

    }

    @Override
    public Seller getOne(String s) {
        return null;
    }

    @Override
    public <S extends Seller> Optional<S> findOne(Example<S> example) {
        return Optional.empty();
    }

    @Override
    public <S extends Seller> List<S> findAll(Example<S> example) {
        return null;
    }

    @Override
    public <S extends Seller> List<S> findAll(Example<S> example, Sort sort) {
        return null;
    }

    @Override
    public <S extends Seller> Page<S> findAll(Example<S> example, Pageable pageable) {
        return null;
    }

    @Override
    public <S extends Seller> long count(Example<S> example) {
        return 0;
    }

    @Override
    public <S extends Seller> boolean exists(Example<S> example) {
        return false;
    }
}