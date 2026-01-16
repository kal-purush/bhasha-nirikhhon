package com.challenger.fridge.repository;

import com.challenger.fridge.domain.Cart;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CartRepository extends JpaRepository<Cart, Long> {

    @Query("select c from Cart c join fetch c.member where c.member.email = :email")
    Optional<Cart> findByMemberEmail(@Param("email") String email);

}