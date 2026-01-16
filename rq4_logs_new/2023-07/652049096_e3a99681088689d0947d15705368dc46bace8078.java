package com.bahubba.bahubbabookclub.repository;

import com.bahubba.bahubbabookclub.model.entity.BookClubMembership;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface BookClubMembershipRepo extends JpaRepository<BookClubMembership, UUID> {
}