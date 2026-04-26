package com.example.caching_patterns;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 *
 * why coes it matter for caching?
 *  - This is slow, athoritative data source.
 *  - every cache pattern we implement is trying to reduce how often we call methods on repository
 *
 *  Cache = fast bookshelf in our home/office
 *  This Repository = slow storage room in basement
 *
 *  Our goal of all cache patterns: go to the basement as rarely as possible,
 *  while keeping our bookshelf accurate
 *
 */

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByEmail(String username) ;
}
