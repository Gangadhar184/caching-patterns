package com.example.caching_patterns;

/**
 * I took user class to understand: primaryKey userId which maps cleanly to a redis cache key pattern : "user: {id}"
 * User data is READ far more than it is WRITTEN, making it good for caching technique
 *
 * - Think of user table as a library card catlog(slow, authoritative, persisten) and
 *   cache as sticky notes on our desk - fast to read, but need to be thrown awy when the catalog entry changes
 *
 */

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

@Entity
@Table(name = "users")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String email;

    private String department;
    private Integer age;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;
}