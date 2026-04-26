package com.example.caching_patterns.controller;

import com.example.caching_patterns.User;
import com.example.caching_patterns.services.CacheAsideService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequiredArgsConstructor
public class CacheController {

    private final CacheAsideService cacheAsideService;

    @GetMapping("/api/cache-aside/users/{id}")
    public ResponseEntity<User> getCacheAside(@PathVariable Long id) {
        log.info("[API] GET /cache-aside/users/{}", id);
        return cacheAsideService.getUser(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/api/cache-aside/users/{id}/mutex")
    public ResponseEntity<User> getCacheAsideWithMutex(@PathVariable Long id) {
        log.info("[API] GET /cache-aside/users/{}/mutex", id);
        return cacheAsideService.getUserWithMutex(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/api/cache-aside/users")
    public ResponseEntity<User> createCacheAside(@RequestBody User user) {
        log.info("[API] POST /cache-aside/users");
        return ResponseEntity.ok(cacheAsideService.createUser(user));
    }

    @PutMapping("/api/cache-aside/users/{id}")
    public ResponseEntity<User> updateCacheAside(
            @PathVariable Long id, @RequestBody User user) {
        log.info("[API] PUT /cache-aside/users/{}", id);
        return ResponseEntity.ok(cacheAsideService.updateUser(id, user));
    }

    @DeleteMapping("/api/cache-aside/users/{id}")
    public ResponseEntity<Void> deleteCacheAside(@PathVariable Long id) {
        cacheAsideService.deleteUser(id);
        return ResponseEntity.noContent().build();
    }

}
