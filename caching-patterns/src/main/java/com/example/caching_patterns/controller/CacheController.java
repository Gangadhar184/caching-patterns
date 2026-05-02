package com.example.caching_patterns.controller;

import com.example.caching_patterns.User;
import com.example.caching_patterns.services.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@Slf4j
@RequiredArgsConstructor
public class CacheController {

    private final CacheAsideService cacheAsideService;
    private final WriteThroughService writeThroughService;
    private final WriteBackService writeBackService;
    private final TtlExpirationService ttlExpirationService;
    private final CachePenetrationService cachePenetrationService;

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



    // PATTERN 2: WRITE-THROUGH


    @GetMapping("/api/write-through/users/{id}")
    public ResponseEntity<User> getWriteThrough(@PathVariable Long id) {
        log.info("[API] GET /write-through/users/{}", id);
        return writeThroughService.getUser(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/api/write-through/users/sync")
    public ResponseEntity<User> createWriteThroughSync(@RequestBody User user) {
        log.info("[API] POST /write-through/users/sync");
        return ResponseEntity.ok(writeThroughService.createOrUpdateUserSync(user));
    }

    @PostMapping("/api/write-through/users/async")
    public ResponseEntity<User> createWriteThroughAsync(@RequestBody User user) {
        log.info("[API] POST /write-through/users/async");
        return ResponseEntity.ok(writeThroughService.createOrUpdateUserAsync(user));
    }

    // PATTERN 3: WRITE-BACK

    @GetMapping("/api/write-back/users/{id}")
    public ResponseEntity<User> getWriteBack(@PathVariable Long id) {
        log.info("[API] GET /write-back/users/{}", id);
        return writeBackService.getUser(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/api/write-back/users")
    public ResponseEntity<User> createWriteBack(@RequestBody User user) {
        log.info("[API] POST /write-back/users");
        return ResponseEntity.ok(writeBackService.createUser(user));
    }

    @PutMapping("/api/write-back/users/{id}")
    public ResponseEntity<User> updateWriteBack(
            @PathVariable Long id, @RequestBody User user) {
        log.info("[API] PUT /write-back/users/{}", id);
        return ResponseEntity.ok(writeBackService.updateUser(id, user));
    }

    @PostMapping("/api/write-back/flush")
    public ResponseEntity<String> triggerWriteBackFlush() {
        log.info("[API] POST /write-back/flush (manual trigger)");
        writeBackService.flushPendingWritesToDatabase();
        return ResponseEntity.ok("Flush triggered successfully");
    }


    // STRATEGY 4: TTL EXPIRATION

    @PostMapping("/api/ttl/otp/{userId}")
    public ResponseEntity<String> createOtp(
            @PathVariable String userId,
            @RequestBody Map<String, String> body) {
        String code = body.getOrDefault("code", "000000");
        ttlExpirationService.storeOtp(userId, code);
        return ResponseEntity.ok("OTP stored for userId=" + userId);
    }

    @PostMapping("/api/ttl/otp/{userId}/validate")
    public ResponseEntity<String> validateOtp(
            @PathVariable String userId,
            @RequestBody Map<String, String> body) {
        String inputCode = body.getOrDefault("code", "");
        return ttlExpirationService.validateOtp(userId, inputCode)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.ok("EXPIRED_OR_NOT_FOUND"));
    }

    @PostMapping("/api/ttl/sessions/{sessionId}")
    public ResponseEntity<String> createSession(
            @PathVariable String sessionId,
            @RequestBody Map<String, Object> sessionData) {
        ttlExpirationService.createSession(sessionId, sessionData);
        return ResponseEntity.ok("Session created: " + sessionId);
    }

    @GetMapping("/api/ttl/sessions/{sessionId}")
    public ResponseEntity<?> getSession(@PathVariable String sessionId) {
        return ttlExpirationService.getSession(sessionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/api/ttl/debug/{key}")
    public ResponseEntity<Map<String, Object>> debugTtl(@PathVariable String key) {
        Long remainingTtl = ttlExpirationService.getRemainingTtl(key);
        boolean exists = ttlExpirationService.keyExists(key);
        return ResponseEntity.ok(Map.of(
                "key", key,
                "exists", exists,
                "remainingTtlSeconds", remainingTtl != null ? remainingTtl : -1
        ));
    }

    // =========================================================
    // PATTERN 5: CACHE PENETRATION PROTECTION
    // =========================================================

    @GetMapping("/api/penetration/null-cache/users/{id}")
    public ResponseEntity<User> getUserNullCache(@PathVariable Long id) {
        log.info("[API] GET /penetration/null-cache/users/{}", id);
        return cachePenetrationService.getUserWithNullCache(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/api/penetration/bloom/users/{id}")
    public ResponseEntity<User> getUserBloomFilter(@PathVariable Long id) {
        log.info("[API] GET /penetration/bloom/users/{}", id);
        return cachePenetrationService.getUserWithBloomFilter(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/api/penetration/bloom/users")
    public ResponseEntity<User> createUserBloomFilter(@RequestBody User user) {
        log.info("[API] POST /penetration/bloom/users");
        return ResponseEntity.ok(cachePenetrationService.createUserWithBloomFilter(user));
    }


}
