package com.example.caching_patterns.controller;

import com.example.caching_patterns.User;
import com.example.caching_patterns.services.CacheAsideService;
import com.example.caching_patterns.services.WriteBackService;
import com.example.caching_patterns.services.WriteThroughService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
@RequiredArgsConstructor
public class CacheController {

    private final CacheAsideService cacheAsideService;
    private final WriteThroughService writeThroughService;
    private final WriteBackService writeBackService;

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


    // =========================================================
    // PATTERN 2: WRITE-THROUGH
    // =========================================================

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

    // =========================================================
    // PATTERN 3: WRITE-BACK
    // =========================================================

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

}
