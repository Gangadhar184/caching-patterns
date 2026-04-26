package com.example.caching_patterns.services;

import com.example.caching_patterns.User;
import com.example.caching_patterns.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;

/**
 * Pattern 1: cache-aside (lazy loading/demand-fill)
 *
 * From first principles:
 *  - A personal bookshelf(cache) - fast to access, limited space
 *  - university library (database) - slow, but has everything
 *
 *  Cache- Aside says: "Dont prestock the shelf, wait until someone asks
 *  for book. If it's on your shelf -> hand it over. If not -> go to library,bring it back, put it on ur shelf
 *
 *  The KEY INSIGHT: The Application (not the cache) manages what goes into cache, The cache is "passive" - it doesnt know about database.
 *
 *  WHY WE USE PATTERN:
 *  1. We only cache data that's actually requested (no wasted space)
 *  2. If Redis goes down, the app still works( fallback to DB)
 *  3. Simple to implment and reason about
 *  4. Works well when read >> write.
 *
 *
 *  HOW IT WORKS:
 *  READ:
 *      1. App checks cache for key "user:7"
 *      2a. CACHE HIT: return data immediately( 1hop)
 *      2b. CACHE MISS:
 *          -> QUery primary DB (2nd hop)
 *          -> WRite result to cache with TTL (3rd Hop)
 *          -> Return data to called
 *
 *  WRITE:
 *      1. Write new data to primary DB
 *      2. DELETE (invalidate the cache key)
 *      WHY DELETE instead of UPDATE? Because its simpler and safer.
 *      The next read will repopulate the cache with fresh data.
 *      Updateing cache on write create complex race conditions.
 *
 *  THE CACHE MISS PENALTY (3-HOP PROBLEM):
 *  - On a cold start or TTL expirt, the first request pays:
 *          - Cache check (~0.5ms) + DB query (~5-50ms) + Cache write(~0.5ms)
 *  Subsequent requests: Cache check (~0.5ms) only
 *
 *  THUNDERING HERD/CACHE STAMPEDE PROBLEM:
 *  lets take a scenario: 1000 ures request "user:42" at the exact moment its TTL expires.
 *  All requests see a cache miss simultaenously
 *  al 100 fire DB queries. Your DB gets 1000 hits at once
 *
 *  Solution : TTL Jitter: add randomess to TTL so keys dont all expire at once
 *              MUTEX/Locking: Only one request fetches from DB oter wait
 *              Probabilistic early expiry: Refresh cache slights before TTL ends
 *
 *   WHEN TO USE CACHE-ASIDE:
 *   - Read heavy workloads
 *   - Data that can tolerate brief staeness
 *   - When u want cache resilience (redis down -> app still works)
 *   - when data access patterns are unpredictable
 *
 *    WHEN NOT TO USE CACHE-ASIDE:
 *  - Write-heavy workloads (too many invalidations → cache always cold)
 *  - When you need strong consistency (cache might serve stale data)
 *  - When cold-start performance is critical (first request always slow)
 *
 *  ------------------
 *  * | Aspect         | Cache-Aside Behavior         |
 *  * |----------------|------------------------------|
 *  * | Read latency   | Fast (after first miss)       |
 *  * | Write latency  | Fast (only hits DB)           |
 *  * | Consistency    | Eventually consistent         |
 *  * | Cache warmth   | Lazy (cold on startup)        |
 *  * | Resilience     | High (graceful degradation)   |
 *  * | Complexity     | Low                           |
 *
 */




@Service("cacheAsideService")
@RequiredArgsConstructor
@Slf4j
public class CacheAsideService {

    private final UserRepository userRepository;
    private final RedisTemplate<String, Object> redisTemplate;

    @Value("${cache.ttle.default:300}")
    private long defaultTtlSeconds;

    private static final String CACHE_KEY_PREFIX = "cache-aside:user";
    private static final String LOCK_KEY_PREFIX = "lock:user";
    private static final  long LOCK_TTL_SECONDS = 5;
    private final Random random = new Random();

    // READ: Cache-Aside GET

    /**
     * READ FLOW
     * 1. Build cache key
     *          - Convention : "{pattern}:{entity}:{id}" -> cache-aside:user:42
     *          - Namespacing prevents key collisions across patterns/servvices
     *
     * 2. Check Redis
     *   - redisTemplate.OpsForValue().get(key) -> O(1) hash lookup in redis
     *
     * 3. (HIT): deserialize JSON -> User object -> return
     *      - total latency : ~0.5ms
     *     MISS: QUery DB -> serialize -> write to Redis -> return
     *      - total latency : ~5-50ms
     */

    public Optional<User> getUser(Long userId) {
        String cacheKey = CACHE_KEY_PREFIX + userId;
        //check cache
        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.debug("[CACHE-ASIDE] CACHE HIT for userId={}", userId);
            return Optional.of((User) cached);
        }
        log.debug("[CACHE-ASIDE] CACHE MISS for userId={}, querying DB", userId);


        // 2. cache miss - query primary db
        Optional<User> userOpt = userRepository.findById(userId);
        //populate cache for next requesst
        //whhy ttl with jitter? = Jitter = base TTL + random(0..60s) prevents stampede
        // if 1000 uers are cache at sametime without jitter, they all expire simulatenlty -> thundering herd
        userOpt.ifPresent(user -> {
            long ttlWithJitter = defaultTtlSeconds + random.nextInt(60);
            redisTemplate.opsForValue().set(
                    cacheKey,user, Duration.ofSeconds(ttlWithJitter)
            );
            log.debug("[CACHE-ASIDE] cached userId={} with TTL={}S", userId, ttlWithJitter);
        });
        return userOpt;
    }



    // ADVANCED READ: Mutex to prevent Cache Stampede

    /**
     * CACHE STAMPEDE PREVENTION USING REDIS LOCK
     *
     * Problem: 1000 concurrent requests hit cache miss simultaneously.
     * Solution: Only the FIRST request acquires a lock and fetches from DB.
     *           The other 999 requests either:
     *           a) Spin-wait a bit and retry cache (lock contention)
     *           b) Return stale data if available
     *
     * SETNX (SET if Not eXists) is atomic in Redis.
     * This guarantees exactly one request wins the lock.
     *
     * NOTE: This trades latency for DB protection. Use when your DB
     * cannot handle concurrent stampedes (e.g., expensive queries).
     */

    public Optional<User> getUserWithMutex(Long userId) {
        String cacheKey = CACHE_KEY_PREFIX + "mutex:"  + userId;
        String lockKey = LOCK_KEY_PREFIX + userId;

        //try cache
        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return Optional.of((User) cached);
        }
        //try to acquire lock(SETNX = atomix "set" if absent"
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, "Locked", Duration.ofSeconds(LOCK_TTL_SECONDS));
        if(Boolean.TRUE.equals(acquired)) {
            //hold lock - fetch from DB and populate cache
            try {
                Optional<User> userOpt = userRepository.findById(userId);
                userOpt.ifPresent(user ->
                        redisTemplate.opsForValue().set(
                                cacheKey, user, Duration.ofSeconds(defaultTtlSeconds))
                );
                return userOpt;
            }finally {
                //always release lock here, failure here causes lock to expire via TTL anyway
                redisTemplate.delete(lockKey);
                log.debug("[CACHE-ASIDE] Released lock for userId={}", userId);
            }
        }else {
            //lock held by another thread - brief wait then retry from cache
            log.debug("[CACHE-ASIDE] lock contention for userId={}, retrying, ", userId);
            try {
                Thread.sleep(100);
            }catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            //retry
            Object retired = redisTemplate.opsForValue().get(cacheKey);
            return retired != null ? Optional.of((User) retired) : userRepository.findById(userId);
        }
    }


    //WRITE : Invalidate on Update
    // =========================================================
    // WRITE: Invalidate on Update
    // =========================================================

    /**
     * WRITE FLOW EXPLAINED:
     *
     * Why DELETE cache instead of UPDATE it?
     *
     * OPTION A (UPDATE cache on write):
     * DB write → Cache write → Done
     * Problem: Race condition! Thread A writes user, Thread B writes user
     * simultaneously. The cache might end up with Thread A's stale value
     * if Thread B's cache write arrived first but DB write arrived second.
     *
     * OPTION B (DELETE/INVALIDATE cache on write):
     * DB write → Delete cache key → Done
     * The NEXT read will fetch fresh data from DB and repopulate cache.
     * No race condition — deletion is idempotent.
     *
     * This is why Cache-Aside typically uses INVALIDATION, not UPDATE.
     *
     * EXCEPTION: If you need extremely low read latency after write
     * and can tolerate some inconsistency risk, you might update cache.
     * But invalidation is the safer default.
     */
    public User updateUser(Long userId, User updatedUser) {
        //1: Write to primary database (source of truth)
        updatedUser.setId(userId);
        User saved = userRepository.save(updatedUser);

        // 2: Invalidate (delete) cache key
        // Next read will re-populate from DB automatically
        String cacheKey = CACHE_KEY_PREFIX + userId;
        redisTemplate.delete(cacheKey);

        log.debug("[CACHE-ASIDE] Invalidated cache for userId={}", userId);
        return saved;
    }

    public User createUser(User user) {
        // New user → save to DB, no cache entry to invalidate
        return userRepository.save(user);
    }

    public void deleteUser(Long userId) {
        // Delete from DB AND cache
        userRepository.deleteById(userId);
        redisTemplate.delete(CACHE_KEY_PREFIX + userId);
        log.debug("[CACHE-ASIDE] Deleted user and cache for userId={}", userId);
    }


}
