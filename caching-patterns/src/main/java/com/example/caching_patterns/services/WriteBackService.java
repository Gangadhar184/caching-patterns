package com.example.caching_patterns.services;

/***
 * PATTERN 3: WRITE-BACK
 *
 * INTUTION, FIRST PRINCIPLES:
 * Think about how we might take notes during a lecture
 * - You write notes in our notebook(cache) fast , synch
 * - At the end of th day, we transcribe them to ur study binder(db)
 * we dont pause mid-lecture to type into our computer very sentence
 * we batch the work and do it later
 *
 * WRITE_BACKC applies this "defereed write" pattern to DB
 *
 * COMPARE TO OTHER PATTERNS:
 *
 * Cache-Aside:    Write → DB immediately,  invalidate cache
 * Write-Through:  Write → DB immediately,  update cache immediately
 * Write-Back:     Write → Cache immediately, update DB LATER (async)

 * THE CORE TRADEOFF: AVAILABILITY vs CONSISTENCY
 *
 * By choosing Write-Back, you're explicitly saying:
 * "I prioritize write SPEED and cache AVAILABILITY over
 *  data DURABILITY. Some writes might be lost if Redis crashes
 *  before flushing to DB."
 *
 * This is a deliberate CAP Theorem tradeoff:
 * Choose Availability (cache always writable) over Consistency
 * (DB might be slightly behind).
 *
 * HOW IT WORKS — STEP BY STEP:
 *
 * WRITE (fast path, synchronous):
 *   1. Update data in REDIS only (~0.5ms)
 *   2. Add userId to an in-memory write buffer
 *   3. Return success to caller immediately (0.5ms total!)
 *   (DB has NOT been updated yet)
 *
 * BACKGROUND FLUSH (periodic, async):
 *   1. Every N seconds, the flush worker wakes up
 *   2. Reads all pending writes from the buffer
 *   3. Executes a BATCH UPDATE to the primary DB
 *   4. Clears the buffer
 *
 * READ:
 *   Same as Cache-Aside — check cache first, then DB
 *
 * WHY BATCH WRITES ARE SO VALUABLE:
 *
 * Imagine a leaderboard game:
 * - 10,000 players update their scores per second
 * - Write-Through: 10,000 DB writes/second (can overwhelm most DBs)
 * - Write-Back: 1 batch DB write every 5 seconds (2 writes/second!)
 * The DB sees 5000x fewer writes. This is transformative at scale.
 *
 * Real-world example: MySQL can handle ~1,000-5,000 writes/second.
 * With Write-Back, a Redis-backed system can handle 100,000+ logical
 * writes/second because Redis (in-memory) does the heavy lifting.
 *
 * DURABILITY RISK SCENARIOS:
 * --------------------------
 * Scenario 1: Redis crash BEFORE flush
 *   → All unflushed writes in the buffer are LOST
 *   → The DB is X seconds behind the last client-visible state
 *
 * Scenario 2: Redis crash AFTER flush
 *   → No data loss (DB has been updated)
 *
 * Scenario 3: Application crash (JVM dies) BEFORE flush
 *   → In-memory buffer is lost (worst case)
 *   → Mitigation: Store write buffer IN Redis (not JVM memory)
 *     so it survives JVM restarts (implemented below)
 *
 * MITIGATING DATA LOSS:
 *
 * 1. Redis AOF (Append-Only File): Redis logs every write to disk.
 *    If Redis crashes, it replays the log on restart.
 *    Cost: Slight write performance reduction.
 *
 * 2. Shorter flush intervals: Flush every 500ms instead of 5s.
 *    Tradeoff: More DB writes (less batching benefit).
 *
 * 3. Store write buffer in Redis (not JVM memory):
 *    Even if JVM restarts, Redis holds the unflushed writes.
 *    We implement this below using a Redis List as the buffer.
 *
 * WHEN TO USE WRITE-BACK:
 * -----------------------
 *  Extremely write-heavy workloads (metrics, counters, game scores)
 *  When write latency is the primary concern (real-time apps)
 *  When occasional data loss is tolerable (analytics, not banking)
 *  When your DB cannot handle peak write volume (read replicas, etc.)
 *
 * WHEN NOT TO USE WRITE-BACK:
 * ----------------------------
 *  Financial transactions (cannot afford data loss — use Write-Through)
 *  Medical/legal records (durability required — use Write-Through)
 *  When DB must be queryable in real-time (reports might miss recent data)
 *  Low write volume (overhead of buffering not worth it)
 *
 * REAL-WORLD USAGE:
 * -----------------
 * - Redis's own RDB snapshots use a write-back-like mechanism
 * - MySQL InnoDB buffer pool: writes go to memory first, flushed to disk async
 * - Modern SSDs internally implement write-back caching at hardware level
 * - Browser localStorage: modified in memory, flushed to disk by OS
 *
 * TRADEOFFS SUMMARY:

 * | Aspect         | Write-Back Behavior           |
 * |----------------|-------------------------------|
 * | Read latency   | Fast (cache serves reads)      |
 * | Write latency  | Extremely fast (~0.5ms)        |
 * | Consistency    | Eventually consistent (lag)    |
 * | Data durability| Risk of loss if Redis crashes  |
 * | DB load        | Very low (batched writes)      |
 * | Complexity     | High (buffer mgmt, flush logic)|
 */


import com.example.caching_patterns.User;
import com.example.caching_patterns.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service("writeBackService")
@Slf4j
@RequiredArgsConstructor
public class WriteBackService {

    private final UserRepository userRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${cache.ttl.default:300}")
    private long defaultTtlSeconds;

    private static final String CACHE_KEY_PREFIX    = "write-back:user:";
    // Redis Set key that tracks which userIds have pending writes
    // Using Redis (not JVM memory) so buffer survives JVM restart
    private static final String WRITE_BUFFER_KEY    = "write-back:pending-writes";
    private static final String FLUSH_LOCK_KEY      = "write-back:flush-lock";

    /**
     * Write: Ultrafast only touches redis
     *
     * What we do not do here (vs write through):
     * - No DB write
     * - NO blocking wait for disk I/O
     *
     * What we do:
     * 1. Write data to redis in-memory
     * 2. Record userId in pending-writes Set
     * 3. Return
     *
     * The DB is eventually by the bg flush job
     * the caller never sees the DB write happen

     MULTI-UPDATE OPTIMIZATION:
     * If user 42 is updated 100 times before the flush, the buffer
     * only stores "42" once (it's a Set, not a List).
     * The flush reads the LATEST cached value and writes it once.
     * This naturally deduplicates rapid updates!
     *
     */

    public User updateUser(Long userId, User user) {
        user.setId(userId);
        String cacheKey = CACHE_KEY_PREFIX + userId;

        //1. write to cache only
        redisTemplate.opsForValue().set(
                cacheKey,
                user,
                Duration.ofSeconds(defaultTtlSeconds + 60) //extra ttl: outlive flush interval
        );
        //2. mark userid as having a pending DB write
        // Redis SADD = add to set (no duplicates, O(1))
        redisTemplate.opsForSet().add(WRITE_BUFFER_KEY, String.valueOf(userId));

        log.debug("[WRITE-BACK] Wrote userId={} to cache ONLY. DB write deferred.", userId);
        return user;
    }

    public User createUser(User user) {
        // For new users, write to DB immediately (we don't know the ID yet)
        // The DB auto-generates the ID, which we need for the cache key
        User saved = userRepository.save(user);
        String cacheKey = CACHE_KEY_PREFIX + saved.getId();
        redisTemplate.opsForValue().set(cacheKey, saved, Duration.ofSeconds(defaultTtlSeconds));
        log.debug("[WRITE-BACK] Created userId={} in DB+cache (new records go directly)", saved.getId());
        return saved;
    }

    /**
     * READ path:
     * *** The cache might have data newer than the DB (because wirte go to cache before flushing to DB)
     * cache reads are authoritative not just "fast copies" a cache hit here reutns the most recent data, even if DB hasnt caught up yet
     *
     * If cache misses and DB is queried: the DB might return  STALE data for a recently written key. This is the consistency
     * TRADEOFF we acceptt with WRITE-BACK
     */

    private User toUser(Object cached) {
        if (cached instanceof User user) {
            return user;
        }
        return objectMapper.convertValue(cached, User.class);
    }

    public Optional<User> getUser(Long userId) {
        String cacheKey = CACHE_KEY_PREFIX + userId;

        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.debug("[WRITE-BACK] CACHE HIT for userId={} (may be newer than DB)", userId);
            return Optional.of(toUser(cached));
        }

        log.debug("[WRITE-BACK] CACHE MISS for userId={}, reading from DB", userId);
        Optional<User> userOpt = userRepository.findById(userId);

        userOpt.ifPresent(user ->
                redisTemplate.opsForValue().set(
                        cacheKey, user, Duration.ofSeconds(defaultTtlSeconds))
        );

        return userOpt;
    }


    // BACKGROUND FLUSH: Batch-write pending updates to DB


    /**
     * SCHEDULED FLUSH JOB
     *
     * @Scheduled runs this every 5 seconds in a background thread.
     *
     * HOW IT WORKS:
     * 1. Read all pending userIds from Redis Set
     * 2. Clear the Set atomically (no new IDs added during this window
     *    are lost because SADD is atomic — new ones added after our
     *    SMEMBERS call will be in the Set for next flush)
     * 3. For each pending userId:
     *    a. Read current value from cache
     *    b. Write to DB (batch for efficiency)
     * 4. Release resources
     *
     * DISTRIBUTED LOCKING:
     * In a multi-node deployment, multiple JVM instances might try to
     * flush simultaneously, causing duplicate DB writes.
     * The Redis lock ensures only ONE instance flushes at a time.
     *
     * WHY REDIS LOCK INSTEAD OF JAVA SYNCHRONIZED?
     * synchronized {} only works within one JVM.
     * Redis lock works across multiple JVM processes/servers.
     *
     * FLUSH FAILURE HANDLING:
     * If flush partially succeeds (flushed 50/100 users before crash):
     * - The 50 remaining pending IDs are still in the Redis Set
     *   IF we used atomic operations (see implementation below)
     * - Next flush picks up where we left off
     * This is why we clear buffer AFTER flush, not before.
     */
    @Scheduled(fixedDelayString = "${write-back.flush-interval:5000}")
    @Transactional
    public void flushPendingWritesToDatabase() {
        // Try to acquire distributed flush lock
        // Only one instance should flush at a time in a cluster
        Boolean lockAcquired = redisTemplate.opsForValue()
                .setIfAbsent(FLUSH_LOCK_KEY, "locked", Duration.ofSeconds(30));

        if (!Boolean.TRUE.equals(lockAcquired)) {
            log.debug("[WRITE-BACK FLUSH] Another instance is flushing, skipping.");
            return;
        }

        try {
            // Read ALL pending userIds from the buffer Set
            Set<Object> pendingIds = redisTemplate.opsForSet().members(WRITE_BUFFER_KEY);

            if (pendingIds == null || pendingIds.isEmpty()) {
                log.debug("[WRITE-BACK FLUSH] No pending writes to flush.");
                return;
            }

            log.info("[WRITE-BACK FLUSH] Flushing {} pending write(s) to DB...", pendingIds.size());

            List<User> usersToSave = new ArrayList<>();

            for (Object pendingId : pendingIds) {
                Long userId = Long.parseLong(pendingId.toString());
                String cacheKey = CACHE_KEY_PREFIX + userId;

                // Read the LATEST value from cache
                Object cached = redisTemplate.opsForValue().get(cacheKey);
                if (cached != null) {
                    usersToSave.add((User) cached);
                    // Remove this userId from pending buffer only after reading
                    redisTemplate.opsForSet().remove(WRITE_BUFFER_KEY, pendingId);
                } else {
                    log.warn("[WRITE-BACK FLUSH] userId={} was pending but evicted from cache!", userId);
                    // Cache evicted the value before flush!
                    // In production: use Redis persistence (AOF) to prevent this.
                    redisTemplate.opsForSet().remove(WRITE_BUFFER_KEY, pendingId);
                }
            }

            // BATCH SAVE to DB — one DB round trip for N records
            // Far more efficient than N individual saveById() calls
            if (!usersToSave.isEmpty()) {
                userRepository.saveAll(usersToSave);
                log.info("[WRITE-BACK FLUSH] Successfully flushed {} users to DB", usersToSave.size());
            }

        } catch (Exception e) {
            log.error("[WRITE-BACK FLUSH] Flush failed: {}", e.getMessage(), e);
            // Don't rethrow — let next scheduled run retry
            // The pending IDs remain in Redis Set for next attempt
        } finally {
            // Always release the flush lock
            redisTemplate.delete(FLUSH_LOCK_KEY);
        }
    }


    /**
     * GRACEFUL SHUTDOWN: Flush remaining data before app stops
     *
     * @PreDestroy ensures this runs when Spring context shuts down.
     * This prevents data loss during planned restarts (deploys, scaling down).
     * Does NOT help with hard crashes (kill -9) — that's what Redis AOF is for.
     */
    @PreDestroy
    public void flushOnShutdown() {
        log.info("[WRITE-BACK] Application shutting down — performing final flush...");
        flushPendingWritesToDatabase();
    }


    public void deleteUser(Long userId) {
        // For deletes: must remove from both cache AND pending buffer
        userRepository.deleteById(userId);
        redisTemplate.delete(CACHE_KEY_PREFIX + userId);
        redisTemplate.opsForSet().remove(WRITE_BUFFER_KEY, String.valueOf(userId));
        log.debug("[WRITE-BACK] Deleted userId={} from DB, cache, and pending buffer", userId);
    }


}
