package com.example.caching_patterns.services;

import com.example.caching_patterns.User;
import com.example.caching_patterns.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.swing.text.html.Option;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * PATTERN 2: WRITE-THROUGH
 *
 * Intution From first principles:
 * Imagine a bank teller. Every time you mkae a deposit:
 * 1. The teller updates the paper ledger(database)
 * 2. The teller also update the digital screen(cache)
 * Unlike cache-aside(where cache is populated lazily on read)
 * WRite-Through populates cache eargely on WRITE
 *
 * PRINCIPLE:
 * - "Every Write goes through the cache layer first"
 * This means after a write the cache is always warm for that key.
 * - The 3 hop cache miss problme on reads is eleminated for written keys.
 *
 * CacheAsie = Fill shelf when someome asks for a book
 * WriteThrough = EVery time a new book arrives , put a copy on shelf and store original in library simultaneously
 *
 * HOW IT WORKS :
 * WRITE (Critical part):
 *  1. Client calls updateUser(7, data)
 *  2. Write to PRIMARY DB(synchronous)
 *  3. WRite same data to cache(synchronous)
 *  4. Return success to client
 *  Total write latency = DB write time + cache write time
 *
 * READ(trivial)
 * 1. Check cache -> almost always hit for recently written data
 * 2. One miss (first-ever read or after crash): fall back to DB
 *
 * THE FUNDAMENTAL TRADEOFF:
 * Write-Through Trades write latency for read latency:
 * - Writes: Slowers(2 writes vs 1)-we pay the cache write coset
 * - Reads: Faster, no cold start misses for written keys
 *
 * QUESTION: Does caching writes even make sense?
 *   ANSWER: Yes! If your write/read ratio is, say, 1:100 (you write
 *  once but read 100 times), the extra write cost amortizes over
 *  100 fast reads. Net performance is a win.
 *
 ASYNC OPTIMIZATION:
 * -------------------
 * The sequential version writes DB then Cache (total = DB_time + Cache_time).
 * The async version writes DB, then fires Cache write on a background thread.
 * The caller only waits for DB write time — cache write happens in parallel.
 *
 * Risk: If cache write fails silently, next read hits a miss.
 * That's acceptable — the read will re-populate from DB.
 *
 * CONSISTENCY GUARANTEE:
 * ----------------------
 * Write-Through provides STRONG consistency for WRITES (both DB and
 * cache are always in sync after a write).
 * But reads STILL have eventual consistency if TTL expires in between.
 *
 * WHEN TO USE WRITE-THROUGH:
 * --------------------------
 *  Read-heavy after write (product pages refreshed after edit)
 *  When cold-start/first-read latency matters (SLA-sensitive reads)
 *  Systems where you know data will be read after every write
 *  Session stores (created once, read many times)
 *
 * WHEN NOT TO USE WRITE-THROUGH:
 * --------------------------------
 *  Write-heavy workloads (pays write penalty on every write)
 *  When most written data is NEVER read (wastes cache space)
 *  Time-series or log data (write-only, no read benefit)
 *
 * TRADEOFFS SUMMARY:

 * | Aspect         | Write-Through Behavior        |
 * |----------------|-------------------------------|
 * | Read latency   | Very fast (always warm)        |
 * | Write latency  | Slower (2x writes)            |
 * | Consistency    | Strong on write               |
 * | Cache warmth   | Eager (always warm after write)|
 * | Resilience     | Good (DB is still source of truth) |
 * | Complexity     | Low-Medium                    |
 */


@Service("writeThroughService")
@Slf4j
@RequiredArgsConstructor
public class WriteThroughService {
    private final UserRepository userRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${cache.ttl.default:300}")
    private long defaultTtlSeconds;
    private static final String CACHE_KEY_PREFIX = "write-through:user:";

    // WRITE: SYNC WRITE_THROUGH
    /**
     * Both writes happen sequentially in the same thread
     * the caller waits for Both to complete
     * Exceution timelin:
     * t=0ms:  DB write starts
     * t=5ms:  DB write completes
     * t=5ms:  Cache write starts
     * t=6ms:  Cache write completes → return to caller
     * Total:  ~6ms
     *
     * After this call:
     * - DB has latest data
     * - Cache has latest data
     * - Next read is guaranted cache hit
     */

    public User createOrUpdateUserSync(User user) {
        log.debug("[WRITE-THROUGH SYNC] Writing userId={} to DB...", user.getId());

        //1. write to primary db(authoritative source)
        User savedUser = userRepository.save(user);
        //2. Write same data to cache synch
        //to ensure cache is warm immediately after write
        String cacheKey = CACHE_KEY_PREFIX + savedUser.getCreatedAt();
        redisTemplate.opsForValue().set(cacheKey, savedUser, Duration.ofSeconds(defaultTtlSeconds));

        log.debug("[WRITE-TRHOUGH SYCN] Wrote userId={} to DB + cache", savedUser.getId());
        return savedUser;
    }

    //WRITE: ASYNCH Write-Through (optimize)
    /**
     * DB write happens on the callers thread(blocking, critical)
     * Cache writes is dispatched to a background thread(non-blocking)
     * Execution timeline:
     * t=0ms:  DB write starts (caller thread)
     * t=5ms:  DB write completes → return to caller
     *         Cache write starts (background thread, parallel)
     * t=6ms:  Cache write completes (background thread, silently)
     * Total caller wait: ~5ms (only DB time, not DB + cache)
     * CONSISTENCY RISK:
     * Between t=5ms and t=6ms, there's a brief window where:
     * - DB has new data
     * - Cache has old data (or no data)
     * A read in this 1ms window gets stale data.
     * For most use cases, this is acceptable.
     *
     * FAILURE RISK:
     * If the background thread fails/crashes between t=5ms and t=6ms:
     * - DB has correct data (safe)
     * - Cache is stale or cold
     * Next read hits DB miss and re-populates. Self-healing!
     *
     * USE THIS WHEN: Write SLA is tight and 1ms cache write matters.
     * DON'T USE WHEN: Reads immediately after write must be consistent
     */
    public User createOrUpdateUserAsync(User user) {
        log.debug("[WRITE-THROUGH ASYNC] writing userId={} to DB...", user.getId());
        //1 synch db write - caller waits for this
        User savedUser = userRepository.save(user);
        //2/ async cache write -calller doesnt not wait ,
        // completableFutre.runAsync() spawns a new thread from the common forkjoinpool and returns immdiately to caller
        final User userToCache = savedUser;
        final String cacheKey = CACHE_KEY_PREFIX + savedUser.getId();
        CompletableFuture.runAsync(()-> {
            try {
                redisTemplate.opsForValue().set(
                        cacheKey, userToCache, Duration.ofSeconds(defaultTtlSeconds)
                );
                log.debug("[WRITE-THROUGH ASYNC] Background cache write done for userId={}",
                        userToCache.getId());
            }catch (Exception e){
                // imp: Don't let background thread failure affect the caller
                // Log it for monitoring, but swallow the exception
                log.warn("[WRITE-THROUGH ASYNC] Background cache write FAILED for userId={}: {}",
                        userToCache.getId(), e.getMessage());
            }
        });
        // Return immediately — don't wait for cache
        log.debug("[WRITE-THROUGH ASYNC] Returned to caller after DB write only");
        return savedUser;
    }

    /**
     *
     * READ : Cache-first with DB fallback
     *
     * READ FLOW:
     * Because Write-Through keeps cache warm, most reads are cache hits.
     * The fallback (cache miss → DB) handles:
     * 1. First-ever read of a key (before any write has occurred)
     * 2. Post-crash recovery (Redis restarted, cache cold)
     * 3. TTL expiry
     *
     * On a miss, we re-populate the cache (effectively switching to
     * Cache-Aside behavior temporarily until the next write).
     */

    public Optional<User> getUser(Long userId) {
        String cacheKey = CACHE_KEY_PREFIX + userId;
        //1. check cache
        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.debug("[WRITE_THROUGH] cache hit for userId={}, ", userId);
            return Optional.of(toUser(cached));
        }
        // 2: Cache miss (shouldn't happen often in write-through)
        log.debug("[WRITE-THROUGH] CACHE MISS for userId={} (unexpected — checking DB)", userId);
        Optional<User> userOpt = userRepository.findById(userId);

        // Re-populate cache if found (recovery path)
        userOpt.ifPresent(user ->
                redisTemplate.opsForValue().set(
                        cacheKey, user, Duration.ofSeconds(defaultTtlSeconds))
        );

        return userOpt;
    }

    private User toUser(Object cached) {
        if (cached instanceof User user) {
            return user;
        }
        return objectMapper.convertValue(cached, User.class);
    }


    //remove from both db and cache
    public void deleteUser(Long userId) {
        userRepository.deleteById(userId);
        redisTemplate.delete(CACHE_KEY_PREFIX + userId);
        log.debug("[WRITE-THROUGH] Deleted userId={} from DB and cache", userId);
    }


}
