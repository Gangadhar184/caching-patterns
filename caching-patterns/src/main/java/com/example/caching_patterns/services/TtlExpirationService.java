package com.example.caching_patterns.services;


/***
 * PATTERN 4: TTL Based cache expiration
 *
 * TTL (Time to Live) is the expiration date on a cached value
 *
 * WITHOUT TTL:
 * - Cached data lives forever
 * - Cache grows unboundely (leads to run out of memory)
 * - Stale data is served indefinitely (user sees wrong info forever)
 *
 * WITH TTL:
 * - After N secs, the key auto deltes from redis
 * - NExt request sees a cache miss -> fetches fresh data
 * - memory is reclaimed automatically
 *
 * REDIS TTL Mechanics

 * Redis uses "lazy expiry" + "active expiry":
 * - LAZY: When you access a key, Redis checks if it's expired.
 *   If yes, deletes it and returns nil. No TTL daemon needed.
 * - ACTIVE: Redis randomly samples ~20 expired keys every 100ms
 *   and deletes them. This prevents expired keys from accumulating.
 *
 * This means an expired key MIGHT live in Redis for a few extra
 * milliseconds if it's not accessed. Rarely matters in practice.
 *
 * THE 4 EXPIRATION STRATEGIES:
 *
 *
 * STRATEGY 1: FIXED TTL
 *
 * TTL is set once when the key is written. Never changes.
 * Example: r.setex("otp:12345", 300, "789456")
 * Use: OTPs, download links, session tokens, feature flags
 *
 * STRATEGY 2: SLIDING TTL (Idle Timeout)
 *
 * TTL resets to its original value on EVERY ACCESS.
 * Example: Users actively using the site get a session that never expires.
 *          Inactive users get logged out after N minutes of no activity.
 *
 * Implementation: Every cache GET also calls EXPIRE to reset TTL.
 * This is how AWS ElastiCache "Memcached" Idle Timeout works.
 * Web framework sessions (Spring Session) use this pattern.
 *
 * TRADEOFF: More Redis calls per read (GET + EXPIRE vs just GET).
 * But the benefit is sessions that survive active use while
 * cleaning up truly idle data.
 *
 * STRATEGY 3: TTL WITH JITTER
 *
 * Base TTL + Random(0..N) seconds.
 * Prevents the "Thundering Herd" / "Cache Stampede" problem.
 *
 * THUNDERING HERD DEEP DIVE:
 *
 * Imagine 50,000 product catalog entries all cached at 9:00 AM
 * with TTL = 300 seconds. At exactly 9:05 AM:
 * - ALL 50,000 keys expire simultaneously
 * - Any request for those products hits a miss
 * - All requests fire DB queries concurrently
 * - DB receives a massive spike of 50,000 queries at once
 * - DB overloads → latency spikes → cascading failures
 *
 * With jitter: TTL = 300 + random(0..60) seconds
 * - Keys expire spread across a 60-second window
 * - Instead of 50,000 simultaneous misses → ~833 misses/second
 * - DB load is 60x lower and smoothly distributed
 *
 * STRATEGY 4: PROBABILISTIC EARLY EXPIRY (PER Algorithm)

 * Proactively refreshes a cache entry BEFORE it expires.
 * Named after the research paper "Optimal Probabilistic Cache
 * Stampede Prevention via Early Expiration" (Vattani et al., 2015).
 *
 * INTUITION: Don't wait for the book to be returned before
 * ordering a new copy. Order it while you still have the old one.
 *
 * HOW PER WORKS:
 * probability_of_early_refresh = exp(-time_remaining / beta * fetch_time)
 * - When TTL is HIGH: probability is near zero → no early refresh
 * - As TTL approaches zero: probability increases → refresh triggered
 * - beta controls "eagerness" — higher beta = earlier refresh
 *
 * This is COMPLEX to implement but eliminates stampedes entirely
 * in systems where compute time to regenerate a value is significant.
 * Used in high-traffic caches (CDNs, Twitter timeline cache, etc.).
 *
 * WHEN TO USE EACH STRATEGY:
 * ---------------------------
 * Fixed TTL:     OTPs, download links, short-lived tokens
 * Sliding TTL:   User sessions, "recently active" indicators
 * Jitter TTL:    Catalog data, configuration, anything bulk-loaded
 * PER/Early exp: High-traffic keys that are expensive to regenerate

 */

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Service("ttlStrategyService")
@Slf4j
@RequiredArgsConstructor
public class TtlExpirationService {

    private final RedisTemplate<String, Object> redisTemplate;

    @Value("${cache.ttl.default:300}")
    private long defaultTtlSeconds;

    @Value("${cache.ttl.null-value:60}")
    private long nullValueTtlSeconds;

    @Value("${cache.ttl.session:1800}")
    private long sessionTtlSeconds;

    @Value("${cache.ttl.otp:300}")
    private long otpTtlSeconds;

    private final Random random = new Random();

    //FIXED TTL
    /**

     * The key expires at t = write_time + TTL.
     * Accessing the key does NOT reset the clock.
     *
     * Redis SETEX internals:
     * - SET key value (writes the value)
     * - EXPIREAT key (write_time + ttl) (sets absolute expiration time)
     * Both happen atomically.
     *
     * VISUALIZED:
     * t=0:   set "otp:12345" = "789456" EX 300
     * t=100: get "otp:12345" → "789456" (still valid, 200s remaining)
     * t=200: get "otp:12345" → "789456" (still valid, 100s remaining)
     * t=300: get "otp:12345" → nil (expired, even if never accessed)
     */

    public void storeOtp(String userId, String otp) {
        String key = "otp-" + userId ;
        //SETEX = set + expire in one atomic command
        redisTemplate.opsForValue().set(key, otp, Duration.ofSeconds(otpTtlSeconds));
        log.debug("[TTL-FIXED] Stored OTP for userId={} with TTL={}s", userId, otpTtlSeconds);
    }

    public Optional<String> validateOtp(String userId, String inputCode) {
        String key = "otp: " + userId;
        Object stored = redisTemplate.opsForValue().get(key);
        if (stored == null) {
            log.debug("[TTL-FIXED] OTP for userId={} expired or never set", userId);
            return Optional.empty();
        }
        if (stored.toString().equals(inputCode)) {
            //otp consumed - delete immediately to prevent reuse
            redisTemplate.delete(key);
            return Optional.of("VALID");
        }
        return Optional.of("Invalid");
    }

    /**
     * Pattern 2: Sliding TTL (Session-like behavior)
     * On every access: TTL is reset to its full duration
     *
     * VISUALIZED with sessionTtlSeconds = 1800 (30 min):
     * t=0:      set "session:abc" → TTL = 1800s
     * t=900:    get "session:abc" → TTL reset to 1800s (active user!)
     * t=900+900=1800: get "session:abc" → TTL reset to 1800s (still active!)
     * t=2700+0: user goes idle...
     * t=2700+1800: "session:abc" finally expires (30 min after LAST access)
     *
     * This is exactly how "auto-logout after 30 minutes of inactivity" works.
     *
     * REDIS COMMAND: EXPIRE key seconds
     * This resets the TTL on an existing key.
     * Combined with a GET, it implements sliding TTL.
     *
     * NOTE: This requires 2 Redis commands per read (GET + EXPIRE).
     * To do this atomically in production, use a Lua script:
     * EVAL "local v = redis.call('GET', KEYS[1]); if v then redis.call('EXPIRE', KEYS[1], ARGV[1]) end; return v" 1 key ttl
     */

    public void createSession(String sessionId, Object sessionData) {
        String key = "session:" + sessionId;
        redisTemplate.opsForValue().set(key, sessionData, Duration.ofSeconds(sessionTtlSeconds));
        log.debug("[TTL-SLIDING] Created session={} with TTL={}s", sessionId, sessionTtlSeconds);
    }

    public Optional<Object> getSession(String sessionId) {
        String key = "session:" + sessionId;

        // GET — fetch the session data
        Object sessionData = redisTemplate.opsForValue().get(key);

        if (sessionData != null) {
            // EXPIRE — reset TTL because user is active
            // This is the "sliding" part: activity extends the session
            redisTemplate.expire(key, Duration.ofSeconds(sessionTtlSeconds));
            log.debug("[TTL-SLIDING] Session {} accessed — TTL reset to {}s",
                    sessionId, sessionTtlSeconds);
            return Optional.of(sessionData);
        }

        log.debug("[TTL-SLIDING] Session {} expired or not found", sessionId);
        return Optional.empty();
    }

    // 3. Jitter TTL (Anti-stampede)
    /**
     * Without jitter: All catalog items cached at startup with TTL=300s
     * → All expire at t=300s → Thundering herd
     *
     * With jitter: Each item gets TTL = 300 + random(0..60)
     * → Items expire spread over t=300s to t=360s
     * → Load is distributed over 60 seconds window
     *
     * CHOOSING JITTER RANGE:
     * Jitter should be ~10-20% of base TTL:
     * - TTL=300s → Jitter=0..60s (20%)
     * - TTL=3600s → Jitter=0..360s (10%)
     *
     * Too small: Not enough spread, stampede still possible
     * Too large: Keys expire wildly unpredictably, hard to reason about
     *
     * FULL JITTER vs DECORRELATED JITTER:
     * Full jitter = base + random(0..jitter_range) [simple, good enough]
     * Decorrelated = min(cap, random(base, prev_jitter*3)) [for retry backoff]
     * Use decorrelated for exponential backoff in retry logic.
     * Use full jitter for cache TTL.
     */

    public void cacheWithJitter(String key, Object value) {
        // Jitter = 20% of base TTL
        long jitterRange = (long) (defaultTtlSeconds * 0.20);
        long jitter = (long) (random.nextDouble() * jitterRange);
        long finalTtl = defaultTtlSeconds + jitter;

        redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(finalTtl));
        log.debug("[TTL-JITTER] Cached key={} with TTL={}s (base={}+jitter={})",
                key, finalTtl, defaultTtlSeconds, jitter);
    }

    // Probabilistic Early Expiry
    /**
     * PROBABILISTIC EARLY EXPIRY (PER) ALGORITHM
     *
     * This is the most sophisticated strategy, used by high-traffic
     * systems (CDNs, large-scale caches) to eliminate stampedes.
     *
     * KEY INSIGHT:
     * Instead of refreshing when TTL reaches 0 (too late — stampede occurs),
     * we PROBABILISTICALLY refresh as TTL decreases, allowing ONE request
     * to "early-refresh" the cache slightly before it expires.
     *
     * THE MATH:
     * P(early refresh) = exp(-remaining_ttl / (beta * fetch_time))
     *
     * Where:
     * - remaining_ttl: seconds left before expiry
     * - fetch_time: time to regenerate the value (DB query time)
     * - beta: tuning parameter (typically 1.0)
     *
     * When remaining_ttl >> fetch_time: P ≈ 0 (don't refresh early)
     * When remaining_ttl ≈ fetch_time: P ≈ 0.37 (refresh likely)
     * When remaining_ttl << fetch_time: P ≈ 1 (definitely refresh)
     *
     * RESULT: Exactly ONE request triggers an early refresh before
     * the key expires. All other concurrent requests get the cached
     * value. No stampede possible.
     *
     * WHEN TO USE PER:
     * - Keys that are VERY hot (accessed thousands of times/second)
     * - Regeneration is expensive (complex DB queries, external API calls)
     * - Zero tolerance for latency spikes
     */

    public Optional<Object> getWithProbabilisticEarlyExpiry(String key, double beta, long fetchTimeMs, java.util.function.Supplier<Object> fetchFunction){
        Object cached = redisTemplate.opsForValue().get(key);
        Long remainingTtlSeconds = redisTemplate.getExpire(key, TimeUnit.SECONDS);

        //no cached value - we just ffetch
        if (cached == null || remainingTtlSeconds == null || remainingTtlSeconds <= 0) {
            log.debug("[TTL-PER] Cache miss for key={}, fetching fresh", key);
            Object freshValue = fetchFunction.get();
            if (freshValue != null) {
                cacheWithJitter(key, freshValue);
            }
            return Optional.ofNullable(freshValue);
        }
        //pre calculation: convert fetch time to seconds for consistency
        double fetchTimeSec = fetchTimeMs/100.0;
        //THE PRE Formula: should we early-refresh? As remaning ttl approaches 0, this probabliy increases
        double earlyRefreshProb = Math.exp(-(double) remainingTtlSeconds / (beta * fetchTimeSec));

        boolean shouldEarlyRefresh = random.nextDouble() < earlyRefreshProb;
        if (shouldEarlyRefresh) {
            log.debug("[TTL-PER] early refresh triggered for key ={} " + "remainingTTL={}S, p={:.3f}", key, remainingTtlSeconds, earlyRefreshProb);
            //proactively refresh cache before expiry
            //this single request refreshs, all other get cached valeu
            Object freshValue = fetchFunction.get();
            if (freshValue != null) {
                cacheWithJitter(key, freshValue);
            }
            return Optional.ofNullable(freshValue);
        }

        // No early refresh needed — serve cached value
        log.debug("[TTL-PER] Serving cached value for key={} ({}s remaining)", key, remainingTtlSeconds);
        return Optional.of(cached);
    }


    /**
     * TTL DEBUGGING UTILITIES
     * These are extremely useful in production for diagnosing
     * cache behavior without connecting to redis-cli
     */
    public Long getRemainingTtl(String key) {
        return redisTemplate.getExpire(key, TimeUnit.SECONDS);
    }

    public boolean keyExists(String key) {
        return redisTemplate.hasKey(key);
    }
}
