package com.example.caching_patterns.services;

/**
 * CACHE PENETRATION PROTECTION
 *
 * Requests for data that doesnt exist in either cache or the primaryDB
 *
 * NORMAL CACHE MISS FLOW: Request for user:7 -> cache miss -> DB returns user -> cache populated
 * (next request hits cache -> fast)
 *
 * CACHE PENETRATION FLOW (harmful):

 *   Request for user:9999999999 → Cache miss → DB returns NULL → Nothing cached
 *   Request for user:9999999999 → Cache miss AGAIN → DB returns NULL → Nothing cached
 *   Request for user:9999999999 → Cache miss AGAIN → DB returns NULL → ...
 *   (Every request hits the DB — cache provides ZERO protection)
 *
 * WHY IS THIS DANGEROUS?
 * ----------------------
 * An attacker (or malformed client) can deliberately send:
 * GET /users/9000000001
 * GET /users/9000000002
 * GET /users/9000000003
 * ... (millions of non-existent IDs)
 *
 * Each request bypasses the cache and hammers the DB.
 * This is effectively a DoS attack on your database.
 *
 * Real-world analogy: Asking a librarian for a book that doesn't exist.
 * The librarian searches the entire catalog every time you ask,
 * even though you could instantly know "this doesn't exist" with
 * a catalog index.
 *
 * TWO DEFENSE STRATEGIES:
 * A: NULL VALUE CACHING
 * Intution:
 * "Cache the absence of data".
 * When db returns null, cache a special "NULL_MARKER" with short TTL
 * Next request for the same non-existen key hits cache->returns null immediately
 * DB is never hit agian until the short TTL expires
 *
 * IMPLEMENTATION: Miss -> DB returns null -> Cache "NULL_MARKER" for 60secs
 * Subsequent requests -> Cache hit -> "NULL_MARKER" -> return null (DB skipped)
 *
 * PROS:
 *  Simple to implement
 *  No false positives (unlike Bloom filters)
 *  Works for any type of non-existence
 *
 * CONS:
 *  Memory consumption at scale
 *    At 1 billion unique non-existent key requests:
 *    Each "NULL" entry = ~100 bytes in Redis
 *    1B × 100 bytes = 100 GB of null entries! (unsustainable)
 *  Short TTL means re-queries after expiry
 *  Timing attack: if attacker creates user:9999 after null is cached,
 *    cache serves stale null until TTL expires
 *
 *
 *    WHEN TO USE NULL CACHING
 *    - Low cardinality attacks (not many unique non-existent keys)
 *    - small scale systems
 *    - when implemenation simplicity matters
 *
 * STRATEGY B: BLOOM FILTER
 *
 * FIRST PRINCIPLES OF BLOOM FILTERS:
 *
 * A Bloom filter is a PROBABILISTIC data structure that answers:
 * "Does this item DEFINITELY NOT exist?" or "Does this item POSSIBLY exist?"
 *
 * GUARANTEED: If Bloom filter says "NOT EXISTS" → item definitely does NOT exist
 * POSSIBLE:   If Bloom filter says "EXISTS" → item MIGHT exist (false positive possible)
 *
 * There are NEVER false negatives (it will never say "not exists" for something that exists).
 * There CAN be false positives (it might say "exists" for something that doesn't).
 *
 * HOW IT WORKS INTERNALLY:
 *
 * Structure: A bit array of M bits, initialized to all 0s.
 * Operations: K independent hash functions
 *
 * ADD item "user:42":
 *   hash1("user:42") % M = position 17 → bit[17] = 1
 *   hash2("user:42") % M = position 84 → bit[84] = 1
 *   hash3("user:42") % M = position 203 → bit[203] = 1
 *
 * CHECK "user:42":
 *   hash1("user:42") % M = 17 → bit[17] == 1 ✓
 *   hash2("user:42") % M = 84 → bit[84] == 1 ✓
 *   hash3("user:42") % M = 203 → bit[203] == 1 ✓
 *   → "POSSIBLY EXISTS" (might be false positive, but likely real)
 *
 * CHECK "user:9999999":
 *   hash1("user:9999999") % M = 17 → bit[17] == 1 ✓ (collision!)
 *   hash2("user:9999999") % M = 42 → bit[42] == 0 ✗
 *   → "DEFINITELY DOES NOT EXIST" (one miss is enough)
 *   → Skip DB entirely! Return null immediately.
 *
 * SPACE EFFICIENCY:
 *
 * For 1 billion users with 1% false positive rate:
 * Required bits = -(n * ln(p)) / (ln(2)^2)
 *               = -(1B * ln(0.01)) / (ln(2)^2)
 *               ≈ 9.6 billion bits
 *               ≈ 1.2 GB
 *
 * Compare to Null Caching for 1B non-existent keys: ~100 GB
 * Bloom filter is ~83x more space-efficient!
 *
 * FALSE POSITIVE RATE FORMULA:
 * p ≈ (1 - e^(-kn/m))^k
 * Where k = number of hash functions, n = elements, m = bit array size
 *
 * OPTIMAL NUMBER OF HASH FUNCTIONS:
 * k = (m/n) * ln(2)
 *
 * IN SPRING BOOT (using Redisson or Redis native BITFIELD):
 * Spring Boot doesn't have built-in Bloom filter support.
 * We implement using Redis SETBIT/GETBIT which operates on individual bits.
 * Production option: Redisson's RBloomFilter or Redis Stack's BF.ADD/BF.EXISTS
 *
 * WHEN TO USE BLOOM FILTERS:
 *   High cardinality attacks (billions of unique non-existent keys)
 *   Need to protect DB from ANY non-existent query
 *   Memory-constrained environments
 *   When you can tolerate rare false positives (occasional extra DB call)
 *
 * WHEN NOT TO USE BLOOM FILTERS:
 *  Dynamic datasets (adding new users means updating the filter)
 *  Deletions are impossible in basic Bloom filters (need Counting Bloom Filter)
 *  Exact membership needed (false positives are unacceptable)
 *
 * TRADEOFFS SUMMARY:
 *
 * | Feature          | Null Cache       | Bloom Filter         |
 * |------------------|------------------|----------------------|
 * | Accuracy         | 100% accurate    | ~1% false positives  |
 * | Memory           | O(unique misses) | O(dataset size)      |
 * | Implementation   | Simple           | Complex              |
 * | Handles deletes  | Yes (TTL)        | Not easily           |
 * | Scale (1B keys)  | ~100 GB          | ~1.2 GB              |
 */



 import com.example.caching_patterns.User;
 import com.example.caching_patterns.UserRepository;
 import com.fasterxml.jackson.databind.ObjectMapper;
 import jakarta.annotation.PostConstruct;
 import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
 import org.springframework.beans.factory.annotation.Value;
 import org.springframework.data.redis.core.RedisTemplate;
 import org.springframework.stereotype.Service;

 import java.time.Duration;
 import java.util.Optional;

@Service("cachePenetrationService")
@Slf4j
@RequiredArgsConstructor
public class CachePenetrationService {

    private final UserRepository userRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${cache.ttl.default:300}")
    private long defaultTtlSeconds;

    @Value("${cache.ttl.null-value:60}")
    private long nullValueTtlSeconds;

    // Sentinel value stored in Redis to represent "this key was queried,
    // DB returned null, don't query DB again until TTL expires"
    private static final String NULL_MARKER = "##NULL##";
    private static final String NULL_CACHE_PREFIX   = "null-cache:user:";
    private static final String BLOOM_CACHE_PREFIX  = "bloom-cache:user:";
    private static final String BLOOM_FILTER_KEY    = "bloom:users";

    //bloom filter configuration
    //capacity = expected no.of elements
    //error_rate = desired false +ve probability(1%=0.01)
    private static final int BLOOM_CAPACITY = 100_000;
    private static final double BLOOM_ERROR_RATE = 0.01;

    //bloom filter derived parameters, these are calculated from capacity + error_rate
    private int bitArraySize;
    private int hashCount;

    @PostConstruct
    public void initBloomFilter() {
        // m = -(n*ln(p))/(ln(2)^2)
        this.bitArraySize = (int) Math.ceil(-(BLOOM_CAPACITY * Math.log(BLOOM_ERROR_RATE) / Math.log(2) * Math.log(2)));
        // Formula: k = (m/n) * ln(2)
        this.hashCount = (int) Math.round(
                ((double) bitArraySize / BLOOM_CAPACITY) * Math.log(2)
        );

        log.info("[BLOOM-FILTER] Initialized: capacity={}, errorRate={}%, " +
                        "bitArraySize={}, hashCount={}",
                BLOOM_CAPACITY, BLOOM_ERROR_RATE * 100, bitArraySize, hashCount);

        // Warm the Bloom filter with existing user IDs from DB
        // In production: this might be done in a startup job or
        // incrementally as users are created
        warmBloomFilter();
    }
    private void warmBloomFilter() {
        log.info("[BLOOM-FILTER] Warming filter with existing user IDs...");
        userRepository.findAll().forEach(user ->
                bloomFilterAdd(String.valueOf(user.getId()))
        );
        log.info("[BLOOM-FILTER] Warm-up complete.");
    }


    //A. NULL VALUE Caching
    /**
     * Null Cache read flow:
     * 1. Check redis for the key
     *  a. HIT with real data -> return it
     *  b. Hit with null_marker -> return empty(dont query DB)
     *  c. Miss -> queryDB
     *
     * 2. On DB query:
     *   a. DB returns data -> cache it normally with full TTL
     *   b. DB returns null -> cache NULL_MARKER with short TTL
     *
     * THe short TTL for null markers:
     * Regualr TTL = 5 mins(data can be stale for 5mins)
     * Null TTL = 1min (null entries refresh faster)
     *
     * Why shorter? Because:
     * - If user:99 genuinely doesnt exit now but is created 30s from now, we want the null to expire quicky so the new user is findable
     * - Null entries are "cheap" to reverify (DB says null quickly)
     *
     */

    public Optional<User> getUserWithNullCache(Long userId){
        String cacheKey = NULL_CACHE_PREFIX + userId;
        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            if (NULL_MARKER.equals(cached.toString())) {
                // This key was previously looked up and returned null
                // DB is definitely not worth querying right now
                log.debug("[NULL-CACHE] NULL_MARKER HIT for userId={} — skipping DB", userId);
                return Optional.empty();
            }
            log.debug("[NULL-CACHE] CACHE HIT for userId={}", userId);
            return Optional.of((User) cached);
        }
        //cache miss - queryDB
        log.debug("[NULL-CACHE] CACHE MISS for userId={}, querying DB", userId);
        Optional<User> userOpt = userRepository.findById(userId);

        if (userOpt.isPresent()) {
            // Real data found — cache it with full TTL
            redisTemplate.opsForValue().set(
                    cacheKey,
                    userOpt.get(),
                    Duration.ofSeconds(defaultTtlSeconds)
            );
            log.debug("[NULL-CACHE] Cached real data for userId={}", userId);
        } else {
            // Non-existent key! Cache a null marker with SHORT TTL
            // This prevents future DB queries for this non-existent key
            redisTemplate.opsForValue().set(
                    cacheKey,
                    NULL_MARKER,
                    Duration.ofSeconds(nullValueTtlSeconds)
            );
            log.debug("[NULL-CACHE] Cached NULL_MARKER for userId={} (TTL={}s)",
                    userId, nullValueTtlSeconds);
        }

        return userOpt;

    }

    // BLOOM FILTER

    /**
     * BLOOM FILTER READ FLOW:
     *
     * 1. Check Bloom filter: "Does userId exist?"
     *    - If "DEFINITELY NOT" → return empty immediately (no DB, no cache query!)
     *    - If "POSSIBLY YES" → proceed to cache lookup
     *
     * 2. Check cache → DB fallback (normal flow)
     *
     * WHY CHECK BLOOM FILTER BEFORE CACHE?
     * - If Bloom says "NOT EXISTS": We save BOTH a cache lookup AND a DB query
     * - If Bloom says "EXISTS": We still do normal cache/DB lookup
     * - Net result: Non-existent key queries are O(k) hash operations (~nanoseconds)
     *   instead of O(1) Redis call + O(log n) DB call
     */

    public Optional<User> getUserWithBloomFilter(Long userId) {
        String userIdStr = String.valueOf(userId);
        //bloom filter check  ultrafast(just bit array lookups)
        if (!bloomFilterMightContain(userIdStr)) {
            log.debug("[BLOOM-FILTER] Definite miss for userId={} — blocked before cache/DB", userId);
            return Optional.empty();
        }
        //bloom says "possibly exists" proceed with normal lookup
        String cacheKey = BLOOM_CACHE_PREFIX + userId;
        Object cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.debug("[BLOOM-FILTER] CACHE HIT for userId={}", userId);
            return Optional.of(toUser(cached));
        }



        // False positive path: Bloom said "EXISTS" but cache miss
        // Query DB to confirm
        log.debug("[BLOOM-FILTER] CACHE MISS for userId={}, querying DB " +
                "(might be false positive from Bloom)", userId);

        Optional<User> userOpt = userRepository.findById(userId);

        if (userOpt.isPresent()) {
            // Confirmed exists — cache it and ensure Bloom is updated
            redisTemplate.opsForValue().set(
                    cacheKey, userOpt.get(), Duration.ofSeconds(defaultTtlSeconds));
        } else {
            // Bloom filter false positive confirmed!
            // userId doesn't actually exist. Nothing to cache.
            // The Bloom filter will continue to have this false positive
            // (we can't delete from a basic Bloom filter)
            log.debug("[BLOOM-FILTER] FALSE POSITIVE confirmed for userId={} " +
                    "(Bloom said existed, DB says no)", userId);
        }

        return userOpt;
    }
    private User toUser(Object cached) {
        if (cached instanceof User user) {
            return user;
        }
        return objectMapper.convertValue(cached, User.class);
    }



    /**
     * ADD to Bloom Filter when a new user is created.
     * Must be called on every user creation to keep filter current.
     */
    public User createUserWithBloomFilter(User user) {
        User saved = userRepository.save(user);
        bloomFilterAdd(String.valueOf(saved.getId()));

        // Also populate cache (write-through style for new users)
        String cacheKey = BLOOM_CACHE_PREFIX + saved.getId();
        redisTemplate.opsForValue().set(
                cacheKey, saved, Duration.ofSeconds(defaultTtlSeconds));

        log.debug("[BLOOM-FILTER] Created userId={} + added to Bloom filter", saved.getId());
        return saved;
    }

    // BLOOM FILTER INTERNALS
    /**
     * BLOOM FILTER ADD:
     *
     * Uses K independent hash functions, each mapping the input to
     * a position in the bit array. Sets each position to 1.
     *
     * We simulate K independent hash functions using:
     * hash_i(item) = (murmur3(item) + i * fnv1a(item)) % bitArraySize
     *
     * This is the "double hashing" technique — generates K effectively
     * independent hashes from just 2 hash computations.
     *
     * We store bits in Redis using SETBIT command.
     * Redis SETBIT is O(1) and thread-safe.
     */
    private void bloomFilterAdd(String item) {
        for (int i = 0; i < hashCount; i++) {
            long bitPosition = getHashPosition(item, i);
            redisTemplate.opsForValue().setBit(BLOOM_FILTER_KEY, bitPosition, true);
        }
    }



    /**
     * BLOOM FILTER CONTAINS CHECK:
     *
     * For each hash function i, compute the bit position.
     * If ALL positions are 1 → "POSSIBLY EXISTS"
     * If ANY position is 0 → "DEFINITELY NOT EXISTS"
     *
     * ONE zero is enough to definitively exclude the item.
     * (Because if it had been added, ALL positions would have been set to 1)
     */
    private boolean bloomFilterMightContain(String item) {
        for (int i = 0; i < hashCount; i++) {
            long bitPosition = getHashPosition(item, i);
            Boolean bitSet = redisTemplate.opsForValue().getBit(BLOOM_FILTER_KEY, bitPosition);
            if (!Boolean.TRUE.equals(bitSet)) {
                return false; // Definite "not present"
            }
        }
        return true; // "Possibly present" (might be false positive)
    }

    /**
     * HASH POSITION CALCULATION:
     * Uses double hashing: h(i, item) = (h1(item) + i * h2(item)) % m
     *
     * h1 = Java's hashCode (deterministic within a JVM run)
     * h2 = FNV-like second hash (for independence)
     *
     * NOTE: In production, use MurmurHash3 or xxHash for better
     * distribution. Java's hashCode is not ideal for Bloom filters
     * but works for demonstration.
     */
    private long getHashPosition(String item, int seed) {
        int h1 = item.hashCode();
        // Second hash: different mixing function for independence
        int h2 = item.chars().reduce(0, (acc, c) -> acc * 31 + c);

        // Ensure non-negative and within bit array bounds
        long hash = Math.abs((long) h1 + (long) seed * h2);
        return hash % bitArraySize;
    }


}
