package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,P> R get(String key, P param, Class<R> type, Function<P, R> dbFallback, Long time, TimeUnit unit){
        String res = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(res)){
            return JSONUtil.toBean(res, type);
        }
        if(res != null){
            return null;
        }
        R r = dbFallback.apply(param);
        if(r == null){
            stringRedisTemplate.opsForValue().set(key, "", time, unit);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, P> R getWithLogicalExpire(
            String key, P id, Class<R> type, Function<P, R> dbFallback, Long time, TimeUnit unit){
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean flag = tryLock(lockKey);
        if(flag){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    unLock(lockKey);
                }
            });

        }
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofSeconds(RedisConstants.LOCK_SHOP_TTL));
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
