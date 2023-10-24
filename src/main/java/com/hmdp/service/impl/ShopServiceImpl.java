package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

//        //互斥锁解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
//        if(shop == null){
//            return Result.fail("店铺不存在");
//        }
        String key = RedisConstants.CACHE_SHOP_KEY + id;
//        Shop shop = cacheClient.get(key,id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        Shop shop = cacheClient.getWithLogicalExpire(key, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
    }

    public Shop queryWithPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if(shopJson != null){
            return null;
        }
        Shop shop = this.getById(id);
        if(shop == null){
            stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
            return null;
        }
        String JsonShop = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(key, JsonShop, Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));
        return shop;
    }

    public Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if(shopJson != null){
            return null;
        }
        //实现缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = this.getById(id);
            if(shop == null){
                stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
                return null;
            }
            String JsonShop = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key, JsonShop, Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            unLock(lockKey);
        }
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean flag = tryLock(lockKey);
        if(flag){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    this.saveShop2Redis(id, RedisConstants.CACHE_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    unLock(lockKey);
                }

            });

        }
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofSeconds(RedisConstants.LOCK_SHOP_TTL));
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id, Long expireMinutes){
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(expireMinutes));
        redisData.setData(shop);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        this.updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok("修改成功");
    }
}
