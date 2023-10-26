package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jodd.util.StringUtil;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if(x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页的起始位置
        int start = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = start + SystemConstants.DEFAULT_PAGE_SIZE;
        //按照距离排序查询
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> search = stringRedisTemplate.opsForGeo().search(key, GeoReference.fromCoordinate(x, y),
                new Distance(5000), RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if(search == null || search.getContent().size() == 0){
            return Result.ok(new ArrayList<>());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = search.getContent();
        List<Long> ids = new ArrayList<>(content.size());
        Map<Long, Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(start).forEach(geoResult -> {
            RedisGeoCommands.GeoLocation<String> content1 = geoResult.getContent();
            ids.add(Long.valueOf(content1.getName()));
            distanceMap.put(Long.valueOf(content1.getName()), geoResult.getDistance());
        });
        String idStr = StringUtil.join(ids, ",");
        if(idStr == null || idStr.length() == 0){
            return Result.ok(new ArrayList<>());
        }
        List<Shop> shops = query().in("id", ids).last("order by field(id, " + idStr + ")").list();
        shops.forEach(shop -> {
            Distance distance = distanceMap.get(shop.getId());
            shop.setDistance(distance.getValue());
        });
        return Result.ok(shops);
    }
}
