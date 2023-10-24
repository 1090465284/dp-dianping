package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String shopType = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_KEY);
        if (StrUtil.isNotBlank(shopType)){
            List<ShopType> list = JSONUtil.toList(shopType, ShopType.class);
            return Result.ok(list);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if(typeList == null || typeList.isEmpty()){
            return Result.fail("查询结果为空");
        }
        String jsonList = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_TYPE_KEY, String.valueOf(jsonList), Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));
        return Result.ok(typeList);
    }
}
