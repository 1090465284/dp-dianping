package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
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
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private IUserService userService;

    private String KEY = "follow:";

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        if(isFollow){
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean success = save(follow);
            if(success){
                redisTemplate.opsForSet().add(KEY + userId, followUserId.toString());
            }
        }else{
            LambdaQueryWrapper<Follow> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.eq(Follow::getFollowUserId, followUserId);
            queryWrapper.eq(Follow::getUserId, userId);
            boolean success = remove(queryWrapper);
            if(success){
                redisTemplate.opsForSet().remove(KEY +userId, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        LambdaQueryWrapper<Follow> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(Follow::getFollowUserId, followUserId);
        queryWrapper.eq(Follow::getUserId, userId);
        int count = count(queryWrapper);
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommmons(Long id) {
        Long userId = UserHolder.getUser().getId();
        Set<String> intersect = redisTemplate.opsForSet().intersect(KEY + id, KEY + userId);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(ids);
        List<UserDTO> res = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(res);
    }
}
