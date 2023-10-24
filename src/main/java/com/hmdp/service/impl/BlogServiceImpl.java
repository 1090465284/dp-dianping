package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.api.R;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.apache.logging.log4j.message.ReusableMessage;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private IUserService userService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private IFollowService followService;

    @Override
    public List<Blog> queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            getBlogUserInfo(blog);
            blog.setIsLike(getIsLiked(blog));
        });
        return records;
    }

    @Override
    public Blog queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog==null){
            throw new RuntimeException("博客不存在");
        }
        getBlogUserInfo(blog);
        blog.setIsLike(getIsLiked(blog));
        return blog;
    }

    @Override
    public void likeBlog(Long id) {
        //判断当前用户是否已经点赞
        UserDTO user = UserHolder.getUser();
        if(user == null){
            //用户未登录
            return ;
        }
        Long userId = user.getId();
        String key = RedisConstants.BLOG_LIKED_KEY +id;
//        Boolean isMember = redisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = redisTemplate.opsForZSet().score(key, userId.toString());
        if(ObjectUtils.isEmpty(score)){
            boolean success = update().eq("id", id).setSql("liked = liked + 1").update();
            if(success){
//                redisTemplate.opsForSet().add(key, userId.toString());
                redisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }else{
            boolean success = update().eq("id", id).setSql("liked = liked - 1").update();
            if(success){
                redisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Set<String> userId = redisTemplate.opsForZSet().range(key, 0, 5);
        if(userId == null || userId.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> top5 = userId.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(top5);
        List<UserDTO> res = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(res);
    }

    @Override
    public Result saveBlog(Blog blog) {
//         获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean success = save(blog);
        if(!success){
            return Result.fail("新增笔记失败");
        }
        //查询笔记的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //推送
        follows.stream().map(Follow::getUserId).forEach(id->{
            String key = FEED_KEY + id;
            redisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        });

        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long lastId, Integer offset) {
        UserDTO user = UserHolder.getUser();
        String key = FEED_KEY +user.getId();
        Set<ZSetOperations.TypedTuple<String>> typedTuples = redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, lastId, offset, 2);
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        long minTime = 0;
        int newOffSet = 1;
        List<Long> ids = new ArrayList<>(typedTuples.size());
        for(ZSetOperations.TypedTuple<String> tuple : typedTuples){
            String value = tuple.getValue();
            ids.add(Long.valueOf(value));
            long time = tuple.getScore().longValue();
            if(time == minTime){
                newOffSet ++;
            }else{
                minTime = time;
                newOffSet = 1;
            }
        }
        List<Blog> blogs = ids.stream().map(this::queryBlogById).collect(Collectors.toList());
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(newOffSet);
        return Result.ok(scrollResult);
    }

    private Boolean getIsLiked(Blog blog){

        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();

        return !ObjectUtils.isEmpty(redisTemplate.opsForZSet().score(key, userId.toString()));
    }

    private void getBlogUserInfo(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
