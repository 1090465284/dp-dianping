package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.ui.ModelMap;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.context.request.WebRequestInterceptor;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.time.Duration;
import java.util.Map;

public class TokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public TokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String token = request.getHeader("authorization");
        if(StrUtil.isBlank(token)){
            return true;
        }
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(key);
        if(entries.isEmpty()){
            return true;
        }
        stringRedisTemplate.expire(key, Duration.ofSeconds(RedisConstants.LOGIN_USER_TTL));
        UserDTO userDTO = BeanUtil.fillBeanWithMap(entries, new UserDTO(), false);
        UserHolder.saveUser(userDTO);

        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        UserHolder.removeUser();
    }
}
