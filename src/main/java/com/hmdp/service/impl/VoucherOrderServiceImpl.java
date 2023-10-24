package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }

//    private class VoucherOrderHandler implements Runnable{
//        String queueName = "streams.orders";
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    //获取redis消息队列的消息 xreadgroup group g1 c1 count 1 block 2000 streams streams.orders >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofMinutes(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//
//                    if(list == null || list.isEmpty()){
//                        continue;
//                    }
//
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> value = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    handleVoucherOrder(voucherOrder);
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                } catch (Exception e) {
//                    handlePendingList();
//                }
//            }
//        }
//        private void handlePendingList(){
//            while(true){
//                try {
//                    //获取redis消息队列的消息 xreadgroup group g1 c1 count 1 block 2000 streams streams.orders >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create(queueName, ReadOffset.from("0"))
//                    );
//
//                    if(list == null || list.isEmpty()){
//                        break;
//                    }
//
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> value = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    handleVoucherOrder(voucherOrder);
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                } catch (Exception e) {
//                    log.error("出现了异常",e);
//                    try {
//                        Thread.sleep(20);
//                    } catch (InterruptedException ex) {
//                        ex.printStackTrace();
//                    }
//                }
//            }
//        }
//    }

//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//        }
//    }
//    private void handleVoucherOrder(VoucherOrder voucherOrder) {
//        Long userId = UserHolder.getUser().getId();
//        Long voucherId = voucherOrder.getId();
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if(!isLock){
//            log.error("不允许重复下单");
//            return ;
//        }
//        try{
//            proxy.createVoucherOrder(voucherOrder);
//        }finally {
//            lock.unlock();
//        }
//    }
//    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
//    private IVoucherOrderService proxy;
//
//    static{
//        SECKILL_SCRIPT = new DefaultRedisScript<>();
//        SECKILL_SCRIPT.setLocation(new ClassPathResource("/lua/seckill.lua"));
//        SECKILL_SCRIPT.setResultType(Long.class);
//    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        /**
         * 1.执行lua脚本
         * 2.没有购买资格返回
         * 3.有购买资格保存到队列
         */
//        Long orderId = redisIdWorker.nextId("order");
//        Long userId = UserHolder.getUser().getId();
//        Long res = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(),orderId.toString()
//        );
//        if(res != 0L){
//            return Result.fail(res == 1? "库存不足" : "不能重复下单");
//        }
//
////        VoucherOrder voucherOrder = new VoucherOrder();
////
////        voucherOrder.setId(orderId);
////        voucherOrder.setUserId(UserHolder.getUser().getId());
////        voucherOrder.setVoucherId(voucherId);
////        //创建阻塞队列
////        orderTasks.add(voucherOrder);
//        //获取代理对象
//        proxy = (IVoucherOrderService)AopContext.currentProxy();
        return Result.ok(0);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        /**
//         * 1.查询优惠券
//         * 2.判断时间
//         * 3.判断库存
//         * 4.扣除库存
//         * 5.创建订单
//         * 6.返回订单
//         */
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        LocalDateTime beginTime = seckillVoucher.getBeginTime();
//        LocalDateTime endTime = seckillVoucher.getEndTime();
//        LocalDateTime now = LocalDateTime.now();
//        if(now.isBefore(beginTime)){
//            return Result.fail("秒杀未开始");
//        }
//        if(now.isAfter(endTime)){
//            return Result.fail("秒杀已结束");
//        }
//        if(seckillVoucher.getStock() <=0){
//            return Result.fail("优惠券已售罄");
//        }
//        Long userId = UserHolder.getUser().getId();
//
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//
//        if(!isLock){
//            return Result.fail("重复下单");
//        }
//        try{
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return ;
        }
//        LambdaUpdateWrapper<SeckillVoucher> lambdaUpdateWrapper = new LambdaUpdateWrapper<>();
//        lambdaUpdateWrapper.set(SeckillVoucher::getStock, seckillVoucher.getStock() - 1);
//        lambdaUpdateWrapper.eq(SeckillVoucher::getVoucherId, voucherId);
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("购买失败");
            return ;
        }

        save(voucherOrder);

    }
}
