package com.unis.db.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.*;

/**
 * @author xuli
 * @date 2019/2/12
 */
@Component
public class ThreadUtils {
    private ExecutorService makeDataExecutorService = null;
    private LinkedBlockingDeque<Future> makeDataLinkedBlockingDeque = null;
    private ExecutorService queryExecutorService = null;
    private LinkedBlockingDeque<Future> queryLinkedBlockingDeque = null;
    private static final Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

    /**
     * 默认线程池中线程的个数：自定义
     */
    @PostConstruct
    public void init() {
        makeDataExecutorService = Executors.newFixedThreadPool(10);
        makeDataLinkedBlockingDeque = new LinkedBlockingDeque<>();
        queryExecutorService = Executors.newFixedThreadPool(12);
        queryLinkedBlockingDeque = new LinkedBlockingDeque<>();
    }

    /**
     * 提交task给线程池
     *
     * @param task 任务
     */
    @SuppressWarnings(value = {"unchecked"})
    public void submit(Callable task) {
        Future future = makeDataExecutorService.submit(task);
        makeDataLinkedBlockingDeque.offer(future);
    }

    @SuppressWarnings(value = {"unchecked"})
    public void submit(Callable task, boolean threadType) {
        if (threadType) {
            submit(task);
        } else {
            Future future = queryExecutorService.submit(task);
            queryLinkedBlockingDeque.offer(future);
        }
    }

    /**
     * 等待所有任务完成
     *
     * @param threadNum 线程数
     * @return T or F
     */
    public boolean waitTask(int threadNum, boolean threadType) {
        int total = 0;
        for (int i = 0; i < threadNum; i++) {
            try {
                Future future;
                //没有就阻塞
                if (threadType) {
                    future = makeDataLinkedBlockingDeque.take();
                } else {
                    future = queryLinkedBlockingDeque.take();
                }
                Boolean flag = (Boolean) future.get();
                if (flag) {
                    total++;
                }
            } catch (ExecutionException | InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (total == threadNum) {
            return true;
        } else {
            logger.error(" * * * * Threads are not fully executed * * * * ");
            return false;
        }
    }
}
