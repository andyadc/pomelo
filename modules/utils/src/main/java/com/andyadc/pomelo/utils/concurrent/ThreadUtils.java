package com.andyadc.pomelo.utils.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * 线程相关工具类.
 *
 * @author andaicheng
 * @version 2017/1/8
 */
public class ThreadUtils {

    /////////// 线程相关功能//////////

    /**
     * sleep等待, 单位为毫秒, 已捕捉并处理InterruptedException.
     */
    public static void sleep(long durationMillis) {
        try {
            Thread.sleep(durationMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * sleep等待，已捕捉并处理InterruptedException.
     */
    public static void sleep(long duration, TimeUnit unit) {
        try {
            Thread.sleep(unit.toMillis(duration));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
