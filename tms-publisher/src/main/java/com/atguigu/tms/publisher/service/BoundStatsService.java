package com.atguigu.tms.publisher.service;

import com.atguigu.tms.publisher.beans.BoundSortBean;

import java.util.List;

/**
 * @author Hliang
 * @create 2023-10-13 22:17
 */
public interface BoundStatsService {
    List<BoundSortBean> getBoundSortCount(String date);
}
