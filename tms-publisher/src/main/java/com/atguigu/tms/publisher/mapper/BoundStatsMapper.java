package com.atguigu.tms.publisher.mapper;

import com.atguigu.tms.publisher.beans.BoundSortBean;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author Hliang
 * @create 2023-10-13 22:24
 */
public interface BoundStatsMapper {
    List<BoundSortBean> selectBoundSortCount(@Param("date") String date);
}
