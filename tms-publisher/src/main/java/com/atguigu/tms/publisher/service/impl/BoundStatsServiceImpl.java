package com.atguigu.tms.publisher.service.impl;

import com.atguigu.tms.publisher.beans.BoundSortBean;
import com.atguigu.tms.publisher.mapper.BoundStatsMapper;
import com.atguigu.tms.publisher.service.BoundStatsService;
import com.atguigu.tms.publisher.utils.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Hliang
 * @create 2023-10-13 22:22
 */
@Service
public class BoundStatsServiceImpl implements BoundStatsService {

    @Autowired
    private BoundStatsMapper boundStatsMapper;

    @Override
    public List<BoundSortBean> getBoundSortCount(String date) {
        if(StringUtils.isEmpty(date)){
            date = DateFormatUtil.now();
        }
        return boundStatsMapper.selectBoundSortCount(date);
    }
}
