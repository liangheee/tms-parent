package com.atguigu.tms.publisher.controller;

import com.atguigu.tms.publisher.beans.BoundSortBean;
import com.atguigu.tms.publisher.common.ResultVo;
import com.atguigu.tms.publisher.service.BoundStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Hliang
 * @create 2023-10-13 22:16
 */
@RestController
public class BoundStatsController {

    @Autowired
    private BoundStatsService boundStatsService;

    @RequestMapping("/boundSortCount/{date}")
    public ResultVo getBoundSortCount(@PathVariable(name = "date",required = false) String date){
        List<BoundSortBean> boundSortCounts = boundStatsService.getBoundSortCount(date);
        HashMap<String, Object> data = new HashMap<>();
        data.put("valueName","分拣");
        ArrayList<Map<String, Object>> mapDatas = new ArrayList<>();
        for (BoundSortBean boundSortCount : boundSortCounts) {
            HashMap<String, Object> innerMap = new HashMap<>();
            innerMap.put("name",boundSortCount.getProvinceName());
            innerMap.put("value",boundSortCount.getValue());
            mapDatas.add(innerMap);
        }
        data.put("mapData",mapDatas);
        return ResultVo.succeed().data(data);
    }

}
