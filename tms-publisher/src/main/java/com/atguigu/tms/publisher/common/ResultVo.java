package com.atguigu.tms.publisher.common;

import lombok.Data;

/**
 * @author Hliang
 * @create 2023-10-13 21:07
 */
@Data
public class ResultVo {
    private Integer status;
    private String msg;
    private Object data;

    private ResultVo (){}

    private ResultVo (Integer status,String msg,Object data){
        this.status = status;
        this.msg = msg;
        this.data = data;
    }

    private ResultVo (Integer status,String msg){
        this.status = status;
        this.msg = msg;
    }

    public static ResultVo succeedWithData(Object data){
        return new ResultVo(0,"",data);
    }

    public static ResultVo succeed(){
        return new ResultVo(0,"",null);
    }

    public static ResultVo error(Integer status,String msg){
        return new ResultVo(status,msg,null);
    }

    public ResultVo status(Integer status){
        this.status = status;
        return this;
    }

    public ResultVo msg(String msg){
        this.msg = msg;
        return this;
    }

    public ResultVo data(Object data){
        this.data = data;
        return this;
    }
}
