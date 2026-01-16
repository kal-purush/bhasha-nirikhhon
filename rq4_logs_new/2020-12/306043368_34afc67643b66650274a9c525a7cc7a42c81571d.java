package org.example.common.domain.response;

import com.baomidou.mybatisplus.core.metadata.IPage;
import org.example.common.enums.ResultCode;
import org.example.common.util.JsonUtil;

import java.io.Serializable;
import java.util.List;

/**
 * @author linzhaoming
 * @since 2020-12-23
 **/
public class PageResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer code;

    private String msg;

    private List<T> data;

    private Long total;

    public PageResult(Integer code, String msg, List<T> data, Long total) {
        this.code = code;
        this.msg = msg;
        this.data = data;
        this.total = total;
    }

    public PageResult(ResultCode resultCode, List<T> data, Long total) {
        this(resultCode.getCode(), resultCode.getMsg(), data, total);
    }

    public PageResult(Integer code, String msg, IPage<T> page) {
        this(code, msg, page.getRecords(), page.getTotal());
    }

    public PageResult(ResultCode resultCode, IPage<T> page) {
        this(resultCode.getCode(), resultCode.getMsg(), page);
    }

    public static <T> PageResult<T> build(List<T> data, Long total) {
        return new PageResult<>(ResultCode.SUCCESS, data, total);
    }

    public static <T> PageResult<T> build(IPage<T> page) {
        return new PageResult<>(ResultCode.SUCCESS, page);
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return JsonUtil.toJSONString(this);
    }
}