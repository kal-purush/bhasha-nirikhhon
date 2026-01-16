package com.gupaoedu.vip.mall.goods.model;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouzhen
 * @description
 * @date 2021/6/22
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("brand")
public class Brand {

    /**
     * 主键
     */
    @TableId(type = IdType.AUTO,value = "id")
    private Integer id;

    /**
     * 品牌名称
     */
    private String name;

    /**
     * 品牌图片
     */
    private String image;

    /**
     * 品牌首字母
     */
    private String initial;

    /**
     * 排序
     */
    private Integer sort;
}