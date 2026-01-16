package com.yun.demo.bean;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.List;

@Data
@TableName("student")
public class Student {

    @TableId(type = IdType.AUTO)
    private int id;

    @TableField("Introduce_id")
    private int introduceId;

    @TableField("name")
    private String name;

    @TableField(exist = false)
    private List<Student> children;

}