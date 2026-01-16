package com.bean;

import java.util.Date;

/*
 * @Description: 用户检测数据具体信息
 */
public class Data {
    private int u_id;
    private int t_id;
    private Date d_date;
    private double d_value;
    private String t_unit;

    /*
     * @Description: 用户检测数据构造函数
     * @Param u_id,  t_id,  d_date,  d_value, t_unit
     * @Return: null
     */

    public Data(int u_id, int t_id, Date d_date, double d_value,String t_unit)
    {
        this.u_id = u_id;
        this.t_id = t_id;
        this.d_date = d_date;
        this.d_value = d_value;
        this.t_unit = t_unit;
    }

    public Data(int u_id,int t_id)
    {
        this.u_id = u_id;
        this.t_id = t_id;
    }

    public int getU_id()
    {
        return u_id;
    }

    public void setU_id(int u_id)
    {
        this.u_id= u_id;
    }

    public int getT_id()
    {
        return t_id;
    }

    public void setT_id(int t_id)
    {
        this.t_id= t_id;
    }

    public Date getD_date()
    {
        return d_date;
    }

    public void setD_date(Date d_date)
    {
        this.d_date= d_date;
    }

    public double getD_value()
    {
        return d_value;
    }

    public void setD_value(double d_value)
    {
        this.d_value= d_value;
    }

    public String getT_unit()
    {
        return t_unit;
    }

    public void setT_unit(String t_unit)
    {
        this.t_unit= t_unit;
    }
}