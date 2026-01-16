package com.zack.test.demo;

import com.zack.lib.utils.LogUtils;

/**
 * 描述：
 * 作者：Zack
 * 创建时间;2018/5/23
 * 修改时间：2018/5/23
 **/
public class LogUtilsControl {

    private String TAG="LogUtilsControl";

    public void testLevel()
    {
        LogUtils.setLevel(LogUtils.ERROR);
        LogUtils.v(TAG,"VERBOS");
        LogUtils.d(TAG,"DEBUG");
        LogUtils.i(TAG,"INFO");
        LogUtils.w(TAG,"WARN");
        LogUtils.e(TAG,"ERROR");
    }

    public void testWriteFile()
    {
        LogUtils.setLogPath("/com/zack");
        LogUtils.v(TAG,"VERBOS");
        LogUtils.d(TAG,"DEBUG");
        LogUtils.i(TAG,"INFO");
        LogUtils.w(TAG,"WARN");
        LogUtils.e(TAG,"ERROR");
    }
}