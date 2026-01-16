package com.leechangu.sweettask;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Calendar;

/**
 * Created by Administrator on 2015/11/2.
 */
@RunWith(MockitoJUnitRunner.class)
public class TaskItemTest {


    @Test
    public void testGetIdAndSetId() throws Exception {
        TaskItem task = new TaskItem();
        task.setId(111);
        Assert.assertEquals(task.getId(), 111);
    }

    @Test
    public void testSetVibrateAndIsVibrate() throws Exception {
        TaskItem task = new TaskItem();
        task.setVibrate(true);
        Assert.assertEquals(task.isVibrate(), true);
        task.setVibrate(false);
        Assert.assertEquals(task.isVibrate(), false);
    }

    @Test
    public void testSetActiveAndIsActive() throws Exception {
        TaskItem task = new TaskItem();
        task.setVibrate(true);
        Assert.assertEquals(task.isVibrate(), true);
        task.setVibrate(false);
        Assert.assertEquals(task.isVibrate(), false);
    }

    @Test
    public void testSetFinishedAndIsFinished() throws Exception {
        TaskItem task = new TaskItem();
        task.setFinished(true);
        Assert.assertEquals(task.isFinished(), true);
        task.setFinished(false);
        Assert.assertEquals(task.isFinished(), false);
    }

    @Test
    public void testCalculateNextAlarmTime() throws Exception {
        TaskItem task = new TaskItem();
        Calendar calendar = task.calculateNextAlarmTime();
        Assert.assertEquals(calendar.get(Calendar.MILLISECOND), 0);
        Assert.assertEquals(calendar.get(Calendar.SECOND), 0);
        Assert.assertEquals(calendar.get(Calendar.MINUTE), 0);
        Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), 0);
        Assert.assertTrue(calendar.compareTo(Calendar.getInstance()) == 1);
    }


    @Test
    public void testGetTimeUntilNextAlarmMessage() throws Exception {
        TaskItem task = new TaskItem();
        Assert.assertNotNull(task.getTimeUntilNextAlarmMessage());
    }
}