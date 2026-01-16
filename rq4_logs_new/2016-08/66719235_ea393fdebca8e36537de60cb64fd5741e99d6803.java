package com.example.icst;

import android.content.Context;
import android.os.Handler;
import android.os.Message;

import com.example.icst.dao.DaoSession;
import com.example.icst.dao.StudentDao;

import java.util.List;

/**
 * Created by 大杨编 on 2016/8/28.
 */
public class UploadThread extends Thread {
    private Handler handler;
    private DaoSession session;
    private StudentDao studentDao;

    public UploadThread(Context context, Handler handler) {
        session = DBUtil.getDaoSession(context);
        studentDao = session.getStudentDao();
        this.handler = handler;
    }

    @Override
    public void run() {
        String message = "";
        for (int i = 1; i <= 5; i++) {
            List<Student> students = studentDao.queryBuilder()
                    .where(StudentDao.Properties.Accepted.eq(i))
                    .orderAsc(StudentDao.Properties.Id)
                    .list();
            int size = students.size();
            if (size != 0) {
                for (int x = 0; x < size - 1; x++)
                    message += Long.toHexString(students.get(x).getId()) + ",";
                message += Long.toHexString(students.get(size - 1).getId());
            }
        }
        for (int i = 11; i <= 15; i++) {
            List<Student> students = studentDao.queryBuilder()
                    .where(StudentDao.Properties.Accepted.eq(i))
                    .orderAsc(StudentDao.Properties.Id)
                    .list();
            int size = students.size();
            if (size != 0) {
                for (int x = 0; x < size - 1; x++)
                    message += Long.toHexString(students.get(x).getId()) + ",";
                message += Long.toHexString(students.get(size - 1).getId());
            }
            if (i != 15) message += ".";
        }
        message = message.replace(".....", "*");
        Message msg = new Message();
        msg.obj = message;
        handler.sendMessage(msg);
    }
}