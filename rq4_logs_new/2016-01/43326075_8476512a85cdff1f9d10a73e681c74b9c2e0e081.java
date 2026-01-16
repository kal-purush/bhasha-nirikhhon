package com.example.susha.sensor_collect.Server;


import android.os.Message;
import android.util.Log;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;

import java.io.File;
import java.util.Vector;

public class FTPTransfer {
    Session session = null;
    String user="Server3";
    String host="130.245.191.166";
    int port=22;
    public FTPTransfer(){

    }
    public long uploadFile(File file){
        long size=0;
        JSch ssh = new JSch();
        try {
            session = ssh.getSession(user,host,port);
            // remove the hard coding and add UI screen to get credentials
            session.setHost("Server3");
            session.setPort(22);
            session.setPassword("LAB166");
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp c = (ChannelSftp)channel;
            Vector vv=c.ls("/");
            if(vv!=null){
                for(int i=0;i<vv.size();i++){
                    Log.i("sftp ls command",vv.toString());
                }
            }
            //use c.put(src,dst,monitor,mode);
            //mode can be OVERWRITE, RESUME, or APPEND
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }

        return size;
    }

}