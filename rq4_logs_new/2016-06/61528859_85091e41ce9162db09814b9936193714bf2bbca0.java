package com.czy;

import android.app.Activity;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.SharedPreferences.Editor;

public class ControlCommand {
	
	public static final String COMMAND_FILE = "CommandFile";
	private Context mContext;
	
	public static final String STOPCOMMAND_KEY = "STOPCOMMAND_KEY";
	public static final String UPCOMMAND_KEY = "UPCOMMAND_KEY";
	public static final String DOWNCOMMAND_KEY = "DOWNCOMMAND_KEY";
	public static final String LEFTCOMMAND_KEY = "LEFTCOMMAND_KEY";
	public static final String RIGHTCOMMAND_KEY = "RIGHTCOMMAND_KEY";
	public static final String LEFTUPCOMMAND_KEY = "LEFTUPCOMMAND_KEY";
	public static final String RIGHTUPCOMMAND_KEY = "RIGHTUPCOMMAND_KEY";
	public static final String LEFTDOWNCOMMAND_KEY = "LEFTDOWNCOMMAND_KEY";
	public static final String RIGHTDOWNCOMMAND_KEY = "RIGHTDOWNCOMMAND_KEY";
	public static final String CONTROLADDRESS_KEY = "CONTROLADDRESS_KEY";
	public static final String CONTROLPORT_KEY = "CONTROLPORT_KEY";
	public static final String VEDIOADDRESS_KEY = "VEDIOADDRESS_KEY";
	
	public final static String RUNNING_STOP = "FF000000FF";
	public final static String RUNNING_UP = "FF000100FF";
	public final static String RUNNING_DOWN = "FF000200FF";
	public final static String RUNNING_LEFT = "FF000300FF";
	public final static String RUNNING_RIGHT = "FF000400FF";
	public final static String RUNNING_LEFT_UP = "FF000500FF";
	public final static String RUNNING_RIGHT_UP = "FF000600FF";
	public final static String RUNNING_LEFT_DOWN = "FF000700FF";
	public final static String RUNNING_RIGHT_DOWN = "FF000800FF";
	public final static String COMMAND_ALARM_ON = "FF030000FF";
	public final static String COMMAND_ALARM_OFF = "FF030100FF";
	public final static String COMMAND_LED_ON = "FF020000FF";
	public final static String COMMAND_LED_OFF = "FF020100FF";
	
	public static final String DEFAULT_SERVER_ADDR = "192.168.1.1";
	public static final String DEFAULT_SERVER_PORT = "2001";
	public static final String DEFAULT_VEDIO_ADDR = "http://192.168.1.1:8080/?action=stream";
	
	public String mUpCommand;
	public String mDownCommand;
	public String mLeftCommand;
	public String mRightCommand;
	public String mLeftUpCommand;
	public String mLeftDownCommand;
	public String mRightUpCommand;
	public String mRightDownCommand;
	public String mStopCommand;
	public String mControlAddr;
	public String mControlPort;
	public String mVideoAddr;
	
	public ControlCommand(Context context){
		mContext = context;
	}
	
	public void CommandSave(){
		SharedPreferences mSharedPreferences = mContext.getSharedPreferences(COMMAND_FILE, Activity.MODE_PRIVATE);
		Editor mEditor = mSharedPreferences.edit();
		mEditor.putString(UPCOMMAND_KEY, mUpCommand);
		mEditor.putString(DOWNCOMMAND_KEY, mDownCommand);
		mEditor.putString(LEFTCOMMAND_KEY, mLeftCommand);
		mEditor.putString(RIGHTCOMMAND_KEY, mRightCommand);
		mEditor.putString(LEFTUPCOMMAND_KEY, mLeftUpCommand);
		mEditor.putString(RIGHTUPCOMMAND_KEY, mRightUpCommand);
		mEditor.putString(LEFTDOWNCOMMAND_KEY, mLeftDownCommand);
		mEditor.putString(RIGHTDOWNCOMMAND_KEY, mRightDownCommand);
		mEditor.putString(STOPCOMMAND_KEY, mStopCommand);
		mEditor.putString(CONTROLADDRESS_KEY, mControlAddr);
		mEditor.putString(CONTROLPORT_KEY, mControlPort);
		mEditor.putString(VEDIOADDRESS_KEY, mVideoAddr);
		mEditor.commit();
	}
	
	public void LoadCommand(){
		SharedPreferences mSharedPreferences = mContext.getSharedPreferences(COMMAND_FILE, Activity.MODE_PRIVATE);
		mUpCommand = mSharedPreferences.getString(UPCOMMAND_KEY, RUNNING_UP);
		mDownCommand = mSharedPreferences.getString(DOWNCOMMAND_KEY, RUNNING_DOWN);
		mLeftCommand = mSharedPreferences.getString(LEFTCOMMAND_KEY, RUNNING_LEFT);
		mRightCommand = mSharedPreferences.getString(RIGHTCOMMAND_KEY, RUNNING_RIGHT);
		mLeftUpCommand = mSharedPreferences.getString(LEFTUPCOMMAND_KEY, RUNNING_LEFT_UP);
		mLeftDownCommand = mSharedPreferences.getString(LEFTDOWNCOMMAND_KEY, RUNNING_LEFT_DOWN);
		mRightUpCommand = mSharedPreferences.getString(RIGHTUPCOMMAND_KEY, RUNNING_RIGHT_UP);
		mRightDownCommand = mSharedPreferences.getString(RIGHTDOWNCOMMAND_KEY, RUNNING_RIGHT_DOWN);
		mStopCommand = mSharedPreferences.getString(STOPCOMMAND_KEY, RUNNING_STOP);
		mControlAddr = mSharedPreferences.getString(CONTROLADDRESS_KEY, DEFAULT_SERVER_ADDR);
		mControlPort = mSharedPreferences.getString(CONTROLPORT_KEY, DEFAULT_SERVER_PORT);
		mVideoAddr = mSharedPreferences.getString(VEDIOADDRESS_KEY, DEFAULT_VEDIO_ADDR);
	}
}