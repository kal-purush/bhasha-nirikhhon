package com.inuker.bluetooth;

import android.os.Handler;
import android.view.View;

import com.inuker.bluetooth.library.BluetoothClient;
import com.inuker.bluetooth.library.connect.response.BleWriteResponse;
import com.inuker.bluetooth.library.utils.BluetoothLog;
import com.inuker.bluetooth.library.utils.ByteUtils;

import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import static com.inuker.bluetooth.library.Constants.REQUEST_SUCCESS;

public class Fota {
    private String packagePath;
    private BluetoothClient mClient;
    private String MAC;
    private String serviceUUID;
    private String w_UUID;
    private Handler mHandler;

    public Fota(String packagePath,
                BluetoothClient client,
                String mac,
                String serviceUUID,
                String w_UUID,
                Handler handler
    ){
        this.packagePath = packagePath;
        mClient = client;
        MAC = mac;
        this.serviceUUID = serviceUUID;
        this.w_UUID = w_UUID;
        mHandler = handler;
    }

    private final BleWriteResponse mSendPackageRequestRsp = new BleWriteResponse() {
        @Override
        public void onResponse(int code) {
            if (code == REQUEST_SUCCESS) {
                //CommonUtils.toast("---------------send package request success");
                System.out.println("---------------send package request success");
            } else {
                //CommonUtils.toast("--------------failed");
                System.out.println("---------------send package request failed");
            }
        }
    };

    int packUpdateInfo(){
        ByteBuffer bb = ByteBuffer.allocate(250).order(
                ByteOrder.BIG_ENDIAN);
        if(false == BleUtils.isFileExit("/sdcard/Download/update.zip"))
        {
            CommonUtils.toast("/sdcard/Download/update.zip文件不存在");
            return -1;
        }
        System.out.println("--------------------calc md5:"+BleUtils.getFileMD5(packagePath));
        System.out.println("--------------------file size:"+BleUtils.getFileSize(packagePath));
        JSONObject fota_json = new JSONObject();
        try {
            fota_json.put("fota_data_size", BleUtils.getFileSize(packagePath));
            fota_json.put("fota_data_hash_value",BleUtils.getFileMD5(packagePath));
        }catch(Exception e){
            CommonUtils.toast("create JSON failed");
            return -2;
        }
        String str = fota_json.toString();
        System.out.println("--------------------json :"+str);
        System.out.println("--------------------json size:"+Integer.toHexString(str.length()));
        bb.put(ByteUtils.stringToBytes("F583"+Integer.toHexString(str.length())));
        bb.put(str.getBytes());
        BluetoothLog.w("fota json write:"+ BleUtils.bytes2hex(bb.array()));

        byte[] bytes = Arrays.copyOfRange(bb.array(),0,str.length() + 3) ;
        BluetoothLog.w("fota json send:"+ BleUtils.bytes2hex(bytes));

        ClientManager.getClient().write(MAC, UUID.fromString(serviceUUID),UUID.fromString(w_UUID),
                bytes, mSendPackageRequestRsp);

        return 0;
    }

    int start(){
        int ret = 0;
        ret = packUpdateInfo();
        if(ret < 0)
            return ret;

        System.out.println("-------------start send fota package");
        //mBtnSendPackage.setEnabled(false);
        //mBtnSendPackage.setText("start send fota package");
        //progressBar.setVisibility(View.VISIBLE);
        BleUtils.getInstance().clearNotify();
        //xmodem x = new xmodem(mClient,MAC,serviceUUID,w_UUID);
        //x.send(packagePath,mHandler);
        XmodemV2 x = new XmodemV2(mClient,MAC,serviceUUID,w_UUID,packagePath,mHandler);
        x.send();
        return 0;
    }
}