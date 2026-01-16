package com.heytz.ble;

import android.bluetooth.BluetoothDevice;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import com.heytz.ble.sdk.BleGattCharacteristic;
import com.heytz.ble.sdk.BleService;
import com.heytz.ble.sdk.IBle;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.cordova.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.UUID;

/**
 * This class echoes a string called from JavaScript.
 */
public class HeytzBle extends CordovaPlugin {

    // actions
    private static final String STARTSCAN = "startScan";
    private static final String STOPSCAN = "stopScan";
    private static final String IS_ENABLED = "isEnabled";
    private static final String CONNECT = "connect";
    private static final String DISCONNECT = "disconnect";
    private static final String STARTNOTIFICATION = "startNotification"; // register for characteristic notification
    private static final String WRITE = "write";

    private static final long SCAN_PERIOD = 10000;
    private static final String TAG = "HeytzBle";

    private BleService mService;
    private IBle mBle;
    private Handler mHandler;
    private BleGattCharacteristic mCharacteristic;
    private String mDeviceAddress;          //当前监听的设备
    private boolean mNotifyStarted;
    private Context context;
    private CallbackContext _callbackcontext;
    private CallbackContext rawDataAvailableCallback;

    private final BroadcastReceiver mBleReceiver = new BroadcastReceiver() {

        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (BleService.BLE_NOT_SUPPORTED.equals(action)) {
                LOG.w(TAG, "Ble not support");
                //todo 不支持ble
            } else if (BleService.BLE_DEVICE_FOUND.equals(action)) {
                // device found
                Bundle extras = intent.getExtras();
                final BluetoothDevice device = extras
                        .getParcelable(BleService.EXTRA_DEVICE);
                //todo 获取到新的device;
                LOG.w(TAG, device.getName());

                PluginResult result = new PluginResult(PluginResult.Status.OK, deviceToJSONObject(device));
                result.setKeepCallback(true);
                _callbackcontext.sendPluginResult(result);

            } else if (BleService.BLE_NO_BT_ADAPTER.equals(action)) {
                LOG.w(TAG, "No bluetooth adapter");
                //todo 没有蓝牙设备
            }


            Bundle extras = intent.getExtras();
            if (!mDeviceAddress.equals(extras.getString(BleService.EXTRA_ADDR))) {
                return;
            }

            String uuid = extras.getString(BleService.EXTRA_UUID);
            if (uuid != null
                    && !mCharacteristic.getUuid().toString().equals(uuid)) {
                return;
            }


            if (BleService.BLE_GATT_DISCONNECTED.equals(action)) {//如果蓝牙断开链接.
                LOG.w(TAG, "Device disconnected...");

            } else if (BleService.BLE_CHARACTERISTIC_READ.equals(action)
                    || BleService.BLE_CHARACTERISTIC_CHANGED.equals(action)) {  //当蓝牙设备有消息读取或者消息改变,
                byte[] data = extras.getByteArray(BleService.EXTRA_VALUE);
//                tv_ascii.setText(new String(val));
//                tv_hex.setText("0x" + new String(Hex.encodeHex(val)));
                if (data != null && data.length > 0) {
                    PluginResult result = new PluginResult(PluginResult.Status.OK, data);
                    result.setKeepCallback(true);
                    rawDataAvailableCallback.sendPluginResult(result);
                }

            } else if (BleService.BLE_CHARACTERISTIC_NOTIFICATION
                    .equals(action)) {//通知的状态.
                LOG.w(TAG, "Notification state changed!");
                mNotifyStarted = extras.getBoolean(BleService.EXTRA_VALUE);
                if (mNotifyStarted) {
                    LOG.w(TAG, "Stop Notify");
                } else {
                    LOG.w(TAG, "Start Notify");
                }
            } else if (BleService.BLE_CHARACTERISTIC_INDICATION.equals(action)) {//指示状态改变
                LOG.w(TAG, "Indication state changed!");
            } else if (BleService.BLE_CHARACTERISTIC_WRITE.equals(action)) {//写入消息成功.
                LOG.w(TAG, "Write success!");
            }
        }


    };

    public JSONObject deviceToJSONObject(BluetoothDevice device) {

        JSONObject json = new JSONObject();

        try {
            json.put("id", device.getAddress()); // mac address
            json.put("getUuids", device.getUuids());
            json.put("name", device.getName());
            json.put("address", device.getAddress()); // mac address
            json.put("getBluetoothClass", device.getBluetoothClass());
            json.put("getBondState", device.getBondState());
            json.put("getType", device.getType());
            json.put("getClass", device.getClass());
            json.put("describeContents", device.describeContents());
        } catch (JSONException e) { // this shouldn't happen
            e.printStackTrace();
        }

        return json;
    }

    @Override
    public void initialize(CordovaInterface cordova, CordovaWebView webView) {
        // your init code here
        super.initialize(cordova, webView);
        context = cordova.getActivity().getApplicationContext();
        context.registerReceiver(mBleReceiver, BleService.getIntentFilter());
        mHandler = new Handler();

    }


    private void initmBle() {
        BleApplication app = (BleApplication) cordova.getActivity().getApplication();
        mBle = app.getIBle();
    }

    @Override
    public boolean execute(String action, JSONArray args, CallbackContext callbackContext) throws JSONException {
        _callbackcontext = callbackContext;
        this.initmBle();
        /**
         * 扫描蓝牙设备
         */
        if (action.equals(STARTSCAN)) {
            this.startScan();
            return true;
        }
        /**
         * 停止扫描
         */
        if (action.equals(STOPSCAN)) {
            this.stopScan();
            return true;
        }
        /**
         * 检查是否启用蓝牙适配器。
         */
        if (action.equals(IS_ENABLED)) {
            if (mBle != null) {
                if (mBle.adapterEnabled()) {
                    callbackContext.success();
                } else {
                    callbackContext.error("Bluetooth is disabled.");
                }
            }
        }
        /**
         * 链接设备
         */
        if (action.equals(CONNECT)) {
            String macAddress = args.getString(0);
            this.connect(macAddress, callbackContext);
        }
        /**
         * 开始监听消息.
         */
        if (action.equals(STARTNOTIFICATION)) {
            String macAddress = args.getString(0);
            UUID serviceUUID = uuidFromString(args.getString(1));
            UUID characteristicUUID = uuidFromString(args.getString(2));

            this.startNotification(macAddress, serviceUUID, characteristicUUID, callbackContext);
        }
        if (action.equals(WRITE)) {
            String macAddress = args.getString(0);
            UUID serviceUUID = uuidFromString(args.getString(1));
            UUID characteristicUUID = uuidFromString(args.getString(2));
            String val = args.getString(3);
            this.write(macAddress, serviceUUID, characteristicUUID, val);
        }
        return false;
    }

    private void startScan() {
        scanLeDevice(true);
    }

    private void stopScan() {
        scanLeDevice(false);
    }

    private void scanLeDevice(final boolean enable) {

        if (mBle == null) {
            return;
        }
        if (enable) {
            // Stops scanning after a pre-defined scan period.
            mHandler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    if (mBle != null) {
                        mBle.stopScan();
                    }
                }
            }, SCAN_PERIOD);
            if (mBle != null) {
                mBle.startScan();
            }
        } else {
            if (mBle != null) {
                mBle.stopScan();
            }
        }
        PluginResult result = new PluginResult(PluginResult.Status.NO_RESULT);
        result.setKeepCallback(true);
        _callbackcontext.sendPluginResult(result);
    }

    private void connect(String macAddress, CallbackContext callbackContext) {
        if (mBle.requestConnect(macAddress)) {
            callbackContext.success();
        } else {
            callbackContext.error("Could not connect to " + macAddress);
        }
    }

    /**
     * 监听消息.
     *
     * @param macAddress
     * @param serviceUUID
     * @param characteristicUUID
     */
    private void startNotification(String macAddress, UUID serviceUUID, UUID characteristicUUID, CallbackContext callbackContext) {
        rawDataAvailableCallback = callbackContext;
        mDeviceAddress = macAddress;
        mCharacteristic = mBle.getService(macAddress, serviceUUID).getCharacteristic(characteristicUUID);
        mBle.requestCharacteristicNotification(macAddress, mCharacteristic);
    }

    /**
     * 写入消息
     */
    private void write(String macAddress, UUID serviceUUID, UUID characteristicUUID, String val) {
        try {
            mCharacteristic = mBle.getService(macAddress, serviceUUID).getCharacteristic(characteristicUUID);
            byte[] data = Hex.decodeHex(val.toCharArray());
            mCharacteristic.setValue(data);
            mBle.requestWriteCharacteristic(mDeviceAddress,
                    mCharacteristic, "");
        } catch (DecoderException e) {
            e.printStackTrace();
        }
    }

    private UUID uuidFromString(String uuid) {
        return HeytzUUIDHelper.uuidFromString(uuid);
    }
}