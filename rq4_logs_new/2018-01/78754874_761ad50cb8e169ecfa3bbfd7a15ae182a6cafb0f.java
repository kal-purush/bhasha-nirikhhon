package jp.co.thcomp.ble_sample;

import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattService;
import android.database.DataSetObserver;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.SwitchCompat;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CompoundButton;
import android.widget.ExpandableListAdapter;
import android.widget.ExpandableListView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jp.co.thcomp.bluetoothhelper.BleCentral;
import jp.co.thcomp.bluetoothhelper.BlePeripheral;
import jp.co.thcomp.bluetoothhelper.FoundLeDevice;
import jp.co.thcomp.util.ToastUtil;

public class MainActivity extends AppCompatActivity {
    private static final boolean ModePeripheral = false;
    private static final boolean ModeCentral = true;

    private BlePeripheral mBlePeripheral;
    private BleCentral mBleCentral;
    private PeripheralListAdapter mPeripheralListAdapter;
    private CharacteristicListAdapter mCharacteristicListAdapter;
    private Map<BluetoothDevice, FoundLeDevice> mFoundLeDeviceMap = new HashMap<>();
    private Map<FoundLeDevice, List<BluetoothGattService>> mDeviceServiceMap = new HashMap<>();
    private ExpandableListView mPeripheralExListView;
    private RecyclerView mCharacteristicRecyclerView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        ((SwitchCompat) findViewById(R.id.swWorkMode)).setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton compoundButton, boolean checked) {
                changeBLEMode(checked);
            }
        });
        ((SwitchCompat) findViewById(R.id.swWorkSwitch)).setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton compoundButton, boolean checked) {
                // 念のため、Peripheral / Centralのいずれかで初期化されているか確認し、されていない場合は初期化する
                if ((mBleCentral == null) && (mBlePeripheral == null)) {
                    boolean workMode = ((SwitchCompat) findViewById(R.id.swWorkMode)).isChecked();
                    if (workMode == ModeCentral) {
                        startBleCentral();
                    } else {
                        startBlePeripheral();
                    }
                }

                changeViewStatus(checked);
            }
        });

        mPeripheralListAdapter = new PeripheralListAdapter();
        mCharacteristicListAdapter = new CharacteristicListAdapter();
        (mCharacteristicRecyclerView = (RecyclerView) findViewById(R.id.rvCharacteristicList)).setAdapter(mCharacteristicListAdapter);
        (mPeripheralExListView = (ExpandableListView) findViewById(R.id.elvPeripheralList)).setAdapter(mPeripheralListAdapter);
    }

    private void changeViewStatus(boolean startBle) {
        SwitchCompat swWorkMode = findViewById(R.id.swWorkMode);

        // WorkMode用のSwitchを表示切替
        swWorkMode.setEnabled(!startBle);

        // BLEのモード向けに表示を切り替える
        if (startBle) {
            if (swWorkMode.isChecked() == ModeCentral) {
                findViewById(R.id.flCentralArea).setVisibility(View.VISIBLE);
                findViewById(R.id.flPeripheralArea).setVisibility(View.GONE);
            } else {
                findViewById(R.id.flCentralArea).setVisibility(View.GONE);
                findViewById(R.id.flPeripheralArea).setVisibility(View.VISIBLE);
            }
        } else {
            if (swWorkMode.isChecked() == ModeCentral) {
                findViewById(R.id.flCentralArea).setVisibility(View.GONE);
            } else {
                findViewById(R.id.flPeripheralArea).setVisibility(View.GONE);
            }
        }
    }

    private void changeBLEMode(boolean started) {
        if (started) {
            SwitchCompat swWorkMode = findViewById(R.id.swWorkMode);

            if (swWorkMode.isChecked() == ModeCentral) {
                startBleCentral();
            } else {
                startBlePeripheral();
            }
        } else {
            if (mBlePeripheral != null) {
                mBlePeripheral.stop();
                mBlePeripheral = null;
            }
            if (mBleCentral != null) {
                mBleCentral.stop();
                mBleCentral = null;
            }
        }
    }

    private void startBleCentral() {
        if (mBleCentral == null) {
            mBleCentral = new BleCentral(this);
            mBleCentral.setOnBluetoothStatusListener(new BleCentral.OnBluetoothStatusListener() {
                @Override
                public void onStatusChange(int status, int scanMode) {
                    switch (status) {
                        case BleCentral.StatusNoSupportBluetooth:
                            ToastUtil.showToast(MainActivity.this, R.string.nosupport_bluetooth, Toast.LENGTH_LONG);
                            finish();
                            break;
                        case BleCentral.StatusDisableBluetooth:
                            ToastUtil.showToast(MainActivity.this, R.string.disable_bluetooth, Toast.LENGTH_LONG);
                            finish();
                            break;
                        case BleCentral.StatusStartBluetooth:
                            mBleCentral.startScanPeripheral(0);
                            break;
                        default:
                            break;
                    }
                }
            });
            mBleCentral.setOnFoundLeDeviceListener(new BleCentral.OnFoundLeDeviceListener() {
                @Override
                public void onFoundLeDevice(FoundLeDevice device) {
                    mFoundLeDeviceMap.put(device.getDevice(), device);
                    mBleCentral.discoverServices(device.getDevice(), new BleCentral.OnServicesDiscoveredListener() {
                        @Override
                        public void onServicesDiscovered(BluetoothDevice device, List<BluetoothGattService> serviceList) {
                            FoundLeDevice foundLeDevice = mFoundLeDeviceMap.get(device);
                            if (foundLeDevice != null) {
                                mDeviceServiceMap.put(foundLeDevice, serviceList);
                                mPeripheralListAdapter.dataSetChange();
                            }
                        }
                    });
                }

                @Override
                public void onTimeoutFindLeDevice() {

                }
            });
            mBleCentral.start();
        }
    }

    private void startBlePeripheral() {
        if (mBlePeripheral == null) {
            mBlePeripheral = new BlePeripheral(this);
            mBlePeripheral.setOnBluetoothStatusListener(new BlePeripheral.OnBluetoothStatusListener() {
                @Override
                public void onStatusChange(int status, int scanMode) {
                    switch (status) {
                        case BlePeripheral.StatusNoSupportBluetooth:
                            ToastUtil.showToast(MainActivity.this, R.string.nosupport_bluetooth, Toast.LENGTH_LONG);
                            finish();
                            break;
                        case BlePeripheral.StatusDisableBluetooth:
                            ToastUtil.showToast(MainActivity.this, R.string.disable_bluetooth, Toast.LENGTH_LONG);
                            finish();
                            break;
                        case BlePeripheral.StatusDisableBleAdvertising:
                            ToastUtil.showToast(MainActivity.this, R.string.disable_ble_advertising, Toast.LENGTH_LONG);
                            finish();
                            break;
                        case BlePeripheral.StatusStartBleAdvertising:
                            break;
                        default:
                            break;
                    }
                }
            });
            mBlePeripheral.setOnServiceChangedListener(new BlePeripheral.OnServiceChangedListener() {
                @Override
                public boolean onPreCharacteristicChanged(BluetoothDevice device, int requestId, BluetoothGattCharacteristic characteristic, boolean preparedWrite, boolean responseNeeded, int offset, byte[] value) {
                    return false;
                }

                @Override
                public void onCharacteristicChanged(BluetoothGattCharacteristic changedCharacteristic) {

                }
            });
            mBlePeripheral.start();
        }
    }

    private class PeripheralListAdapter implements ExpandableListAdapter {
        private DataSetObserver mObserver;
        private FoundLeDevice[] mFoundLeDeviceArray = new FoundLeDevice[0];

        @Override
        public void registerDataSetObserver(DataSetObserver dataSetObserver) {
            mObserver = dataSetObserver;
        }

        @Override
        public void unregisterDataSetObserver(DataSetObserver dataSetObserver) {
            if (mObserver != null && mObserver.equals(dataSetObserver)) {
                mObserver = null;
            }
        }

        public void dataSetChange() {
            if (mObserver != null) {
                if (mBleCentral != null) {
                    mFoundLeDeviceArray = mBleCentral.getFoundLeDeviceList();
                } else {
                    mFoundLeDeviceArray = new FoundLeDevice[0];
                }
                mObserver.onChanged();
            }
        }

        @Override
        public int getGroupCount() {
            return mFoundLeDeviceArray.length;
        }

        @Override
        public int getChildrenCount(int i) {
            return 0;
        }

        @Override
        public Object getGroup(int i) {
            return mFoundLeDeviceArray[i];
        }

        @Override
        public Object getChild(int i, int i1) {
            return null;
        }

        @Override
        public long getGroupId(int i) {
            return i;
        }

        @Override
        public long getChildId(int i, int i1) {
            return (((long) i) << Integer.SIZE) | i1;
        }

        @Override
        public boolean hasStableIds() {
            return false;
        }

        @Override
        public View getGroupView(int i, boolean b, View convertView, ViewGroup parent) {
            if (convertView == null) {
                convertView = getLayoutInflater().inflate(R.layout.item_peripheral_list, parent, false);
            }

            if (mBleCentral != null) {
                FoundLeDevice foundLeDevice = mFoundLeDeviceArray[i];
                BluetoothDevice device = foundLeDevice.getDevice();
                StringBuilder contentBuilder = new StringBuilder();

                String content = device.getName();
                if (content != null && content.length() > 0) {
                    contentBuilder.append(getString(R.string.ble_device_name)).append(": ").append(content).append("\n");
                }
                content = device.getAddress();
                if (content != null && content.length() > 0) {
                    contentBuilder.append(getString(R.string.ble_device_address)).append(": ").append(content).append("\n");
                }
                ((TextView) convertView.findViewById(R.id.tvPeripheralInformation)).setText(content);
            }

            return convertView;
        }

        @Override
        public View getChildView(int i, int i1, boolean b, View convertView, ViewGroup parent) {
            if (convertView == null) {
                convertView = getLayoutInflater().inflate(R.layout.item_peripheral_list, parent, false);
            } else {
                ((ViewGroup) convertView.findViewById(R.id.llCharacteristicList)).removeAllViews();
            }

            if (mBleCentral != null) {
                FoundLeDevice foundLeDevice = mFoundLeDeviceArray[i];
                List<BluetoothGattService> serviceList = foundLeDevice.getServiceList();
                BluetoothDevice device = foundLeDevice.getDevice();
                StringBuilder contentBuilder = new StringBuilder();

                if (serviceList != null && serviceList.size() > i1) {
                    BluetoothGattService service = serviceList.get(i1);
                    List<BluetoothGattCharacteristic> characteristicList = service.getCharacteristics();
                    mBleCentral.readCharacteristic(device, a, new BleCentral.OnCharacteristicReadListener() {
                        @Override
                        public void onCharacteristicRead(BluetoothGatt gatt, BluetoothGattCharacteristic characteristic, int status) {

                        }
                    });
                    contentBuilder.append(getString(R.string.ble_characteristic)).append(": ").append(service.getCharacteristics())
                }

                String content = device.getName();
                if (content != null && content.length() > 0) {
                    contentBuilder.append(getString(R.string.ble_device_name)).append(": ").append(content).append("\n");
                }
                content = device.getAddress();
                if (content != null && content.length() > 0) {
                    contentBuilder.append(getString(R.string.ble_device_address)).append(": ").append(content).append("\n");
                }
                ((TextView) convertView.findViewById(R.id.tvPeripheralInformation)).setText(content);
            }

            return convertView;
        }

        @Override
        public boolean isChildSelectable(int i, int i1) {
            return false;
        }

        @Override
        public boolean areAllItemsEnabled() {
            return false;
        }

        @Override
        public boolean isEmpty() {
            boolean isEmpty = true;

            if (mBleCentral != null) {
                FoundLeDevice[] foundLeDeviceArray = mBleCentral.getFoundLeDeviceList();
                isEmpty = ((foundLeDeviceArray == null) || (foundLeDeviceArray.length == 0));
            }

            return isEmpty;
        }

        @Override
        public void onGroupExpanded(int i) {
            // 処理なし
        }

        @Override
        public void onGroupCollapsed(int i) {
            // 処理なし
        }

        @Override
        public long getCombinedChildId(long l, long l1) {
            return 0;
        }

        @Override
        public long getCombinedGroupId(long l) {
            return 0;
        }
    }

    private class CharacteristicListAdapter extends RecyclerView.Adapter {

        @Override
        public RecyclerView.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
            return null;
        }

        @Override
        public void onBindViewHolder(RecyclerView.ViewHolder holder, int position) {

        }

        @Override
        public int getItemCount() {
            return 0;
        }
    }
}