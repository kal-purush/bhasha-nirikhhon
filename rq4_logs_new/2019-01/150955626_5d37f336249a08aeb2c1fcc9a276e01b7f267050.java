package wangfeixixi.cip.ui.mvvm;

import android.arch.lifecycle.ViewModelProviders;
import android.databinding.Observable;
import android.databinding.ObservableList;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.view.animation.Animation;
import android.view.animation.RotateAnimation;

import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.Subscribe;
import org.greenrobot.eventbus.ThreadMode;

import java.util.ArrayList;

import me.tatarka.bindingcollectionadapter2.BR;
import wangfeixixi.cip.R;
import wangfeixixi.cip.beans.JsonRootBean;
import wangfeixixi.cip.databinding.MainActivityBinding;
import wangfeixixi.cip.widget.carview.CarBean;
import wangfeixixi.cip.widget.carview.utils.BitmapUtils;
import wangfeixixi.cip.widget.compass.Compass;
import wangfeixixi.cip.widget.compass.SOTWFormatter;
import wangfeixixi.com.base.UIUtils;
import wangfeixixi.com.base.mvvm.base.BaseActivity;
import wangfeixixi.lbs.LocationInfo;
import wangfeixixi.lbs.gaode.GaodeMapService;

public class MainActivity extends BaseActivity<MainActivityBinding, MainViewModel> {

    private GaodeMapService mLbs;

    @Override
    public int initContentView(Bundle bundle) {
        return R.layout.main_activity;
    }

    @Override
    public int initVariableId() {
        return BR.mainViewModel;
    }

    @Override
    public MainViewModel initViewModel() {
        return ViewModelProviders.of(this).get(MainViewModel.class);
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        mLbs.onCreate(savedInstanceState);
    }

    @Override
    public void initData() {
        super.initData();
        EventBus.getDefault().register(this);
        mLbs = new GaodeMapService(this);
        binding.flMapContainer.addView(mLbs.getMap());

        sotwFormatter = new SOTWFormatter(this);
        setupCompass();
    }

    @Override
    public void initViewObservable() {
        viewModel.carviewSwitchSpeed.addOnPropertyChangedCallback(new Observable.OnPropertyChangedCallback() {
            @Override
            public void onPropertyChanged(Observable sender, int propertyId) {
                binding.carview.switchSpeed(viewModel.carviewSwitchSpeed.get());
                updateLbs(viewModel.carBeans);

//                binding.carview.updateBodys(viewModel.carBeans.toArray(new CarBean[viewModel.carBeans.size()]));

            }
        });

        viewModel.carBeas.addOnPropertyChangedCallback(new Observable.OnPropertyChangedCallback() {
            @Override
            public void onPropertyChanged(Observable sender, int propertyId) {
//                binding.carview.updateBodys(viewModel.carBeas.get());
            }
        });

        viewModel.carBeans.addOnListChangedCallback(new ObservableList.OnListChangedCallback<ObservableList<CarBean>>() {
            @Override
            public void onChanged(ObservableList<CarBean> sender) {
//                binding.carview.updateBodys(sender.toArray(new CarBean[sender.size()]));
            }

            @Override
            public void onItemRangeChanged(ObservableList<CarBean> sender, int positionStart, int itemCount) {

            }

            @Override
            public void onItemRangeInserted(ObservableList<CarBean> sender, int positionStart, int itemCount) {

            }

            @Override
            public void onItemRangeMoved(ObservableList<CarBean> sender, int fromPosition, int toPosition, int itemCount) {

            }

            @Override
            public void onItemRangeRemoved(ObservableList<CarBean> sender, int positionStart, int itemCount) {

            }
        });
    }

    private Compass compass;
    private float currentAzimuth;
    private SOTWFormatter sotwFormatter;

    private void setupCompass() {
        compass = new Compass(this);
        Compass.CompassListener cl = getCompassListener();
        compass.setListener(cl);
    }

    private void adjustArrow(float azimuth) {
        Animation an = new RotateAnimation(-currentAzimuth, -azimuth,
                Animation.RELATIVE_TO_SELF, 0.5f, Animation.RELATIVE_TO_SELF,
                0.5f);
        currentAzimuth = azimuth;

        an.setDuration(500);
        an.setRepeatCount(0);
        an.setFillAfter(true);
    }

    private Compass.CompassListener getCompassListener() {
        return new Compass.CompassListener() {
            @Override
            public void onNewAzimuth(final float azimuth) {
                // UI updates only in UI thread
                // https://stackoverflow.com/q/11140285/444966
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        adjustArrow(azimuth);
//                        tv_warning.setText(sotwFormatter.format(azimuth));
                    }
                });
            }
        };
    }


    private void updateLbs(ArrayList<CarBean> list) {
        mLbs.clearAllMarker();
        for (int i = 0; i < list.size(); i++) {
            mLbs.addOrUpdateMarker(new LocationInfo(String.valueOf(i), list.get(i).latitude, list.get(i).longitude), BitmapUtils.scaleBitmap(BitmapFactory.decodeResource(UIUtils.getResources(), R.mipmap.car_other), 0.1f));
        }
        mLbs.moveCamera(new LocationInfo("自身", list.get(0).latitude, list.get(0).longitude), 20);
    }

    @Override
    protected void onResume() {
        super.onResume();
        mLbs.onResume();
        compass.start();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        mLbs.onDestroy();
        if (EventBus.getDefault().isRegistered(this)) {
            EventBus.getDefault().unregister(this);
        }
    }

    @Override
    protected void onPause() {
        super.onPause();
        mLbs.onPause();
        compass.stop();
    }

    @Override
    protected void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        mLbs.onSaveInstanceState(outState);
    }

    @Subscribe(threadMode = ThreadMode.MAIN)
    public void receiveCars(JsonRootBean bean) {
        viewModel.receiveCars(bean);
    }
}