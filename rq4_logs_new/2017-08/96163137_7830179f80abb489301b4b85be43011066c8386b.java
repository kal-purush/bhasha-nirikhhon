package com.yundian.celebrity;

import com.yundian.celebrity.bean.LoginReturnInfo;
import com.yundian.celebrity.listener.IDataRequestListener;
import com.yundian.celebrity.ui.main.contract.LoginContract;
import com.yundian.celebrity.ui.main.model.LoginModel;
import com.yundian.celebrity.ui.main.presenter.LoginPresenter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.robolectric.RobolectricGradleTestRunner;
import org.robolectric.annotation.Config;
import org.robolectric.shadows.ShadowLog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;

/**
 * 接口部分使用真实数据，只进行了view的mock测试，验证各种数据返回后的view的处理是否符合预期
 */
@RunWith(CustomTestRunner.class)
@Config(shadows = {ShadowLog.class},constants = BuildConfig.class, sdk = 21,packageName="com.yundian.celebrity",application=TestAppliction.class)
public class LoginModelTest {

    private LoginPresenter presenter;

    @Mock
    private LoginContract.View view;

//    @Spy
//    private LoginModel loginModel;

    @Mock
    private IDataRequestListener listener;

    @Mock
    private LoginReturnInfo loginReturnInfo;

    private String errorPhone;
    private String successPhone;
    private String errorPassword;
    private String successPassword;
    private LoginModel loginModel;

    @Before
    public void setupMocksAndView() {
        // Mockito has a very convenient way to inject mocks by using the @Mock annotation. To
        // inject the mocks in the test the initMocks method needs to be called.
        MockitoAnnotations.initMocks(this);

//        // The presenter wont't update the view unless it's active.
//        when(view.isActive()).thenReturn(true);

//        // 设置标识，用于区分处理Observer的线程等情况
//        BoreConstants.isUnitTest = true;
        errorPhone = "18551681236";
        errorPassword = "123455";

        successPassword = "123456";
        successPhone = "18657195470";

        loginModel = new LoginModel();
//        presenter = new LoginPresenter(view);
//        presenter.setLoginModel(loginModel);
    }

    @Test
    public void testLoginModelError() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1); //创建CountDownLatch

        loginModel.login(errorPhone,errorPassword,new IDataRequestListener() {

            @Override
            public void loadSuccess(Object object) {
                latch.countDown();

            }

            @Override
            public void loadFail(Object object) {
                latch.countDown();
            }
        });
        latch.await();
//        assertEquals(1, result.size());

        PowerMockito.verifyPrivate(loginModel).invoke("requestError", listener);
//        verify(loginModel).requestError(listener);
    }

    @Test
    public void testLoginModelSuccess() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1); //创建CountDownLatch
        IDataRequestListener listener = new IDataRequestListener() {

            @Override
            public void loadSuccess(Object object) {
                latch.countDown();
            }

            @Override
            public void loadFail(Object object) {
                latch.countDown();
            }
        };
        loginModel=new LoginModel();


        loginModel.login(successPhone,successPassword, listener);

        latch.await();
//        assertEquals(1, result.size());

        PowerMockito.verifyPrivate(loginModel).invoke("wangyiRegister", successPhone,loginReturnInfo, listener);
//        verify(loginModel).requestError(listener);
    }

}