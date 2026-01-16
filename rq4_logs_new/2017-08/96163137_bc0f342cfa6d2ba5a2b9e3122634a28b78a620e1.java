package com.yundian.celebrity;



import android.os.UserManager;
import android.text.Editable;
import android.view.View;

import com.yundian.celebrity.helper.CheckHelper;
import com.yundian.celebrity.ui.main.activity.LoginActivity;
import com.yundian.celebrity.widget.CheckException;
import com.yundian.celebrity.widget.WPEditText;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Created by xiaochuang on 4/29/16.
 */
public class LoginPresenterTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    LoginActivity loginActivity;

    @Mock
    CheckHelper checkHelper;

    @Mock
    CheckException checkException;

    @Mock
    CheckException checkException1;


    @Test
    public void testLogin() {
//        CheckException checkException = new CheckException();
        Mockito.when(checkHelper.checkMobile(anyString(),checkException)).thenReturn(true);
        Mockito.when(checkHelper.checkPassword(anyString(),checkException)).thenReturn(true);

//        CheckException checkException1 = new CheckException();
        loginActivity.loging();
//        Mockito.verify(checkHelper).checkMobile(anyString(),checkException);
//        Assert.assertEquals(true, checkHelper.checkMobile("xiaochuangishandsome",new CheckException()));
//        LoginActivity loginActivity = Mockito.mock(LoginActivity.class);
//        WPEditText  passwordEditText= (WPEditText)loginActivity.findViewById(R.id.passwordEditText);
//        Editable text = passwordEditText.getEditText().getText();
//        Editable text1 = passwordEditText.getEditText().getText();



//        Mockito.verify(checkHelper).(anyString(), anyString());

    }

//    @Test
//    @Spec(desc = "should mock return given value")
//    public void test() {
//        PasswordValidator validator = Mockito.mock(PasswordValidator.class);
//        Mockito.when(validator.verifyPassword("xiaochuangishandsome")).thenReturn(true);
//        Assert.assertEquals(true, validator.verifyPassword("xiaochuangishandsome"));
//
//        Mockito.when(validator.verifyPassword(anyString())).thenReturn(true);
//        Assert.assertEquals(true, validator.verifyPassword("xiaochuangishandsome11"));
//
//    }
//
//    @Test
//    @Spec(desc = "should mock perform certain action")
//    public void testMockAnswer() {
//        UserManager mockUserManager = Mockito.mock(UserManager.class);
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                //这里可以获得传给performLogin的参数
//                Object[] arguments = invocation.getArguments();
//
//                NetworkCallback callback = (NetworkCallback) arguments[2];
//                callback.onFailure(500, "Server error");
//                return 500; //对于如果mock的是非void方法来说，这个将作为目标方法的返回值
//            }
//        }).when(mockUserManager).performLogin(anyString(), anyString(), any(NetworkCallback.class));
//
//
//        mockUserManager.performLogin("xiaochuang", "xiaochuang password", Mockito.mock(NetworkCallback.class));
//    }
//
//    @Test
//    public void testSpy() {
//        //跟创建mock类似，只不过调用的是spy方法，而不是mock方法。spy的用法
//        PasswordValidator spyValidator = Mockito.spy(PasswordValidator.class);
//
//        //在默认情况下，spy对象会调用这个类的real implementation，并返回相应的返回值
//        boolean result = spyValidator.verifyPassword("xiaochuang_is_handsome");//true
//        Assert.assertTrue(result);
//        result = spyValidator.verifyPassword("xiaochuang_is_not_handsome"); //false
//        Assert.assertFalse(result);
//
//        //也可以指定spy对象的方法的行为
//        Mockito.when(spyValidator.verifyPassword(anyString())).thenReturn(true);
//
//        result = spyValidator.verifyPassword("xiaochuang_is_not_handsome");
//        Assert.assertTrue(result);
//        Mockito.verify(spyValidator, Mockito.times(2)).verifyPassword("xiaochuang_is_not_handsome");
//
//    }
}