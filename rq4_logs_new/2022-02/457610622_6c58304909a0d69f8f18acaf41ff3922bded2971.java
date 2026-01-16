package com.example.myapplication.activity;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Log;
import android.view.View;

import com.example.myapplication.R;
import com.example.myapplication.bean.AnnotationTestBean;
import com.example.myapplication.databinding.ActivityJavaTestBinding;
import com.example.myapplication.databinding.ActivityTestSheJiBinding;
import com.example.myapplication.javatest.MyAnnotationTest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class JavaTestActivity extends AppCompatActivity implements View.OnClickListener {

    private ActivityJavaTestBinding binding;
    public static final String TAG = JavaTestActivity.class.toString();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityJavaTestBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());
        initAction();
    }

    private void initAction() {
        binding.btnZj.setOnClickListener(this::onClick);
    }

    @Override
    public void onClick(View v) {
        int id = v.getId();
        if (id == binding.btnZj.getId()) {
            /**
             * 　　方法1：<T extends Annotation> T getAnnotation(Class<T> annotationClass): 返回改程序元素上存在的、指定类型的注解，如果该类型注解不存在，则返回null。
             * 　　方法2：Annotation[] getAnnotations():返回该程序元素上存在的所有注解。
             * 　　方法3：boolean is AnnotationPresent(Class<?extends Annotation> annotationClass):判断该程序元素上是否包含指定类型的注解，存在则返回true，否则返回false.
             * 　　方法4：Annotation[] getDeclaredAnnotations()：返回直接存在于此元素上的所有注释。与此接口中的其他方法不同，该方法将忽略继承的注释。（如果没有注释直接存在于此元素上，则返回长度为零的一个数组。）该方法的调用者可以随意修改返回的数组；这不会对其他调用者返回的数组产生任何影响。
             */
            AnnotationTestBean bean = new AnnotationTestBean();
            /**
             * 利用反射解析注解
             */
            Class<? extends AnnotationTestBean> aClass = bean.getClass();
            //判断是否有注解
            if (aClass.isAnnotationPresent(MyAnnotationTest.class)) {
                Log.d(TAG, "MyAnnotationTest: the bean had the AnnotationTest");
                //获取该对象注解
                MyAnnotationTest annotation = aClass.getAnnotation(MyAnnotationTest.class);
                Log.d(TAG, "MyAnnotationTest: the isChange is : " + annotation.isChange() +
                        "the vaule is : " + annotation.value() + "the isCheck is : " + annotation.isCheck());

                //获取成员变量的注解
                try {
                    Field name = aClass.getField("name");
                    MyAnnotationTest.UseTest annotation1 = name.getAnnotation(MyAnnotationTest.UseTest.class);
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }

                //获取方法的注解
                try {
                    Method method = aClass.getMethod("method");
                    MyAnnotationTest.MethodTest annotation1 = method.getAnnotation(MyAnnotationTest.MethodTest.class);
                    Log.d(TAG, "method: the annotation value is "+annotation1.value());
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }

            }else {
                Log.d(TAG, "MyAnnotationTest: the bean do not have the AnnotationTest");
            }

        }
    }
}