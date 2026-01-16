package com.example.myapplication.javatest;

import android.util.Log;

import com.example.myapplication.bean.HookBean;

import java.io.IOException;

import me.ele.lancet.base.Origin;
import me.ele.lancet.base.Scope;
import me.ele.lancet.base.annotations.ClassOf;
import me.ele.lancet.base.annotations.Insert;
import me.ele.lancet.base.annotations.Proxy;
import me.ele.lancet.base.annotations.TargetClass;

/**
 * 轻量级Android AOP框架Lancet测试
 *
 * AOP(Aspect Oriented Programming)是面向切面编程，AOP和我们平时接触的OOP编程是不同的编程思想，OOP是面向对象
 * 编程，提倡的是将功能模块化，对象化。而AOP的思想则是提倡针对同一类问题统一处理，当然，我们在实际编程过程中，不
 * 可能单纯的AOP或者OOP的思想来编程，很多时候，可能会混合多种编程思想。
 * 代码注入是AOP中的重要部分：AOP可用于日志埋点、性能监控、动态权限控制、甚至是代码调试等等
 *
 * 通俗的讲，AOP就是将日志记录、性能统计、安全控制、事务处理、异常处理代码从业务逻辑代码中划分出来，通过这些行为
 * 的分离，我们希望可以将它们独立到一个类中，进而改变这些行为的时候不影响业务逻辑的代码–解耦
 *
 * 实现AOP的技术，主要分为两大类：
 *
 * 采用动态代理技术，利用截取消息的方式，对该信息进行装饰，以取代原有对象行为的执行
 * 采用静态织入的方式，引入特定的语句创建“方面”，从而使得编译器可以在编译期间织入有关“方面的代码
 * 有一些AOP术语需要我们理解：
 *
 * Cross-cutting concerns(横切关注点)：监管面向对象模型中大多数类会实现单一特定的功能，但通常也会开放一些通用
 * 的附属功能给其他类。例如，我们喜欢在数据访问层中的类添加日志，同时也希望当UI层中一个县城进入或者退出调用一个
 * 方法时添加日志。监管每个类都有一个区别于其他类的主要功能，但在代码里，仍然经常需要添加一些相同的附属功能。
 * Advice(通知)：注入到class文件中的代码。典型的Advice类型有before、after和around，分别表示在目标方法执行之
 * 前、执行后和完全代替目标方法执行的代码。除了在方法中注入代码，也可能会对代码做其他修改，比如在一个class中增加
 * 字段或者接口。
 * **Joint Point（连接点）：程序中可能作为代码注入目标的特定的点，例如一个方法调用或者方法入口。
 * Pointcut（切入点）：告诉代码注入工具，在任何注入一段特定代码的表达式。例如，在哪些joint points应用一个特定的
 * Advice。切入点可以选择唯一一个，比如执行某一个方法，也可以有多个选择，比如，标记了一个定义成@DebugLog的自定
 * 义注解的所有方法。
 * Aspect（切面）：Pointcut和Advice的组合看做切面。例如，我们在应用中通过定义一个pointcut和给定恰当的advice，
 * 添加一个日志切面。
 * Weaving（织入）：注入代码（advices）到目标位置（joint points）的过程
 *
 * Lancet
 * Lancet是一个轻量级Android AOP框架
 *
 * 编译速度快，并支持增量编译
 * 简介的API，几行Java代码完成注入需求
 * 没有任何多余代码插入apk
 * 支持用于SDK，可以在SDK编写注入代码来修改依赖SDK的App
 */
public class LancetTest {

    /**
     * @TargetClass指定了将要被织入代码的目标类android.util.Log
     * @Proxy指定了将要被织入代码目标方法i
     * 织入方式为Proxy
     * Origin.call()代表了Log.i()这个目标方法
     * 如果被织入的代码是静态方法，这里也需要添加static关键字，否则不会生效
     *
     * 所以这个示例Hook方法的作用就是将代码中所有Log.i(tag,msg)替换为Log.i(tag,msg+"lancet")，将生成的apk
     * 反编译后，查看代码，所有调用Log.i的地方都会变为
     *
     * @Proxy将使用新的方法替换代码里存在的原有的目标方法。
     * 比如代码里有10个地方调用了Dog.bark()，代码这个方法后，所有的10个地方的代码会变味_Lancet.xxx.bark()。而在这个新方法中会执行你在Hook方法中所写的代码。
     * @Proxy通常用与对系统API的劫持。因为虽然我们不能注入代码到系统提供的库之中，但我们可以劫持掉所有调用系统API的地方。
     *
     * @NameRegex
     * @NameRegex 用来限制范围操作的作用域. 仅用于Proxy模式中, 比如你只想代理掉某一个包名下所有的目标操作. 或
     * 者你在代理所有的网络请求时，不想代理掉自己发起的请求. 使用NameRegex对 TargetClass ,
     * ImplementedInterface 筛选出的class再进行一次匹配.
     *
     * @param tag
     * @param msg
     * @return
     */
//    @TargetClass("android.util.Log")
//    @Proxy("i")
//    public static int aaa(String tag, String msg){
//        msg = msg + "(add lancet)";
//        return (int) Origin.call();
//    }

    /**
     * @Insert 将新代码插入到目标方法原有代码前后。
     * @Insert 常用于操作App与library的类，并且可以通过This操作目标类的私有属性与方法(下文将会介绍)。
     * @Insert 当目标方法不存在时，还可以使用mayCreateSuper参数来创建目标方法。
     * 比如下面将代码注入每一个Activity的onResume生命周期
     *
     * 如果一个类 MyActivity extends AppcompatActivity 没有重写 onStop 会自动创建onStop方法，而Origin在
     * 这里就代表了super.onStop(), 最后就是这样的效果：
     * protected void onStop() {
     *     System.out.println("hello world");
     *     super.onStop();
     * }
     *
     * public/protected/private 修饰符会完全照搬 Hook 方法的修饰符。
     */
//    @TargetClass(value = "android.support.v7.app.AppCompatActivity",scope = Scope.LEAF)
//    @Insert(value = "onResume",mayCreateSuper = true)
//    protected void onResume(){
//        Log.d(HookBean.TAG, "onResume: this is LancetTest OnResum");
//        Origin.callVoid();
//    }

    /**
     * 很多情况，我们不会仅匹配一个类，会有注入某各类所有子类，或者实现某个接口的所有类等需求。所以通过
     * TargetClass , ImplementedInterface 2个注解及 Scope 进行目标类匹配。
     *
     * @TargetClass
     * 通过类查找.
     *
     * @TargetClass 的 value 是一个类的全称.
     * Scope.SELF 代表仅匹配 value 指定的目标类.
     * Scope.DIRECT 代表匹配 value 指定类的直接子类.
     * Scope.All 代表匹配 value 指定类的所有子类.
     * Scope.LEAF 代表匹配 value 指定类的最终子类.众所周知java是单继承，所以继承关系是树形结构，所以这里代表了
     * 指定类为顶点的继承树的所有叶子节点.
     * @ImplementedInterface
     * 通过接口查找. 情况比通过类查找稍复杂一些.
     *
     * @ImplementedInterface 的 value 可以填写多个接口的全名.
     * Scope.SELF : 代表直接实现所有指定接口的类.
     * Scope.DIRECT : 代表直接实现所有指定接口，以及指定接口的子接口的类.
     * Scope.ALL: 代表 Scope.DIRECT 指定的所有类及他们的所有子类.
     * Scope.LEAF: 代表 Scope.ALL 指定的森林结构中的所有叶节点.
     *
     * 当我们使用@ImplementedInterface(value = "I", scope = ...)时, 目标类如下:
     *
     * Scope.SELF -> A
     * Scope.DIRECT -> A C
     * Scope.ALL -> A B C D
     * Scope.LEAF -> B D
     */

    /**
     * 匹配目标方法
     * 虽然在 Proxy , Insert 中我们指定了方法名, 但识别方法必须要更细致的信息. 我们会直接使用 Hook 方法的修
     * 饰符，参数类型来匹配方法.
     * 所以一定要保持 Hook 方法的 public/protected/private static 信息与目标方法一致，参数类型，返回类型与
     * 目标方法一致.
     * 返回类型可以用 Object 代替.
     * 方法名不限. 异常声明也不限.
     *
     * 但有时候我们并没有权限声明目标类. 这时候怎么办？
     *
     * @ClassOf
     * 可以使用 ClassOf 注解来替代对类的直接 import.
     *
     * ClassOf 的 value 一定要按照 **(package_name.)(outer_class_name$)inner_class_name([]...)**的模板.
     * 比如:
     *
     * java.lang.Object
     * java.lang.Integer[][]
     * A[]
     * A$B
     */

//    @TargetClass("com.example.myapplication.bean.HookBean")
//    @Insert("show3")
//    public void show3(@ClassOf(com.example.myapplication.bean.HookBean.HookPriClass)Object o){
//        Origin.callVoid();
//    }


    /**
     * API
     * 我们可以通过 Origin 与 This 与目标类进行一些交互.
     *
     * Origin
     * Origin 用来调用原目标方法. 可以被多次调用.
     * Origin.call() 用来调用有返回值的方法.
     * Origin.callVoid() 用来调用没有返回值的方法.
     * 另外，如果你有捕捉异常的需求.可以使用
     * Origin.call/callThrowOne/callThrowTwo/callThrowThree()
     * Origin.callVoid/callVoidThrowOne/callVoidThrowTwo/callVoidThrowThree()
     */

//    @TargetClass("java.io.InputStream")
//    @Proxy("read")
//    public int read(byte[] bytes) throws IOException {
//        try {
//            return (int) Origin.<IOException>callThrowOne();
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw e;
//        }
//    }

    /**
     * This
     * 仅用于Insert 方式的非静态方法的Hook中.(暂时)
     *
     * get()
     * 返回目标方法被调用的实例化对象.
     *
     * putField & getField
     * 你可以直接存取目标类的所有属性，无论是 protected or private.
     * 另外，如果这个属性不存在，我们还会自动创建这个属性. Exciting!
     * 自动装箱拆箱肯定也支持了.
     *
     * 一些已知的缺陷:
     *
     * Proxy 不能使用 This
     * 你不能存取你父类的属性. 当你尝试存取父类属性时，我们还是会创建新的属性.
     */



    /**
     * Tips
     * 内部类应该命名为 package.outer_class$inner_class
     * SDK 开发者不需要 apply 插件, 只需要 provided me.ele:lancet-base:x.y.z
     * 尽管我们支持增量编译. 但当我们使用 Scope.LEAF、Scope.ALL覆盖的类有变动 或者修改 Hook 类时, 本次编译将会变成全量编译.
     */

    /**
     * Android4.4版本以前是Dalvik虚拟机，4.4版本开始引入ART虚拟机（Android Runtime）。在4.4版本上，两种运行
     * 时环境共存，可以相互切换，但是在5.0版本以后，Dalvik虚拟机则被彻底的丢弃，全部采用ART
     */

}