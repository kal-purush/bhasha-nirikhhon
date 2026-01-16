package com.jzw.compiler;

import com.google.auto.service.AutoService;
import com.jzw.annotation.MyRouterTest;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.sun.jdi.ClassType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

/**
 * 实现注解处理器
 *
 * 大概原理：
 * Java API 已经提供了扫描源码并解析注解的框架，开发者可以通过继承 AbstractProcessor 类来实现自己的注解解析逻
 * 辑。APT 的原理就是在注解了某些代码元素（如字段、函数、类等）后，在编译时编译器会检查 AbstractProcessor 的子
 * 类，并且自动调用其 process() 方法，然后将添加了指定注解的所有代码元素作为参数传递给该方法，开发者再根据注解元
 * 素在编译期输出对应的 Java 代码
 * APT在代码编译期解析注解，并且生成新的 Java 文件，减少手动的代码输入。
 *
 * APT是一个命令行工具，它对源代码文件进行检测找出其中的annotation后，使用AbstractProcessor来处理annotation。
 *
 * JavaPoet + Auto Service + java APT完成对编译时注解的处理。
 */
@AutoService(Processor.class)
public class MyRouteProcessor extends AbstractProcessor {

    private Filer filer;

    // 这个方法返回当前处理器 能处理哪些注解，这里我们只返回 MyRouterTest
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        /**
         * 这里你必须指定，这个注解处理器是注册给哪个注解的。注意，它的返回值是一个字符串的集合，包含本处理器想
         * 要处理的注解类型的合法全称。换句话说，你在这里定义你的注解处理器注册到哪些注解上。
         */
        return Collections.singleton(MyRouterTest.class.getCanonicalName());
    }

    // 这个方法返回当前处理器 支持的代码版本
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return super.getSupportedSourceVersion();
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        filer = processingEnv.getFiler();
    }

    /**
     * 此方法注解处理器核心，在此方法中处理注解，可随意添加你想做的事
     * 比如我们要生成一个新的类，类中有一个静态方法，方法返回添加了 @Annotation 注解的所有类。这些操作都需要我
     * 们在 process() 方法中去实现。步骤： (1) 获取所有添加了注解的元素； (2) 生成一个方法，方法的代码块是返回
     * (1)中获取到的列表。 (3) 生成一个类，类中加入(2)中生成的方法； (4) 将(3)中生成的类写入文件。
     * @param set
     * @param roundEnvironment
     * @return
     */
    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
       //获取所有被 @MyRouterTest 注解的类
        Set<? extends Element> elementsAnnotatedWith = roundEnvironment.getElementsAnnotatedWith(MyRouterTest.class);
        // 创建一个方法，返回 Set<Class>
        MethodSpec methodSpec = createMethodWithElements(elementsAnnotatedWith);
        // 创建一个类
        TypeSpec typeSpec = createClassWithMethod(methodSpec);

        // // 将这个类写入文件
        writeClassToFile(typeSpec);
        return false;
    }

    /**
     * 将一个创建好的类写入到文件中参与编译
     */
    private void writeClassToFile(TypeSpec typeSpec) {
        // 声明一个文件在 "me.moolv.apt" 下
        JavaFile build = JavaFile.builder("me.moolv.apt", typeSpec).build();

        // 写入文件
        try {
            build.writeTo(filer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 创建一个类，并把参数中的方法加入到这个类中
     */
    private TypeSpec createClassWithMethod(MethodSpec methodSpec) {
        // 定义一个名字叫 OurClass 的类
        TypeSpec.Builder builder = TypeSpec.classBuilder("OurClass");
        // 声明为 public
        builder.addModifiers(Modifier.PUBLIC);
        // 为这个类加入一段注释
        builder.addJavadoc("这个类是添加注解，注解处理器自动创建的手写类");
        // 为这个类新增一个方法
        builder.addMethod(methodSpec);

        return builder.build();
    }

    /**
     * 创建一个方法，这个方法返回 elements 中的所有类信息。
     */
    private MethodSpec createMethodWithElements(Set<? extends Element> elementsAnnotatedWith) {
        //getAllClass 时方法名 即 public static Set<Class> getAllClass（）{}
        MethodSpec.Builder builder = MethodSpec.methodBuilder("getAllClass");
        //为方法加上 public static
        builder.addModifiers(Modifier.PUBLIC,Modifier.STATIC);
        //定义返回值类型为 Set<Class>
        ParameterizedTypeName parameterizedTypeName = ParameterizedTypeName.get(ClassName.get(Set.class), ClassName.get(Class.class));
        builder.returns(parameterizedTypeName);
        // 经过上面的步骤，
        // 我们得到了 public static Set<Class> getAllClasses() {} 这个方法,
        // 接下来我们实现它的方法体：

        // 方法中的第一行: Set<Class> set = new HashSet<>();
        builder.addStatement("$T<$T> set = new $T<>();",Set.class,Class.class,HashSet.class);
        // 上面的 "$T" 是占位符，代表一个类型，可以自动 import 包。其它占位符：
        // $L: 字符(Literals)、 $S: 字符串(String)、 $N: 命名(Names)
        // 遍历 elements, 添加代码行
        for (Element element : elementsAnnotatedWith){
            // 因为 @MyRouterTest 只能添加在类上，所以这里直接强转为 ClassType
            ClassType typeMirror = (ClassType)element.asType();
            // 在我们创建的方法中，新增一行代码： set.add(XXX.class);
            builder.addStatement("set.add($T.class)",typeMirror);

        }
        // 经过上面的 for 循环，我们就把所有添加了注解的类加入到 set 变量中了，
        // 最后，只需要把这个 set 作为返回值 return 就好了：
        builder.addStatement("return set");

        return builder.build();
    }
}