package com.example.myapplication.javatest;

/**
 * classloder解析
 */
public class ClassLoderTest {
    /**
     * 类加载器使用的都是双亲委派模型
     * 双亲委派模型：如果一个类加载器收到了类加载的请求，它首先不会自己去尝试加载这个类，而是把这个请求委派给父类
     * 加载器去完成，每一个层次的类加载器都是如此，因此所有的加载请求最终都应该传送到顶层的启动类加载器中，只有当
     * 父加载器反馈自己无法完成这个加载请求（它的搜索范围中没有找到所需的类）时，子加载器才会尝试自己去加载。
     */

    /**
     * 1.ClassLoader的类型
     * 我们知道Java中的ClassLoader可以加载jar文件和Class文件（本质是加载Class文件），这一点在Android中并不适
     * 用，因为无论是DVM还是ART它们加载的不再是Class文件，而是dex文件，这就需要重新设计ClassLoader相关类，我们
     * 先来学习ClassLoader的类型。
     * Android中的ClassLoader类型和Java中的ClassLoader类型类似，也分为两种类型，分别是系统ClassLoader和自定
     * 义ClassLoader。其中系统ClassLoader主要有3种分别是BootClassLoader、PathClassLoader和DexClassLoader。
     */

    /**
     * BootClassLoader :所有android加载器中最顶层加载器.Android系统启动时会使用BootClassLoader来预加载常用类，
     * 与Java中的BootClassLoader不同，它并不是由C/C++代码实现，而是由Java实现的，BootClassLoade的代码如下所示。
     *
     * class BootClassLoader extends ClassLoader {
     *     private static BootClassLoader instance;
     *     @FindBugsSuppressWarnings("DP_CREATE_CLASSLOADER_INSIDE_DO_PRIVILEGED")
     *     public static synchronized BootClassLoader getInstance() {
     *         if (instance == null) {
     *             instance = new BootClassLoader();
     *         }
     *         return instance;
     *     }
     * ...
     * BootClassLoader是ClassLoader的内部类，并继承自ClassLoader。BootClassLoader是一个单例类，需要注意的
     * 是BootClassLoader的访问修饰符是默认的，只有在同一个包中才可以访问，因此我们在应用程序中是无法直接调用的。
     *
     * BootClassLoader的创建:从Zygote进程开始说起,main方法是ZygoteInit入口方法，其中调用了ZygoteInit的preload方法，
     * preload方法中又调用了ZygoteInit的preloadClasses方法，preloadClasses方法用于Zygote进程初始化时预加载常用类。
     * 如：android.app.ApplicationLoaders
     * android.app.ApplicationPackageManager
     * android.app.ApplicationPackageManager$OnPermissionsChangeListenerDelegate
     * android.app.ApplicationPackageManager$ResourceName
     * android.app.ContentProviderHolder
     * android.app.ContentProviderHolder$1
     * android.app.ContextImpl
     * android.app.ContextImpl$ApplicationContentResolver
     * android.app.DexLoadReporter
     * android.app.Dialog
     * android.app.Dialog$ListenersHandler
     * android.app.DownloadManager
     * android.app.Fragment
     * 可以看到preloaded-classes文件中的预加载类的名称有很多都是我们非常熟知的。预加载属于拿空间换时间的策略，
     * Zygote环境配置的越健全越通用，应用程序进程需要单独做的事情也就越少，预加载除了预加载类，还有预加载资源和预加载共享库
     * preloadClasses方法如下：
     * private static void preloadClasses() {
     *         final VMRuntime runtime = VMRuntime.getRuntime();
     *         InputStream is;
     *         try {
     *             is = new FileInputStream(PRELOADED_CLASSES);//1
     *         } catch (FileNotFoundException e) {
     *             Log.e(TAG, "Couldn't find " + PRELOADED_CLASSES + ".");
     *             return;
     *         }
     *         ...
     *         try {
     *             BufferedReader br
     *                 = new BufferedReader(new InputStreamReader(is), 256);//2
     *
     *             int count = 0;
     *             String line;
     *             while ((line = br.readLine()) != null) {//3
     *                 line = line.trim();
     *                 if (line.startsWith("#") || line.equals("")) {
     *                     continue;
     *                 }
     *                   Trace.traceBegin(Trace.TRACE_TAG_DALVIK, line);
     *                 try {
     *                     if (false) {
     *                         Log.v(TAG, "Preloading " + line + "...");
     *                     }
     *                     Class.forName(line, true, null);//4
     *                     count++;
     *                 } catch (ClassNotFoundException e) {
     *                     Log.w(TAG, "Class not found for preloading: " + line);
     *                 }
     *         ...
     *         } catch (IOException e) {
     *             Log.e(TAG, "Error reading " + PRELOADED_CLASSES + ".", e);
     *         } finally {
     *             ...
     *         }
     *     }
     *可以看到preloaded-classes文件中的预加载类的名称有很多都是我们非常熟知的。预加载属于拿空间换时间的策略，
     * Zygote环境配置的越健全越通用，应用程序进程需要单独做的事情也就越少，预加载除了预加载类，还有预加载资源和
     * 预加载共享库，因为不是本文重点，这里就不在延伸讲下去了。
     * 回到preloadClasses方法的注释2处，将FileInputStream封装为BufferedReader，并注释3处遍历BufferedReader，
     * 读出所有预加载类的名称，每读出一个预加载类的名称就调用注释4处的代码加载该类，Class的forName方法如下所示。
     *  public static Class<?> forName(String name, boolean initialize,
     *                                    ClassLoader loader)
     *         throws ClassNotFoundException
     *     {
     *         if (loader == null) {
     *             loader = BootClassLoader.getInstance();//1
     *         }
     *         Class<?> result;
     *         try {
     *             result = classForName(name, initialize, loader);//2
     *         } catch (ClassNotFoundException e) {
     *             Throwable cause = e.getCause();
     *             if (cause instanceof LinkageError) {
     *                 throw (LinkageError) cause;
     *             }
     *             throw e;
     *         }
     *         return result;
     *     }
     *   注释1处创建了BootClassLoader，并将BootClassLoader实例传入到了注释2处的classForName方法中，
     *   classForName方法是Native方法，它的实现由c/c++代码来完成，如下所示。
     *   @FastNative
     *     static native Class<?> classForName(String className, boolean shouldInitialize,
     *             ClassLoader classLoader) throws ClassNotFoundException;
     *
     * BaseDexClassLoader:BaseDexClassLoader是PathClassLoader和DexClassLoader的父类，BaseDexClassLoader
     * 中维护了一个DexPathList，PathClassLoader和DexClassLoader查找类的操作直接调用BaseClassLoader的
     * findClass方法，而BaseClassLoader的findClass中又通过内部维护的DexPathList来查找，DexPathList中又维
     * 护这一个Element数组，这个数组中Element元素其实就是Dex文件.
     * DexClassLoader：能够加载未安装的apk.
     * PathClassLoader：apk的默认加载器，它是用来加载系统类和主dex文件中的类的，但是系统类是由BootClassLoader
     * 加载的，如果apk中有多个dex文件，只会加载主dex.
     *
     * PathClassLoader构造方法中像上传递时第二个参数传了null，这个参数代表的是dex的路径，
     * DexPathList在生成Element数组时会判断这个参数是否为null，如果为null就使用系统默认路径/data/dalvik-cache，
     * 这也是导致如果要加载外置dex文件只能使用DexClassLoader的原因
     *
     * PathClassLoader的创建：
     * PathClassLoader的创建也得从Zygote进程开始说起，Zygote进程启动SyetemServer进程时会调用ZygoteInit的
     * startSystemServer方法，如下所示。
     *
     * private static boolean startSystemServer(String abiList, String socketName)
     *            throws MethodAndArgsCaller, RuntimeException {
     *     ...
     *         int pid;
     *         try {
     *             parsedArgs = new ZygoteConnection.Arguments(args);//2
     *             ZygoteConnection.applyDebuggerSystemProperty(parsedArgs);
     *             ZygoteConnection.applyInvokeWithSystemProperty(parsedArgs);
     *             //1
     *             pid = Zygote.forkSystemServer(
     *                     parsedArgs.uid, parsedArgs.gid,
     *                     parsedArgs.gids,
     *                     parsedArgs.debugFlags,
     *                     null,
     *                     parsedArgs.permittedCapabilities,
     *                     parsedArgs.effectiveCapabilities);
     *         } catch (IllegalArgumentException ex) {
     *             throw new RuntimeException(ex);
     *         }
     *        if (pid == 0) {//2
     *            if (hasSecondZygote(abiList)) {
     *                waitForSecondaryZygote(socketName);
     *            }
     *            handleSystemServerProcess(parsedArgs);//3
     *        }
     *        return true;
     *    }
     *
     *  注释1处，Zygote进程通过forkSystemServer方法fork自身创建子进程（SystemServer进程）。注释2处如果
     *  forkSystemServer方法返回的pid等于0，说明当前代码是在新创建的SystemServer进程中执行的，接着就会执行
     *  注释3处的handleSystemServerProcess方法：
     *  private static void handleSystemServerProcess(
     *             ZygoteConnection.Arguments parsedArgs)
     *             throws Zygote.MethodAndArgsCaller {
     *
     *     ...
     *         if (parsedArgs.invokeWith != null) {
     *            ...
     *         } else {
     *             ClassLoader cl = null;
     *             if (systemServerClasspath != null) {
     *                 cl = createPathClassLoader(systemServerClasspath, parsedArgs.targetSdkVersion);//1
     *                 Thread.currentThread().setContextClassLoader(cl);
     *             }
     *             ZygoteInit.zygoteInit(parsedArgs.targetSdkVersion, parsedArgs.remainingArgs, cl);
     *         }
     *     }
     *   注释1处调用了createPathClassLoader方法，如下所示。
     *   static PathClassLoader createPathClassLoader(String classPath, int targetSdkVersion) {
     *       String libraryPath = System.getProperty("java.library.path");
     *       return PathClassLoaderFactory.createClassLoader(classPath,
     *                                                       libraryPath,
     *                                                       libraryPath,
     *                                                       ClassLoader.getSystemClassLoader(),
     *                                                       targetSdkVersion,
     *                                                       true /* isNamespaceShared /);}
     * createPathClassLoader方法中又会调用PathClassLoaderFactory的createClassLoader方法，看来
     * PathClassLoader是用工厂来进行创建的。
     * public static PathClassLoader createClassLoader(String dexPath,
     *                                                     String librarySearchPath,
     *                                                     String libraryPermittedPath,
     *                                                     ClassLoader parent,
     *                                                     int targetSdkVersion,
     *                                                     boolean isNamespaceShared) {
     *         PathClassLoader pathClassloader = new PathClassLoader(dexPath, librarySearchPath, parent);
     *       ...
     *         return pathClassloader;
     *     }
     *  在PathClassLoaderFactory的createClassLoader方法中会创建PathClassLoader。
     *
     * 很多博客里说PathClassLoader只能加载已安装的apk的dex，其实这说的应该是在dalvik虚拟机上，在art虚拟机上
     * PathClassLoader可以加载未安装的apk的dex（在art平台上已验证），然而在/data/dalvik-cache 确未找到相应
     * 的dex文件，怀疑是art虚拟机判断apk未安装，所以只是将apk优化后的odex放在内存中，之后进行释放，这只是个猜
     * 想，希望有知道的可以告知一下。因为dalvik上无法使用，所以我们也没法使用。
     *
     */

    /**
     * 可以看出，BaseDexClassLoader中有个pathList对象，pathList中包含一个DexFile的数组dexElements，由上面
     * 分析知道，dexPath传入的原始dex(.apk,.zip,.jar等)文件在optimizedDirectory文件夹中生成相应的优化后的
     * odex文件，dexElements数组就是这些odex文件的集合，如果不分包一般这个数组只有一个Element元素，也就只有一
     * 个DexFile文件，而对于类加载呢，就是遍历这个集合，通过DexFile去寻找。最终调用native方法的defineClass。
     */

    /**
     * ART虚拟机的兼容性问题
     * Android Runtime（缩写为ART），在Android 5.0及后续Android版本中作为正式的运行时库取代了以往的Dalvik
     * 虚拟机。ART能够把应用程序的字节码转换为机器码，是Android所使用的一种新的虚拟机。它与Dalvik的主要不同在
     * 于：Dalvik采用的是JIT技术，字节码都需要通过即时编译器（just in time ，JIT）转换为机器码，这会拖慢应用
     * 的运行效率，而ART采用Ahead-of-time（AOT）技术，应用在第一次安装的时候，字节码就会预先编译成机器码，这
     * 个过程叫做预编译。ART同时也改善了性能、垃圾回收（Garbage Collection）、应用程序除错以及性能分析。但是
     * 请注意，运行时内存占用空间较少同样意味着编译二进制需要更高的存储。
     * ART模式相比原来的Dalvik，会在安装APK的时候，使用Android系统自带的dex2oat工具把APK里面的.dex文件转化
     * 成OAT文件，OAT文件是一种Android私有ELF文件格式，它不仅包含有从DEX文件翻译而来的本地机器指令，还包含有
     * 原来的DEX文件内容。这使得我们无需重新编译原有的APK就可以让它正常地在ART里面运行，也就是我们不需要改变
     * 原来的APK编程接口。ART模式的系统里，同样存在DexClassLoader类，包名路径也没变，只不过它的具体实现与原
     * 来的有所不同，但是接口是一致的。实际上，ART运行时就是和Dalvik虚拟机一样，实现了一套完全兼容Java虚拟机的接口。
     *
     */
}