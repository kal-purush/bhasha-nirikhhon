package com.kodeir.jcore.natives;

public class HelloNative {

    /*
    C:\Users\SRyabinin\IdeaProjects\JCoreChecks\target\classes\com\kodeir\jcore\natives>
        "C:\Program Files\Java\jdk1.8.0_131\bin\javah.exe"
         -jni
         -classpath "C:\Users\SRyabinin\IdeaProjects\JCoreChecks\target\classes"
         com.kodeir.jcore.natives.HelloNative
     */

    public native void displayHelloNative();

    static {
        System.loadLibrary("hello");
    }

}