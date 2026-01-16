package com.itwithgeorge;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class CustomClassTransformer implements ClassFileTransformer {
    @Override
    public byte[] transform(ClassLoader loader,
                            String className,
                            Class<?> classBeingRedefined,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer) throws IllegalClassFormatException {

        // manipulate the bytes here
        // Javassist, ASM, Byte Buddy etc. (BCEL, CGLIB,AspectJ)
        byte[] manipulatedBytes = classfileBuffer;

        return manipulatedBytes;
    }
}