package com.naver.design.abstractFactory;

import com.naver.design.commonUsage.*;

public interface ComputerFactory {
    CPU getCPU();
    Disk getDisk();
}