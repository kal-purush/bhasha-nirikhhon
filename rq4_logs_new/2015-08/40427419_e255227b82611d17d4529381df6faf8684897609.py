#!/usr/bin/python

from __future__ import print_function
import cpuid

if __name__ == "__main__":
    print("Vendor:", cpuid.vendor())
    print("Stepping ID:", cpuid.stepping_id())
    print("Model:", hex(cpuid.model()))
    print("Family:", cpuid.family())
    print("Processor Type:", cpuid.processor_type())
    print("Brand ID:", hex(cpuid.brand_id()))
    print("Brand String:", cpuid.brand_string())
    print("Features:", cpuid.features())