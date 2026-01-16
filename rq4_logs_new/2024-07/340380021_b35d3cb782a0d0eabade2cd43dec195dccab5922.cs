using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RayTracer.MathLib.Benchmarks.Benchmarks;

[MemoryDiagnoser]
public class Matrix4x4Benchmarks
{
    [Benchmark]
    public void DetFloat()
    {
        ExperimentalClasses._32Bit.Matrix4x4 float4x4 = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        _ = float4x4.Det;
    }

    [Benchmark]
    public void DetDouble()
    {
        RayTracer.MathLib.Matrix4x4 double4x4 = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        _ = double4x4.Det;
    }

    [Benchmark]
    public void TransposeFloat()
    {
        ExperimentalClasses._32Bit.Matrix4x4 float4x4 = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        _ = ExperimentalClasses._32Bit.Matrix4x4.Transpose(float4x4);
    }

    [Benchmark]
    public void TransposeDouble()
    {
        RayTracer.MathLib.Matrix4x4 double4x4 = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        _ = RayTracer.MathLib.Matrix4x4.Transpose(double4x4);
    }

    [Benchmark]
    public void InverseFloat()
    {
        ExperimentalClasses._32Bit.Matrix4x4 float4x4 = new(-2, -8, 3, 5,
                     -3, 1, 7, 3,
                     1, 2, -9, 6,
                     -6, 7, 7, -9);
        ExperimentalClasses._32Bit.Matrix4x4 float4x4Inv = ExperimentalClasses._32Bit.Matrix4x4.Inverse(float4x4);
    }

    [Benchmark]
    public void InverseDouble()
    {
        RayTracer.MathLib.Matrix4x4 double4x4 = new(-2, -8, 3, 5,
                     -3, 1, 7, 3,
                     1, 2, -9, 6,
                     -6, 7, 7, -9);
        RayTracer.MathLib.Matrix4x4 double4x4Inv = RayTracer.MathLib.Matrix4x4.Inverse(double4x4);
    }
}