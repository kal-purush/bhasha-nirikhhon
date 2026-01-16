using Linty.Analyzers;
using Linty.Analyzers.Vector;
using Microsoft.CodeAnalysis.Diagnostics;
using NUnit.Framework;
using RoslynNUnitLight;

namespace UnityEngineAnalyzer.Test.Vector
{
    [TestFixture]
    sealed class VectorAnalyzerTests : AnalyzerTestFixture
    {
        protected override DiagnosticAnalyzer CreateAnalyzer() => new VectorAnalyzer();

        [Test]
        public void Vector2magnitudeShouldSetDiagnostics()
        {
            const string code = @"
using UnityEngine;

class A : MonoBehaviour
{
    Vector2 pos2D;

    void Update()
    {
        var magnitude = [|pos2D.magnitude|];
    }
}";
            HasDiagnostic(code, DiagnosticIDs.VectorMagnitudeIsSlow);
        }

        [Test]
        public void Vector3magnitudeShouldSetDiagnostics()
        {
            const string code = @"
using UnityEngine;

class B : MonoBehaviour
{
    void Update()
    {
        var magnitude = [|transform.position.magnitude|];
    }
}";
            HasDiagnostic(code, DiagnosticIDs.VectorMagnitudeIsSlow);
        }

        [Test]
        public void Vector3magnitudeShouldSetDiagnosticsInsideIfStatement()
        {
            const string code = @"
using UnityEngine;

class C : MonoBehaviour
{
    private float distance = 5.0f;

    void Update()
    {
        if ([|transform.position.magnitude|] > distance) 
        { 
             //OPTIMIZE THIS CALL BY USING sqrMagnitude
        }
    }
}";
            HasDiagnostic(code, DiagnosticIDs.VectorMagnitudeIsSlow);
        }
    }
}