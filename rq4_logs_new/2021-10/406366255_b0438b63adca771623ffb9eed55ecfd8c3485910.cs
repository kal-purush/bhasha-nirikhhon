// <copyright file="EngineComponent.cs" company="PlaceholderCompany">
// Copyright (c) PlaceholderCompany. All rights reserved.
// </copyright>

namespace AutomatedCar.SystemComponents.PowertrainComponents
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class EngineComponent
    {
        private const int MaxRPM = 7000; // Maximálisan megengedett fordulatszám
        private const int BaseRPM = 500; // Naggyáboli alapjárati érték

        private double gasPedalValue; // Gázpedál állásának tárolása
        private double breakPedalValue; // Fékpedál állásának tárolása

        protected VirtualFunctionBus virtualFunctionBus;

        public int engineRPM { get; private set; }

        public EngineComponent(VirtualFunctionBus virtualFunctionBus)
        {
            this.engineRPM = BaseRPM;
            this.virtualFunctionBus = virtualFunctionBus;
        }
    }
}