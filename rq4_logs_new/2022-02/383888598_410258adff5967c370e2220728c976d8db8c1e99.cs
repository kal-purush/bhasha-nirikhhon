using Aspentech.HYSYS;
using Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace Simulators.Tests
{
    public class ComponentProperties
    {
        public static void TestDefinition(string filePath, string fileName, ISimulator hysysSimulator)
        {
            hysysSimulator.OpenCase(new CaseInfo(filePath, fileName));

            string csvFilePath = Path.Combine(filePath, $"Properties_results.csv");
            PersistenceManager persistanceManager = new PersistenceManager(csvFilePath);

            SimulationCase simCase = (SimulationCase)hysysSimulator.GetActiveSimulationCase();
            IHysysVariableManager varManager = new HysysVariableManager(hysysSimulator);
            Flowsheet flowsheet = varManager.get_flowsheet(simCase.Flowsheet, "FracU");
            ProcessStream fuelStream = varManager.get_process_stream(flowsheet, "LiqProd");
            ProcessStream energyStream = varManager.get_energy_stream(flowsheet, "Q-liquid-prod");

            string[] components = fuelStream.FluidPackage.ComponentList.Components.Names;

            for (int i = 0; i < components.Length; i++)
            {
                simCase.Solver.CanSolve = false;
                fuelStream.ComponentMolarFraction.Erase();

                dynamic newMolarFractionVAlues = fuelStream.ComponentMolarFractionValue;
                for (int j = 0; j < components.Length; j++)
                {
                    if (j == i)
                    {
                        newMolarFractionVAlues[j] = 1;
                    }
                    else
                    {
                        newMolarFractionVAlues[j] = 0;
                    }

                }
                fuelStream.ComponentMolarFraction.Calculate(newMolarFractionVAlues);
                simCase.Solver.CanSolve = true;

                string LiqProd_hhv_gas = "Document.0/FlowSht.1/UnitOpObject.400(Frac Unit)/FlowSht.600/StreamObject.400(LiqProd)/HysysCorrelation.300.55:ExtraData.550.0";
                string LiqProd_hhv_std = "Document.0/FlowSht.1/UnitOpObject.400(Frac Unit)/FlowSht.600/StreamObject.400(LiqProd)/HysysCorrelation.300.9:ExtraData.550.0";
                varManager.add_variable_through_moniker(fuelStream, LiqProd_hhv_gas, "HHV[Gas]");
                varManager.add_variable_through_moniker(fuelStream, LiqProd_hhv_std, "HHV[std]");
                varManager.add_variable_through_process_stream(fuelStream, "MolarFlow");
                varManager.add_variable_through_process_stream(fuelStream, "MassFlow");
                varManager.add_variable_through_process_stream(fuelStream, "MolarDensity");
                varManager.add_variable_through_process_stream(energyStream, "HeatFlow");

                string data = string.Empty;


                var header_names = new List<string>();
                var header_units = new List<string>();
                var values = new List<string>();

                if(i==0)
                {
                    header_names.Add("Component");
                    header_units.Add(" ");
                }

                values.Add(components[i]);

                foreach (var variable in varManager.get_list())
                {
                    if (i == 0)
                    {
                        header_names.Add(variable.Name);
                        header_units.Add(variable.Uom);
                    }
                    values.Add(variable.value.ToString());
                }

                if(i==0)
                {
                    data = string.Join(",", header_names);
                    persistanceManager.WriteToFile(data, false);
                    data = string.Join(",", header_units);
                    persistanceManager.WriteToFile(data, true);
                }
                
                data = string.Join(",", values);
                persistanceManager.WriteToFile(data, true);

                varManager.clear_list();
            }

            //Fluid fuelFluid = fuelStream.DuplicateFluid();
            //PropertyVectors propvector = fuelFluid.PropertyVectors;

        }
    }

    public interface IHysysVariableManager
    {
        Flowsheet get_flowsheet(Flowsheet flowsheet, string flowsheetName);
        ProcessStream get_process_stream(Flowsheet flowsheet, string streamName);
        ProcessStream get_energy_stream(Flowsheet flowsheet, string streamName);
        void add_variable_through_process_stream(ProcessStream processStream, string physicalType);
        void add_variable_through_moniker(ProcessStream processStream, string moniker, string physicalType);
        void clear_list();
        ICollection<HysysVariable> get_list();
    }
    public class HysysVariableManager : IHysysVariableManager
    {
        private ISimulator hysysSimulator;
        private ICollection<HysysVariable> storedVariables = new List<HysysVariable>();
        public HysysVariableManager(ISimulator simulator) => hysysSimulator = simulator;
        public void clear_list()
        {
            storedVariables.Clear();
        }
        public ICollection<HysysVariable> get_list()
        {
            return storedVariables;
        }
        public Flowsheet get_flowsheet(Flowsheet flowsheet, string flowsheetName)
        {
            if (flowsheet.name == flowsheetName)
            {
                return flowsheet;
            }
            else
            {
                Flowsheet foundFlowsheet = null;
                foreach (Flowsheet subFlowsheet in flowsheet?.Flowsheets)
                {
                    if (subFlowsheet.name == flowsheetName)
                    {
                        foundFlowsheet = subFlowsheet;
                        break;
                    }
                }
                return foundFlowsheet;
            }

        }
        public ProcessStream get_process_stream(Flowsheet flowsheet, string streamName)
        {
            return flowsheet?.MaterialStreams[streamName];
        }
        public ProcessStream get_energy_stream(Flowsheet flowsheet, string streamName)
        {
            return flowsheet?.EnergyStreams[streamName];
        }

        public void add_variable_through_moniker(ProcessStream processStream, string moniker, string physicalType)
        {
            dynamic hysysVariable = hysysSimulator.GetCaseVariable(moniker);
            UnitConversionType_enum conversionTypeEnum = (UnitConversionType_enum)(hysysVariable.UnitConversionType);
            SimulationCase simCase = (SimulationCase)hysysSimulator.GetActiveSimulationCase();
            string conversionType = ((Application)simCase.Application)
                .UnitConversionSetManager
                .GetUnitConversionSet(conversionTypeEnum)
                .CalculationUnit
                .name;

            storedVariables.Add(new HysysVariable()
            {
                Name = $"{processStream.name}.{physicalType}",
                Uom = conversionType,
                value = hysysVariable.Value
            });
        }
        public void add_variable_through_process_stream(ProcessStream processStream, string physicalType)
        {
            Type type = processStream.GetType();
            RealVariable hysysVariable = type.InvokeMember(physicalType, BindingFlags.GetProperty, null, processStream, null) as RealVariable;
            SimulationCase simCase = (SimulationCase)hysysSimulator.GetActiveSimulationCase();
            string conversionType = ((Application)simCase.Application)
                .UnitConversionSetManager
                .GetUnitConversionSet(hysysVariable.UnitConversionType)
                .CalculationUnit
                .name;

            storedVariables.Add(new HysysVariable()
            {
                Name = $"{processStream.name}.{physicalType}",
                Uom = conversionType,
                value = hysysVariable.Value
            });
        }
    }
    public class HysysVariable
    {
        public string Name { get; set; }
        public double value { get; set; }
        public string Uom { get; set; }
    }
}