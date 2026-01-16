using Microsoft.Build.Framework;
using SymbioticTS.Core;
using SymbioticTS.Core.IOAbstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using MSBuildTask = Microsoft.Build.Utilities.Task;

namespace SymbioticTS.Build
{
    public class SymbioticTSTransformTask : MSBuildTask
    {
        private string _inputAssemblyPath;

        private string _outputPath;

        /// <summary>
        /// Gets or sets the input assembly path.
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        [Required]
        public string InputAssemblyPath
        {
            get => this._inputAssemblyPath;
            set
            {
                if (string.IsNullOrEmpty(value) || !File.Exists(value))
                {
                    throw new ArgumentException($"The specified input assembly path does not exist: {value}.");
                }

                this._inputAssemblyPath = value;
            }
        }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        [Required]
        public string OutputPath
        {
            get => this._outputPath;
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentException($"The specified output path does not exist: {value}.");
                }

                this._outputPath = value;
            }
        }

        public override bool Execute()
        {
            this.Log.LogMessage(MessageImportance.Normal, $"{nameof(SymbioticTS)}: Transforming types from {this.InputAssemblyPath} to {this.OutputPath}.");

            IFileSink fileSink = new DirectoryFileSink(this.OutputPath);
            TsSymbolWriter symbolWriter = new TsSymbolWriter(fileSink);
            TsTypeManager typeManager = new TsTypeManager();

            Assembly assembly = Assembly.LoadFrom(this.InputAssemblyPath);

            IReadOnlyList<Assembly> assemblies = typeManager.DiscoverAssemblies(assembly);
            IReadOnlyList<Type> types = typeManager.DiscoverTypes(assemblies);
            IReadOnlyList<TsTypeSymbol> typeSymbols = typeManager.ResolveTypeSymbols(types);

            this.Log.LogMessage(MessageImportance.Normal, $"{nameof(SymbioticTS)}: Found and transforming {typeSymbols.Count} types.");

            symbolWriter.WriteSymbols(typeSymbols);

            return true;
        }
    }
}