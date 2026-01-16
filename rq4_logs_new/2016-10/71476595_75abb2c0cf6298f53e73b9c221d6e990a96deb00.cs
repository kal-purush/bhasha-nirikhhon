using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO.Abstractions;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TplPlayground.CommandFileProcessor.Model;

namespace TplPlayground.CommandFileProcessor
{
    [Export(typeof(Dataflow))]
    public class Dataflow : IDataflowBlock
    {
        private readonly ExportFactory<IFileSystem> _fileSystemFactory;

        [ImportingConstructor]
        public Dataflow(ExportFactory<IFileSystem> fileSystemFactory)
        {
            this._fileSystemFactory = fileSystemFactory;

            var parallelBlockOptions = new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 10,
                MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
            };

            var linkOptions = new DataflowLinkOptions
            {
                PropagateCompletion = true
            };

            // create the dataflow blocks
            var readFileBlock = new TransformBlock<string, Tuple<string, string[]>>(filePath => ReadFileContents(filePath), parallelBlockOptions);
            var breakIntoSectionsBlock = new TransformManyBlock<Tuple<string, string[]>, FileSection>(fileInfo => BreakIntoSections(fileInfo), parallelBlockOptions);
            var printBlock = new ActionBlock<FileSection>(
                section => PrintResults(section),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1
                });

            InputBlock = readFileBlock;
            Completion = printBlock.Completion;

            // link dataflow blocks together
            readFileBlock.LinkTo(breakIntoSectionsBlock, linkOptions);
            breakIntoSectionsBlock.LinkTo(printBlock, linkOptions);

            // debug task completion
            DebugCompletion(readFileBlock, "Read File Block");
            DebugCompletion(breakIntoSectionsBlock, "Break Into Sections Block");
            DebugCompletion(printBlock, "Print Block");
        }

        public Task Completion
        {
            get;
        }

        public ITargetBlock<string> InputBlock
        {
            get;
        }

        public void Complete() =>
            InputBlock.Complete();

        public void Fault(Exception exception)
        {
            // TODO:...
        }

        private static void DebugCompletion(IDataflowBlock block, string name) =>
            block.Completion.ContinueWith(_ => Debug.WriteLine($"Completed Task: {name}", ConsoleColor.Cyan));

        private static void PrintResults(FileSection section) =>
            Debug.WriteLine(section.ToString());

        private IEnumerable<FileSection> BreakIntoSections(Tuple<string, string[]> fileInfo)
        {
            using (var fileSystem = _fileSystemFactory.CreateExport())
            {
                string message = $"Splitting paragraphs: {fileSystem.Value.Path.GetFileName(fileInfo.Item1)}";
                const string commandStart = "---- CMD:";
                Debug.WriteLine(message);

                var headerSection = new HeaderSection(
                    fileInfo.Item1,
                    0,
                    fileInfo.Item2.TakeWhile(line => !line.StartsWith(commandStart)));

                var paragraphs = fileInfo.Item2
                    .SkipWhile(line => !line.StartsWith(commandStart))
                    .Aggregate(
                        new List<List<string>>(),
                        (list, value) =>
                        {
                            if (value.StartsWith(commandStart))
                            {
                                list.Add(new List<string>());
                            }

                            list.Last().Add(value);
                            return list;
                        });

                var result = paragraphs.Select((paragraph, index) =>
                    new CommandSection(fileInfo.Item1, index + 1, paragraph.First().Substring(commandStart.Length).Trim(), paragraph)).ToList();
                Debug.WriteLine($"Completed {char.ToLowerInvariant(message[0]) + message.Substring(1)}");
                return new FileSection[] { headerSection }.Concat(result);
            }
        }

        private Tuple<string, string[]> ReadFileContents(string filePath)
        {
            using (var fileSystem = _fileSystemFactory.CreateExport())
            {
                var lines = fileSystem.Value.File.ReadAllLines(filePath);
                Debug.WriteLine(filePath + Environment.NewLine + string.Join(Environment.NewLine, lines));
                return Tuple.Create(filePath, lines);
            }
        }
    }
}