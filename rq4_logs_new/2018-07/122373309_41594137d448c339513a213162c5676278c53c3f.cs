using System;
using System.Collections.Generic;
using System.IO;

using LargeSort.Shared;

namespace IntSort
{
    /// <summary>
    /// Implements functionality for merging chunk files
    /// </summary>
    public class ChunkFileMerger : IChunkFileMerger
    {
        IFileIO fileIO = null;
        IIntegerFileMerger integerFileMerger = null;

        /// <summary>
        /// Initializes an instance of ChunkFileMerger
        /// </summary>
        /// <param name="fileIO">An implementation of file I/O functionality</param>
        /// <param name="integerFileMerger">An implementation of integer file merge functionality</param>
        public ChunkFileMerger(IFileIO fileIO, IIntegerFileMerger integerFileMerger)
        {
            this.fileIO = fileIO;
            this.integerFileMerger = integerFileMerger;
        }

        /// <see cref="IChunkFileMerger.MergeChunkFilesIntoSingleFile(List{string}, int, string, string, string, Action{int, int})"
        public List<string> MergeChunkFilesIntoSingleFile(List<string> chunkFiles, int mergeCount, string intermediateFileTemplate,
            string outputFile, string outputDirectory, Action<int, int> updateProgress)
        {
            int currentGeneration = 1;

            //Call the updateProgress callback every time an integer is merged
            Action<int> updateMergeProgress = (integerCount =>
            {
                updateProgress?.Invoke(currentGeneration, integerCount);
            });

            //Create the output directory if it does not already exist
            fileIO.CreateDirectory(outputDirectory);

            List<string> intermediateFiles = new List<string>();
            List<string> remainingFiles = chunkFiles;

            //Merge the integer files until only one file is left
            do
            {
                //Create the file template for the current generation
                string generationFileTemplate = string.Format(intermediateFileTemplate, currentGeneration, "{0}");

                //Merge the files for the current generation
                remainingFiles = integerFileMerger.MergeIntegerFiles(remainingFiles, mergeCount, generationFileTemplate,
                    outputDirectory, updateMergeProgress);

                intermediateFiles.AddRange(remainingFiles);

                currentGeneration++;
            }
            while (remainingFiles.Count > 1);

            string outputFilePath = Path.Combine(outputDirectory, outputFile);

            //Delete any previous output file with the same name
            fileIO.DeleteFile(outputFilePath);

            //Rename the remaining file to the output file
            fileIO.RenameFile(remainingFiles[0], outputFilePath);

            return intermediateFiles;
        }
    }
}