using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CollectionOfHelpers.FileExtensions
{
    public static class FileModification
    {
        public delegate string NameTransformer(string filename);

        /// <summary>
        /// Rename each file in the given DirectoryInfo location (no children),
        /// using the provided StringTransform.
        /// Note that this is a static method with side effects, so use it with care.
        /// </summary>
        /// <param name="folderAddress"></param>
        /// <param name="StringTransform"></param>
        /// <param name="SkipIfFalse">If a file name doesn't cause this predicate to return true, it won't be renamed</param>
        /// <returns>The total number of files in the directory that were transformed</returns>
        public static int RenameFilesInFolder(this DirectoryInfo folderAddress, NameTransformer StringTransform,
            Predicate<string> SkipIfFalse = null)
        {
            var total = 0;
            foreach (var file in folderAddress.GetFiles("*"))
            {
                if (SkipIfFalse != null && !SkipIfFalse(file.Name))
                {
                    continue;
                }
                total++;

                Directory.Move(file.FullName, file.Directory.FullName + "\\" + StringTransform(file.Name));
            }
            return total;
        }
    }
}