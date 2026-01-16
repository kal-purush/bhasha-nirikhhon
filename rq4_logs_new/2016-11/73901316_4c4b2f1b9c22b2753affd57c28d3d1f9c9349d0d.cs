using System;
using System.IO;

namespace CerealFileTransfer {
    class File {
        private String workingDirectory;
        private Int32 packageSize;

        File(String workingDirectory, Int32 packageSize) {
            this.workingDirectory = workingDirectory;
            this.packageSize = packageSize;
        }

        public Byte[][] Read(String fileName) {
            FileStream fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
            FileInfo fileInfo = new FileInfo(fileName);
            Int32 packages = new Int32();
            packages = Convert.ToInt32(fileInfo.Length / this.packageSize);
            if (fileInfo.Length % this.packageSize != 0) { packages++; }
            Byte[][] package = new Byte[packages][];
            Byte[] buffer = new Byte[this.packageSize];
            for (int i = 0; i < packages; i++) {
                fileStream.Read(buffer, 0, 1);
                package[i] = buffer;
            }
            return package;
        }

        public void Write(String fileName, Byte[][] package) {
            FileStream fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
            Int32 packages = new Int32();
            packages = package.Length;
            for (int i = 0; i < packages; i++) { fileStream.Write(package[i], 0, 1); }
        }
    }
}