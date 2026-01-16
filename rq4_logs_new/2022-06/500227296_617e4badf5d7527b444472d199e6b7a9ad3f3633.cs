using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Win32.Constants
{
    public static class FileAccessRights
    {
        /// <summary>
        /// <para>2</para>
        /// For a directory, the right to create a file in the directory.
        /// </summary>
        public const Int64 FILE_ADD_FILE = 0x0002;

        /// <summary>
        /// <para>4</para>
        /// For a directory, the right to create a subdirectory.
        /// </summary>
        public const Int64 FILE_ADD_SUBDIRECTORY = 0x0004;

        /// <summary>
        /// All possible access rights for a file.
        /// </summary>
        public const Int64 FILE_ALL_ACCESS = STANDARD_RIGHTS_REQUIRED | SYNCHRONIZE | 0x1FF;

        /// <summary>
        /// <para>4</para>
        /// For a file object, the right to append data to the file. (For local files, write operations will not overwrite existing data if this flag is specified without FILE_WRITE_DATA.) For a directory object, the right to create a subdirectory (FILE_ADD_SUBDIRECTORY).
        /// </summary>
        public const Int64 FILE_APPEND_DATA = 0x0004;

        /// <summary>
        /// <para>4</para>
        /// For a named pipe, the right to create a pipe.
        /// </summary>
        public const Int64 FILE_CREATE_PIPE_INSTANCE = 0x0004;

        /// <summary>
        /// <para>64</para>
        /// For a directory, the right to delete a directory and all the files it contains, including read-only files.
        /// </summary>
        public const Int64 FILE_DELETE_CHILD = 0x40;

        /// <summary>
        /// <para>32</para>
        /// For a native code file, the right to execute the file. This access right given to scripts may cause the script to be executable, depending on the script interpreter.
        /// </summary>
        public const Int64 FILE_EXECUTE = 0x20;

        /// <summary>
        /// <para>1</para>
        /// For a directory, the right to list the contents of the directory.
        /// </summary>
        public const Int64 FILE_LIST_DIRECTORY = 0x0001;

        /// <summary>
        /// <para>128</para>
        /// The right to read file attributes.
        /// </summary>
        public const Int64 FILE_READ_ATTRIBUTES = 0x80;

        /// <summary>
        /// <para>1</para>
        /// For a file object, the right to read the corresponding file data. For a directory object, the right to read the corresponding directory data.
        /// </summary>
        public const Int64 FILE_READ_DATA = 0x0001;

        /// <summary>
        /// <para>8</para>
        /// The right to read extended file attributes.
        /// </summary>
        public const Int64 FILE_READ_EA = 0x0008;

        /// <summary>
        /// <para>32</para>
        /// For a directory, the right to traverse the directory. By default, users are assigned the BYPASS_TRAVERSE_CHECKING privilege, which ignores the FILE_TRAVERSE access right. See the remarks in <see href="https://docs.microsoft.com/en-us/windows/win32/fileio/file-security-and-access-rights"> File Security and Access Rights</see> for more information.
        /// </summary>
        public const Int64 FILE_TRAVERSE = 0x20;

        /// <summary>
        /// <para>256</para>
        /// The right to write file attributes.
        /// </summary>
        public const Int64 FILE_WRITE_ATTRIBUTES = 0x100;

        /// <summary>
        /// <para>2</para>
        /// For a file object, the right to write data to the file. For a directory object, the right to create a file in the directory (FILE_ADD_FILE).
        /// </summary>
        public const Int64 FILE_WRITE_DATA = 0x0002;

        /// <summary>
        /// <para>16</para>
        /// The right to write extended file attributes.
        /// </summary>
        public const Int64 FILE_WRITE_EA = 0x10;

        /// <summary>
        /// Includes READ_CONTROL, which is the right to read the information in the file or directory object's security descriptor. This does not include the information in the SACL.
        /// </summary>
        public const Int64 STANDARD_RIGHTS_READ = READ_CONTROL;

        /// <summary>
        /// Same as STANDARD_RIGHTS_READ.
        /// </summary>
        public const Int64 STANDARD_RIGHTS_WRITE = READ_CONTROL;

        public const Int64 READ_CONTROL = 0x00020000L;
        public const Int64 STANDARD_RIGHTS_EXECUTE = READ_CONTROL;
        public const Int64 STANDARD_RIGHTS_REQUIRED = 0x000F0000L;
        public const Int64 DELETE = 0x00010000L;
        public const Int64 WRITE_DAC = 0x00040000L;
        public const Int64 WRITE_OWNER = 0x00080000L;
        public const Int64 SYNCHRONIZE = 0x00100000L;
        public const Int64 STANDARD_RIGHTS_ALL = 0x001F0000L;
        public const Int64 SPECIFIC_RIGHTS_ALL = 0x0000FFFFL;
        /// <summary>
        /// AccessSystemAcl access type
        /// </summary>
        public const Int64 ACCESS_SYSTEM_SECURITY = 0x01000000L;
        /// <summary>
        /// MaximumAllowed access type
        /// </summary>
        public const Int64 MAXIMUM_ALLOWED = 0x02000000L;
        public const Int64 GENERIC_READ = 0x80000000L;
        public const Int64 GENERIC_WRITE = 0x40000000L;
        public const Int64 GENERIC_EXECUTE = 0x20000000L;
        public const Int64 GENERIC_ALL = 0x10000000L;
    }
}