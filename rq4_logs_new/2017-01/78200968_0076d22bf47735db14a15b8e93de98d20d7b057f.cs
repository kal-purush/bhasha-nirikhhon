using Android.Content;
using Android.Database.Sqlite;
using System;
using System.IO;
using System.Reflection;


namespace CarbCounter
{

    public class SQLiteUtil : SQLiteOpenHelper
    {
        private static string DATABASE_DIRECTORY =
            System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal);

        private static string DATABASE_FILE_NAME = "Spotlight.sqlite";
        private static int VERSION = 1;
        private Context context = null;
        static SQLiteDatabase _objSQLiteDatabase = null;

        public SQLiteUtil(Context _context) : base(_context, DATABASE_FILE_NAME, null, VERSION)
        {
            this.context = _context;
        }

        #region implemented abstract members of SQLiteOpenHelper

        public override void OnCreate(SQLiteDatabase _database)
        {
            CreateSQLiteDatabase();
        }

        public override void OnUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
        {

        }

        #endregion

        public override SQLiteDatabase WritableDatabase
        {
            get
            {
                SQLiteDatabase _objBaseSQLiteDatabase = base.WritableDatabase;
                if (_objSQLiteDatabase == null)
                {
                    CreateSQLiteDatabase();
                }
                return _objSQLiteDatabase;
            }
        }

        public SQLiteDatabase CreateSQLiteDatabase()
        {
            string _strSQLitePathOnDevice = GetSQLitePathOnDevice();
            Stream _streamSQLite = null;
            FileStream _streamWrite = null;
            Boolean isSQLiteInitialized = false;

            try
            {
                if (File.Exists(_strSQLitePathOnDevice))
                {
                    isSQLiteInitialized = true;
                }
                else
                {
                    //_streamSQLite = context.Resources.OpenRawResource(Resource.Raw.Spotlight);
                    _streamSQLite = context.Resources.OpenRawResource(1);
                    _streamWrite = new FileStream(_strSQLitePathOnDevice, FileMode.OpenOrCreate, FileAccess.Write);
                    if (_streamSQLite != null && _streamWrite != null)
                    {
                        if (CopySQLiteOnDevice(_streamSQLite, _streamWrite))
                        {
                            isSQLiteInitialized = true;
                        }
                    }
                }
                if (isSQLiteInitialized)
                {
                    _objSQLiteDatabase = SQLiteDatabase.OpenDatabase(_strSQLitePathOnDevice, null,
                        DatabaseOpenFlags.OpenReadonly);
                }
            }
            catch (Exception _exception)
            {
                MethodBase _currentMethod = MethodInfo.GetCurrentMethod();
                Console.WriteLine(String.Format("CLASS : {0}; METHOD : {1}; EXCEPTION : {2}"
                    , _currentMethod.DeclaringType.FullName
                    , _currentMethod.Name
                    , _exception.Message));
            }
            return _objSQLiteDatabase;
        }

        private string GetSQLitePathOnDevice()
        {
            string _strSQLitePathOnDevice = string.Empty;
            try
            {
                _strSQLitePathOnDevice = Path.Combine(DATABASE_DIRECTORY, DATABASE_FILE_NAME);
            }
            catch (Exception _exception)
            {
                MethodBase _currentMethod = MethodInfo.GetCurrentMethod();
                Console.WriteLine(String.Format("CLASS : {0}; METHOD : {1}; EXCEPTION : {2}"
                    , _currentMethod.DeclaringType.FullName
                    , _currentMethod.Name
                    , _exception.Message));
            }
            return _strSQLitePathOnDevice;
        }

        private bool CopySQLiteOnDevice(Stream _streamSQLite, Stream _streamWrite)
        {
            bool _isSuccess = false;
            int _length = 256;
            Byte[] _buffer = new Byte[_length];
            try
            {
                int _bytesRead = _streamSQLite.Read(_buffer, 0, _length);
                while (_bytesRead > 0)
                {
                    _streamWrite.Write(_buffer, 0, _bytesRead);
                    _bytesRead = _streamSQLite.Read(_buffer, 0, _length);
                }
                _isSuccess = true;
            }
            catch (Exception _exception)
            {
                MethodBase _currentMethod = MethodInfo.GetCurrentMethod();
                Console.WriteLine(String.Format("CLASS : {0}; METHOD : {1}; EXCEPTION : {2}"
                    , _currentMethod.DeclaringType.FullName
                    , _currentMethod.Name
                    , _exception.Message));
            }
            finally
            {
                _streamSQLite.Close();
                _streamWrite.Close();
            }
            return _isSuccess;
        }
    }
}