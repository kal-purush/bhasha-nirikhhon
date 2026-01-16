using System;
using System.Runtime.InteropServices;

namespace Kvint
{
    //Структура для возвращения значений
    [StructLayout(LayoutKind.Sequential)]
    public struct CsData
    {
        public double Value;
        public double Time;
        public int ErrorCode;
    }

    //------------------------------------------------------------------------------------------------------------

    public static class CsApi
    {
        //Инициализировать библиотеку CSAPI из пользовательского приложения, возвращает true в случае успеха
        //Следует вызывать до любого другого вызова CSAPI. В коде DLL, работающей под управлением расчетной станции вызывать не нужно.
        [DllImport("CsApi6.dll", EntryPoint = "CSInit")]
        public static extern bool Init();

        //Завершить работу с библиотекой CSAPI из пользовательского приложения
        [DllImport("CsApi6.dll", EntryPoint = "CSDone")]
        public static extern void Done();

        //Получить версию Квинта
        [DllImport("CsApi6.dll", EntryPoint = "CSGetKvintVersion")]
        public static extern int GetKvintVersion();

        //Открыть параметр по CardId, на выходе handle параметра
        [DllImport("CsApi6.dll", EntryPoint = "CSOpenParamById")]
        public static extern int OpenParamById(int cardId, //идентификатор объекта (ключевое поле в таблице Card)
                                                                    int paramNo, //номер параметра объекта (поле в таблице ObjPar), Если paramNo = 0, то главный параметр объекта
                                                                    int flags); //указать 0 для простого получения из архива

        //Открыть параметр по CardId для архивной станции на другом компьютере, на выходе handle параметра
        //Как serverName допустимо указывать короткое NetBIOS-имя, полное DNS-имя (типа computer.domain.local) или IP-адрес.
        //В дублированной сети необходимо указывать полное имя.
        //В случае дублированного архива,  указать имена обоеих станций, разделенных точкой с запятой, например: srv1.domain.local;srv2.domain.local
        [DllImport("CsApi6.dll", EntryPoint = "CSOpenParamExternal")]
        public static extern int OpenParamExternal(int cardId, //идентификатор объекта (ключевое поле в таблице Card)
                                                                         int paramNo, //номер параметра объекта (поле в таблице ObjPar), Если paramNo = 0, то главный параметр объекта
                                                                         string serverName, //сетевое имя компьютера, на котором запущена архивная станция
                                                                         int flags); //указать 0 для простого получения из архива

        //Открыть параметр по марке, на выходе handle параметра
        [DllImport("CsApi6.dll", EntryPoint = "CSOpenParamByName")]
        public static extern int OpenParamByName(string marka, //марка объекта (поле Marka в таблице Card)
                                                                        string paramName, //название параметра объекта (поле ParamName в таблице ObjPar), если paramName = NULL или "", то главный параметр объекта
                                                                        int flags); //указать 0 для простого получения из архива
        //Закрыть параметр 
        [DllImport("CsApi6.dll", EntryPoint = "CSCloseHandle")]
        public static extern void CloseHandle(int handle);//handle параметра

        //Найти в архиве значение параметра на момент времени, ближайший к указанному
        //true, если значение найдено
        [DllImport("CsApi6.dll", EntryPoint = "CSFindFirst")]
        public static extern bool FindFirst(int handle, //handle параметра
                                                            ref CsData data, //возвращаемые данные
                                                            double time, //указанное время
                                                            int direction = 1); //1 (направление поиска)
        //[MarshalAs(UnmanagedType.LPStruct)] 

        //Найти в архиве следующее значение параметра
        //true, если значение найдено
        [DllImport("CsApi6.dll", EntryPoint = "CSFindNext")]
        public static extern bool FindNext(int handle, //handle параметра
                                                            ref CsData data, //возвращаемые данные
                                                            int direction = 1); //1

        //Получить значение параметра, если получено достоверное значение параметра, возвращает true
        [DllImport("CsApi6.dll", EntryPoint = "CSReadData")]
        public static extern bool ReadData(int handle, //handle параметра
                                                             ref CsData data); //Возвращаемое значение
        
        //Время по Гринвичу секундах, прошедших с 1 января 1970
        [DllImport("CsApi6.dll", EntryPoint = "CSGetTime")]
        public static extern double GetTime();

        //Перевести время из формата Windows в формат Квинт
        [DllImport("CsApi6.dll", EntryPoint = "CSTimePack")]
        public static extern double TimePack(string s, //исходное время в формате Windows
                                                               ref double time); //возвращаемое время в формате Квинт

        //Переводит время Квинт в строку
        [DllImport("CsApi6.dll", EntryPoint = "CSTimeToString")]
        public static extern double TimeToString(string buffer, //приемный буфер
                                                                      int bufSize, //размер буфера
                                                                      double time, //время в формате Квинт
                                                                      string dateFormat = "0",
                                                                      string timeFormat = "0",
                                                                      int flags = 0,
                                                                      int timeZone = 1);

        //Переводит время из квинтового в нормальное
        public static DateTime KvintToTime(double d)
        {
            return new DateTime(1970, 1, 1).AddSeconds(d);
        }

        //Переводит время из нормального в квинтовое
        public static double TimeToKvint(DateTime t)
        {
            return t.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
        }

    }
}