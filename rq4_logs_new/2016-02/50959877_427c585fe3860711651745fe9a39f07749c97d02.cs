using System;
using System.Collections.Generic;

using Dapper;

namespace PicnicOrm.Dapper
{
    public class GridReaderWrapper : IGridReader, IDisposable
    {
        #region Fields

        /// <summary>
        /// </summary>
        private readonly SqlMapper.GridReader _gridReader;

        #endregion

        #region Constructors

        /// <summary>
        /// </summary>
        /// <param name="gridReader"></param>
        public GridReaderWrapper(SqlMapper.GridReader gridReader)
        {
            _gridReader = gridReader;
        }

        #endregion

        #region Interfaces

        /// <summary>
        /// </summary>
        public void Dispose()
        {
            _gridReader.Dispose();
        }

        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> Read<T>()
        {
            return _gridReader.Read<T>();
        }

        #endregion
    }
}