using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LocusCommon.Serialization.Bases
{
    public interface ILocusFormatter
    {
        // プロパティ


        // メソッド

        /// <summary>
        /// 指定したオブジェクトをシリアライズします。
        /// </summary>
        /// <param name="obj"></param>
        void Serialize(Object obj);

        /// <summary>
        /// 指定したストリームから、指定した型でデシリアライズします。
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="targetType"></param>
        /// <returns></returns>
        Object Deserialize(Stream stream, Type targetType);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="stream"></param>
        /// <returns></returns>
        TTarget Deserialize<TTarget>(Stream stream);
    }
}