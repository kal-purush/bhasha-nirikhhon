using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LocusCommon.Serialization.Bases
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class LocusFormatterBase : ILocusFormatter
    {
        // コンストラクタ

        /// <summary>
        /// 新しい <see cref="LocusFormatterBase"/> クラスのインスタンスを初期化します。
        /// </summary>
        public LocusFormatterBase()
        {
            // 実装なし
        }


        // 公開メソッド

        /// <summary>
        /// 使用できません。
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="obj"></param>
        public virtual object Deserialize(Stream stream, Type targetType)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 使用できません。
        /// </summary>
        /// <typeparam name="TTarget"></typeparam>
        /// <param name="stream"></param>
        /// <returns></returns>
        public virtual TTarget Deserialize<TTarget>(Stream stream)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 使用できません。
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="obj"></param>
        public virtual void Serialize(Stream stream, object obj)
        {
            throw new NotImplementedException();
        }
    }
}