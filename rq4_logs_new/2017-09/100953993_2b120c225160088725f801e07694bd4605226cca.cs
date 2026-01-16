using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace UnityModule.Settings {

    /// <summary>
    /// 環境設定を拡張
    /// </summary>
    // ReSharper disable once PartialTypeWithSinglePart
    public partial class EnvironmentSetting : Setting<EnvironmentSetting> {

        /// <summary>
        /// Xcode Archive を起動するかどうかの実体
        /// </summary>
        [SerializeField]
        private bool shouldRunXcodeArchive;

        /// <summary>
        /// Xcode Archive を起動するかどうか
        /// </summary>
        public bool ShouldRunXcodeArchive {
            get {
                return this.shouldRunXcodeArchive;
            }
            set {
                this.shouldRunXcodeArchive = value;
            }
        }

    }

}