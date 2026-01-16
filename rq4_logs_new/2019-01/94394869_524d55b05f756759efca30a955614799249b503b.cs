using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Content.PM;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using YanSoft.CurrencyExchanger.Core.Utils;

namespace YanSoft.CurrencyExchanger.Droid.Utils
{
    public class AppVersionHelper : IAppVersionHelper
    {

        private PackageInfo _packageInfo;
        public AppVersionHelper()
        {
            var context = Android.App.Application.Context;
            _packageInfo = context.PackageManager.GetPackageInfo(context.PackageName, PackageInfoFlags.MetaData);
        }
        public string GetBuildNumber()
        {
            return _packageInfo.VersionCode.ToString();
        }

        public string GetBuildVersion()
        {
            return _packageInfo.VersionName;
        }
    }
}