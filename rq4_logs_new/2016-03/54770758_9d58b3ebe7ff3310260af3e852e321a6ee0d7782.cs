//-----------------------------------------------------------------------------------------------------------
// Copyright (C) 2016-2017 Rival
// 版权所有
//
// 文件名：NetTime.cs
//
// 文件功能描述：
//
// 获取网络时间并计时
//
// 创建标识：taixihuase 20160329
//
// 修改标识：
// 修改描述：
// 
//
// 修改标识：
// 修改描述：
//
//----------------------------------------------------------------------------------------------------------

using System;
using System.Globalization;
using System.Threading;
using Assets.Scripts.Photon;
using ExitGames.Concurrency.Fibers;
using Protocol;

namespace Assets.Scripts.Components
{
    /// <summary>
    /// 类型：类
    /// 名称：NetTime
    /// 作者：taixihuase
    /// 作用：网络时间类
    /// 编写日期：2016/3/29
    /// </summary>
    public sealed class NetTime
    {
        public static readonly NetTime Instance = new NetTime();

        private DateTime _timer;

        private bool _isPause;

        private bool _isStart;

        private readonly ExtendedPoolFiber _fiber;

        private readonly ExtendedPoolFiber _checkFiber;

        private const long Duration = 500;

        /// <summary>
        /// 类型：方法
        /// 名称：NetTime
        /// 作者：taixihuase
        /// 作用：构造网络时间类
        /// 编写日期：2016/3/29
        /// </summary>
        private NetTime()
        {
            _fiber = new ExtendedPoolFiber();
            _checkFiber = new ExtendedPoolFiber();
            Init();
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Init
        /// 作者：taixihuase
        /// 作用：初始化其余数据
        /// 编写日期：2016/3/29
        /// </summary>
        private void Init()
        {
            _fiber.Start();
            _checkFiber.Start();
            _isPause = false;
            _isStart = false;
            _fiber.Enqueue(Start);
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Start
        /// 作者：taixihuase
        /// 作用：每间隔 1000 毫秒检测网络，连通时获取北京时间，并开始监测网络状态和时间自动更新
        /// 编写日期：2016/3/29
        /// </summary>
        private void Start()
        {
            while (PhotonSingleton.Service == null || PhotonSingleton.Service.ServerConnected == false)
            {
                Thread.Sleep(1000);
            }
            _timer = TimeTool.GetBeijingTime();
            _isStart = true;
            _fiber.ScheduleOnInterval(Update, Duration, Duration);
            _checkFiber.Enqueue(CheckNetwork);
        }

        /// <summary>
        /// 类型：方法
        /// 名称：CheckNetwork
        /// 作者：taixihuase
        /// 作用：每间隔 5000 毫秒检测网络，连通时保持或恢复计时，断开时暂停计时
        /// 编写日期：2016/3/29
        /// </summary>
        private void CheckNetwork()
        {
            while (true)
            {
                if ((PhotonSingleton.Service == null || PhotonSingleton.Service.ServerConnected == false))
                {
                    if (_isStart && _isPause == false)
                        Pause();
                }
                else
                {
                    if (_isStart && _isPause)
                        Resume();
                }
                Thread.Sleep(5000);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Update
        /// 作者：taixihuase
        /// 作用：自动更新时间
        /// 编写日期：2016/3/29
        /// </summary>
        private void Update()
        {
            _timer = _timer.AddMilliseconds(Duration);
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Pause
        /// 作者：taixihuase
        /// 作用：暂停计时，此方法只会禁止获取时间，后台仍会继续更新时间
        /// 编写日期：2016/3/29
        /// </summary>
        public void Pause()
        {
            lock (this)
            {
                _isPause = true;
            }
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Resume
        /// 作者：taixihuase
        /// 作用：恢复计时，此方法将允许获取时间
        /// 编写日期：2016/3/29
        /// </summary>
        public void Resume()
        {
            lock (this)
            {
                _isPause = false;
            }
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Get
        /// 作者：taixihuase
        /// 作用：安装指定格式或默认格式获取时间字符串表示，当计时未开始或暂停时返回 null
        /// 编写日期：2016/3/29
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
        public string Get(string format = "yyyy-M-d H:m:s")
        {
            if (!_isStart || _isPause)
                return null;
            return format == null ? _timer.ToString(CultureInfo.InvariantCulture) : _timer.ToString(format);
        }
    }
}