//-----------------------------------------------------------------------------------------------------------
// Copyright (C) 2016-2017 Rival
// 版权所有
//
// 文件名：IEventReceive.cs
//
// 文件功能描述：
//
// 处理广播事件接口，由各场景逻辑实现接口
//
// 创建标识：taixihuase 20160326
//
// 修改标识：
// 修改描述：
// 
//
// 修改标识：
// 修改描述：
//
//----------------------------------------------------------------------------------------------------------

using ExitGames.Client.Photon;

namespace Assets.Scripts.Photon
{
    /// <summary>
    /// 类型：接口
    /// 名称：IEventReceive
    /// 作者：taixihuase
    /// 作用：广播事件处理接口
    /// 编写日期：2016/3/26
    /// </summary>
    interface IEventReceive
    {
        /// <summary>
        /// 类型：方法
        /// 名称：OnResponse
        /// 作者：taixihuase
        /// 作用：当接收到广播时，对广播进行处理
        /// 编写日期：2016/3/26
        /// </summary>
        /// <param name="eventData"></param>
        void OnEvent(EventData eventData);
    }
}