//-----------------------------------------------------------------------------------------------------------
// Copyright (C) 2016-2017 Rival
// 版权所有
//
// 文件名：LoadSceneAsync.cs
//
// 文件功能描述：
//
// 进行场景的异步加载和加载界面显示
//
// 创建标识：taixihuase 20160328
//
// 修改标识：
// 修改描述：
// 
//
// 修改标识：
// 修改描述：
//
//----------------------------------------------------------------------------------------------------------

using System.Collections;
using Protocol;
using UnityEngine;

namespace Assets.Scripts.Components
{
    /// <summary>
    /// 类型：类
    /// 名称：LoadSceneAsync
    /// 作者：taixihuase
    /// 作用：异步加载场景
    /// 编写日期：2016/3/28
    /// </summary>
    public class LoadSceneAsync : MonoBehaviour
    {
        private AsyncOperation _async;

        private int _toProgress;

        private int _displayProgress;

        // Use this for initialization
        private void Start()
        {
            Load();
        }

        // Update is called once per frame
        private void Update()
        {
        }

        private void OnDisable()
        {
            StopAllCoroutines();
        }

        /// <summary>
        /// 类型：方法
        /// 名称：Load
        /// 作者：taixihuase
        /// 作用：异步加载指定场景
        /// 编写日期：2016/3/28
        /// </summary>
        public void Load()
        {
            var sceneCode = SceneManager.Instance.Peek();
            if (sceneCode != null)
            {
                StartCoroutine(Loading(sceneCode.Value));
            }
        }

        /// <summary>
        /// 类型：协程方法
        /// 名称：Loading
        /// 作者：taixihuase
        /// 作用：进行异步加载场景
        /// 编写日期：2016/3/28
        /// </summary>
        /// <param name="scene"></param>
        /// <returns></returns>
        private IEnumerator Loading(SceneManager.SceneCode scene)
        {
            _async = UnityEngine.SceneManagement.SceneManager.LoadSceneAsync(scene.GetDescription());
            _async.allowSceneActivation = false;

            while (_async.progress < 0.9f)
            {
                _toProgress = (int) (_async.progress*100);
                if (_displayProgress < _toProgress)
                {
                    ++_displayProgress;
                    Debug.Log(_displayProgress);
                }
            }

            _toProgress = 100;
            while (_displayProgress < _toProgress)
            {
                ++_displayProgress;
                Debug.Log(_displayProgress);
                yield return new WaitForEndOfFrame();
            }

            yield return new WaitForSeconds(0.5f);

            _async.allowSceneActivation = true;
        }
    }
}