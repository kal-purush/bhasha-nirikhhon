// 日本語対応
using Cysharp.Threading.Tasks;
using System;
using System.Threading;

namespace TeamB_TD
{
    namespace NovelGameEditor5.Commands
    {
        public class ActionCamera : ICommand
        {
            private string _actionName;
            private string[] _actionArgs;

            private Func<string[], UniTask> _action;

            public ActionCamera(string[] parametorData)
            {
                _actionName = parametorData[0];
                _actionArgs = parametorData[1..];

                switch (_actionName)
                {
                    case nameof(ShekeCamera): _action = ShekeCamera; break;
                    default: throw new MissingMethodException(_actionName);
                }
            }

            public async UniTask RunCommand(CancellationToken token = default)
            {
                await _action.Invoke(_actionArgs);
            }

#pragma warning disable 1998
            private async UniTask ShekeCamera(string[] parametorData)
            {
                var pawer = float.Parse(parametorData[0]);
                var duration = float.Parse(parametorData[1]);

                // Camera揺らす。
                // 待機する。
                await UniTask.Delay((int)(duration * 1000f));
            }
#pragma warning restore 1998
        }
    }
}