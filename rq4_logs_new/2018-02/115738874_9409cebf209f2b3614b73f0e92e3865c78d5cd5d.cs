using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Threading.Tasks;

namespace GGM.Web.Router.Util
{
    internal static class TaskUtil
    {
        private delegate object TaskResultResolver(Task task);

        private static object _createResolverLock = new object();
        private static Dictionary<Type, TaskResultResolver> _taskResultResolvers = new Dictionary<Type, TaskResultResolver>();

        /// <summary>
        /// Task의 반환 자료형을 동적으로 판단하여 가져와 object로 반환합니다.
        /// </summary>
        /// <param name="task">Completed된 Task </param>
        /// <returns>Task&ltT&gt의 결과</returns>
        public static object GetResultFromTask(Task task)
        {
            if (!task.IsCompleted)
                throw new System.Exception("The task is not completed.");
            var resolver = GetTaskResultResolver(task.GetType());
            return resolver(task);
        }

        private static TaskResultResolver GetTaskResultResolver(Type type)
        {
            if (!_taskResultResolvers.ContainsKey(type))
            {
                // Resolver가 없을 경우 생성.
                lock (_createResolverLock)
                {
                    // 재 진입 후에도 다시한번 체크
                    if (!_taskResultResolvers.ContainsKey(type))
                    {
                        var dynamicMethod = new DynamicMethod(
                            name: $"{nameof(TaskResultResolver)}+{type}+{Guid.NewGuid()}"
                          , returnType: typeof(object)
                          , parameterTypes: new[] {typeof(Task)});

                        var il = dynamicMethod.GetILGenerator();
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, type);
                        il.Emit(OpCodes.Call, type.GetProperty("Result").GetGetMethod());
                        il.Emit(OpCodes.Ret);
                        _taskResultResolvers[type] = dynamicMethod.CreateDelegate(typeof(TaskResultResolver)) as TaskResultResolver;
                    }
                }
            }

            return _taskResultResolvers[type];
        }
    }
}