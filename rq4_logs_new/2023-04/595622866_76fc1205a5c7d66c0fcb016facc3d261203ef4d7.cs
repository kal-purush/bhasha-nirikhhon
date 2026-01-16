using System;
using System.Linq;
using System.Threading.Tasks;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Media.Animation;

namespace FileManager.Helpers
{
    public static class ControlExtensions
    {
        public static async Task GoToVisualStateAsync(this Control control, FrameworkElement visualStatesHost, string stateGroupName, string stateName)
        {
            if (control != null)
            {
                var taskCompletionSource = new TaskCompletionSource<Storyboard>();

                var storyboard = GetStoryboardForVisualState(visualStatesHost, stateGroupName, stateName);
                if (storyboard != null)
                {
                    EventHandler<object> handler = null;
                    handler = (s, e) =>
                    {
                        storyboard.Completed -= handler;
                        taskCompletionSource.SetResult(storyboard);
                    };

                    storyboard.Completed += handler;
                }

                VisualStateManager.GoToState(control, stateName, true);

                if (storyboard != null)
                {
                    await taskCompletionSource.Task;
                }
            }
        }

        private static Storyboard GetStoryboardForVisualState(FrameworkElement visualStatesHost, string stateGroupName, string stateName)
        {
            Storyboard storyboard = null;

            if (visualStatesHost != null)
            {
                VisualStateGroup stateGroup = null;
                var stateGroups = VisualStateManager.GetVisualStateGroups(visualStatesHost);
                if (!string.IsNullOrEmpty(stateGroupName))
                {
                    stateGroup = stateGroups.FirstOrDefault(visualStateGroup => visualStateGroup.Name == stateGroupName);
                }

                VisualState state = null;
                if (stateGroup != null)
                {
                    state = stateGroup.States.FirstOrDefault(visualState => visualState.Name == stateName);
                }

                if (state == null)
                {
                    foreach (var group in stateGroups)
                    {
                        state = group.States.FirstOrDefault(visualState => visualState.Name == stateName);
                        if (state != null)
                        {
                            break;
                        }
                    }
                }

                if (state != null)
                {
                    storyboard = state.Storyboard;
                }
            }

            return storyboard;
        }
    }
}