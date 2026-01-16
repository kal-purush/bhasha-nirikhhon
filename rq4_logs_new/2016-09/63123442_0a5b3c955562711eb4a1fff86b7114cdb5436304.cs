using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.Kinect.Toolkit.Interaction;
using Reactive.Bindings;

namespace InteractionWpf
{
    public class MainViewModel
    {
        public AppModel AppModel { get; } = new AppModel();

        public ReadOnlyReactiveProperty<string> LeftHandInfo { get; }
        public ReadOnlyReactiveProperty<string> RightHandInfo { get; }

        public ReadOnlyReactiveProperty<string> LeftInteractiveColor { get; }
        public ReadOnlyReactiveProperty<string> RightInteractiveColor { get; }

        public ReadOnlyReactiveProperty<bool> IsLeftGripped { get; }
        public ReadOnlyReactiveProperty<bool> IsRightGripped { get; }

        public ReadOnlyReactiveProperty<bool> IsLeftPressed { get; }
        public ReadOnlyReactiveProperty<bool> IsRightPressed { get; }

        public MainViewModel()
        {
            LeftHandInfo = AppModel.LeftHand.Select(ToString).ToReadOnlyReactiveProperty();
            RightHandInfo = AppModel.RightHand.Select(ToString).ToReadOnlyReactiveProperty();

            LeftInteractiveColor = AppModel.LeftHand.Select(hp => ToInteractiveColor(hp != null && hp.IsInteractive)).ToReadOnlyReactiveProperty();
            RightInteractiveColor = AppModel.RightHand.Select(hp => ToInteractiveColor(hp != null && hp.IsInteractive)).ToReadOnlyReactiveProperty();

            IsLeftGripped = ToGripped(AppModel.LeftHand);
            IsRightGripped = ToGripped(AppModel.RightHand);

            IsLeftPressed = AppModel.LeftHand.Select(ToPressed).ToReadOnlyReactiveProperty();
            IsRightPressed = AppModel.RightHand.Select(ToPressed).ToReadOnlyReactiveProperty();
        }

        static string ToString(InteractionHandPointer hp)
        {
            if (hp == null) return "";

            return string.Join("\r\n", PropertyHelper.GetPropertyValues(hp)
                .Select(p => $"{p.Key}: {p.Value}"));
        }

        static string ToInteractiveColor(bool isInteractive) => isInteractive ? "#009900" : "#FF6666";

        static ReadOnlyReactiveProperty<bool> ToGripped(IObservable<InteractionHandPointer> hps) =>
            hps
                .Where(hp => hp == null || !hp.IsInteractive || hp.HandEventType != InteractionHandEventType.None)
                .Select(hp => hp != null && hp.IsInteractive && hp.HandEventType == InteractionHandEventType.Grip)
                .ToReadOnlyReactiveProperty();

        static bool ToPressed(InteractionHandPointer hp) => hp != null && hp.IsInteractive && hp.IsPressed;
    }

    public static class PropertyHelper
    {
        public static Dictionary<string, object> GetPropertyValues(object obj)
        {
            var properties = TypeDescriptor.GetProperties(obj);
            return properties
                .Cast<PropertyDescriptor>()
                .ToDictionary(p => p.Name, p => p.GetValue(obj));
        }
    }
}