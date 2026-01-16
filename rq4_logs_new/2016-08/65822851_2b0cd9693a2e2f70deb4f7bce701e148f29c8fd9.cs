using System.Collections.Generic;
using KangEngine.Core;
using KangEngine.Input.Base;
using UnityEngine;

namespace KangEngine.Input
{
    public class InputManager : KangSingleTon<InputManager>
    {
        private Dictionary<string, InputBase> _controllers;

        public InputManager()
        {
            _controllers = new Dictionary<string, InputBase>();
        }

        public void AddInput(string inputName, InputBase input)
        {
            if (this._controllers.ContainsKey(inputName))
                return;

            this._controllers[inputName] = input;
        }

        public InputBase GetInput(string inputName)
        {
            InputBase input;
            this._controllers.TryGetValue(inputName, out input);
            return input;
        }

        public void RemoveInput(string inputName)
        {
            if (this._controllers.ContainsKey(inputName))
                this._controllers.Remove(inputName);
        }

        public float GetAxis(string controllerName, AxisType axisType)
        {
            InputBase input = GetInput(controllerName);
            if (input != null)
            {
                if (axisType == AxisType.AT_Horizontal)
                    return input.AxisValueX;
                else if (axisType == AxisType.AT_Vertical)
                    return input.AxisValueY;
            }

            return 0f;
        }


        public bool GetAxisEnable(string controllerName, AxisType axisType)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return false;

            if (axisType == AxisType.AT_Horizontal)
                return input.enableAxisX;
            else if (axisType == AxisType.AT_Vertical)
                return input.enableAxisY;

            return false;
        }

        public void SetAxisEnable(string controllerName, AxisType axisType, bool value)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return;

            if (axisType == AxisType.AT_Horizontal)
                input.enableAxisX = value;
            else if (axisType == AxisType.AT_Vertical)
                input.enableAxisY = value;
        }


        public bool GetAxisInverse(string controllerName, AxisType axisType)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return false;

            if (axisType == AxisType.AT_Horizontal)
                return input.inverseAxisX;
            else if (axisType == AxisType.AT_Vertical)
                return input.inverseAxisY;

            return false;
        }

        public void SetAxisInverse(string controllerName, AxisType axisType, bool value)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return;

            if (axisType == AxisType.AT_Horizontal)
                input.inverseAxisX = value;
            else if (axisType == AxisType.AT_Vertical)
                input.inverseAxisY = value;
        }

        public float GetSensitivity(string controllerName)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return 0f;

            return input.sensitivity;
        }

        public void SetSensitivity(string controllerName, float value)
        {
            InputBase input = GetInput(controllerName);
            if (input == null)
                return;

            input.sensitivity = value;
        }

        public void Update()
        {
            int touchCount = UnityEngine.Input.touchCount;
            if (touchCount > 0)
            {
                for (int cnt = 0; cnt < touchCount; cnt++)
                {
                    List<string> keys = new List<string>(this._controllers.Keys);
                    for (int idx = 0; idx < keys.Count; ++idx)
                    {
                        InputBase input = null;
                        if (this._controllers.TryGetValue(keys[idx], out input))
                            TouchInput(UnityEngine.Input.GetTouch(cnt), input);
                    }
                }

                return;
            }

            List<string> akeys = new List<string>(this._controllers.Keys);
            for (int idx = 0; idx < akeys.Count; ++idx)
            {
                InputBase input = null;
                if (this._controllers.TryGetValue(akeys[idx], out input))
                    MouseInput(input);
            }
        }

        private void TouchInput(Touch touch, InputBase controller)
        {
            switch (touch.phase)
            {
                case TouchPhase.Began:
                    if (controller.CheckTouchPosition(touch.position) && !controller.touchDown)
                    {
                        controller.touchId = touch.fingerId;
                        controller.UpdatePosition(touch.position);
                    }
                    break;

                case TouchPhase.Stationary:
                case TouchPhase.Moved:
                    if (controller.touchId == touch.fingerId && controller.touchDown)
                    {
                        controller.UpdatePosition(touch.position);
                    }
                    break;

                case TouchPhase.Ended:
                case TouchPhase.Canceled:
                    if (controller.touchId == touch.fingerId && controller.touchDown)
                    {
                        controller.ControlReset();
                    }
                    break;
            }
        }

        private void MouseInput(InputBase controller)
        {
            if (controller.CheckTouchPosition(UnityEngine.Input.mousePosition) && UnityEngine.Input.GetMouseButtonDown(0))
                controller.UpdatePosition(UnityEngine.Input.mousePosition);

            if (controller.touchDown && UnityEngine.Input.GetMouseButton(0))
                controller.UpdatePosition(UnityEngine.Input.mousePosition);

            if (UnityEngine.Input.GetMouseButtonUp(0) && controller.touchDown)
                controller.ControlReset();
        }
    }
}