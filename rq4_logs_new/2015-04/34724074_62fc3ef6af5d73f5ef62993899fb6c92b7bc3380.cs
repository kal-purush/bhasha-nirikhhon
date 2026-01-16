using System;

using Android.Content;
using Android.Graphics.Drawables;
using Android.Runtime;
using Android.Util;
using Android.Views;
using Android.Widget;

namespace ClearableEditText
{
    public class ClearableEditText : EditText, IListener
    {
        private Drawable xDrawable;
        private IListener listener;

        protected ClearableEditText(IntPtr javaReference, JniHandleOwnership transfer)
            : base(javaReference, transfer)
        {
            Init();
        }

        public ClearableEditText(Context context)
            : base(context)
        {
            Init();
        }

        public ClearableEditText(Context context, IAttributeSet attrs)
            : base(context, attrs)
        {
            Init();
        }

        public ClearableEditText(Context context, IAttributeSet attrs, int defStyle)
            : base(context, attrs, defStyle)
        {
            Init();
        }

        public void SetListener(IListener iListener)
        {
            listener = iListener;
        }

        public override bool OnTouchEvent(MotionEvent e)
        {
            if (GetCompoundDrawables()[2] != null)
            {
                var tappedX = e.GetX() > (Width - PaddingRight - xDrawable.IntrinsicWidth);

                if (tappedX)
                {
                    if (e.Action == MotionEventActions.Up)
                    {
                        Text = string.Empty;

                        if (listener != null)
                        {
                            listener.DidClearTextBox();
                        }
                    }
                    return true;
                }
            }
            return true;
        }

        private void Init()
        {
            xDrawable = GetCompoundDrawables()[2];
            if (xDrawable == null)
            {
                xDrawable = Resources.GetDrawable(Android.Resource.Drawable.PresenceOffline);
            }
            xDrawable.SetBounds(0, 0, xDrawable.IntrinsicWidth, xDrawable.IntrinsicHeight);
            SetClearIconVisible(true);
            listener = this;
        }

        protected void SetClearIconVisible(bool visible)
        {
            bool wasVisible = (GetCompoundDrawables()[2] != null);
            if (visible != wasVisible)
            {
                var x = visible ? xDrawable : null;
                SetCompoundDrawables(GetCompoundDrawables()[0], GetCompoundDrawables()[1], x, GetCompoundDrawables()[3]);
            }
        }
        public void DidClearTextBox()
        {
            Text = string.Empty;
        }
    }
}