using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Graphics;
using Android.Graphics.Drawables;
using Android.OS;
using Android.Provider;
using Android.Runtime;
using Android.Text;
using Android.Util;
using Android.Views;
using Android.Widget;
using Java.Lang;
using Org.W3c.Dom;
using Boolean = System.Boolean;

namespace ClearableEditView
{
    public interface IListener
    {
        void DidClearTextBox();
    }

    public class ClearableEditText : EditText, Android.Views.View.IOnFocusChangeListener
    {
        private Drawable xD;
        private IListener _listener;
        private IOnFocusChangeListener f;

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

        public void SetListener(IListener listener)
        {
            _listener = listener;
        }

        public override bool OnTouchEvent(MotionEvent e)
        {
            if (GetCompoundDrawables()[2] != null)
            {
                Boolean tappedX = e.GetX() > (Width - PaddingRight - xD.IntrinsicWidth);

                if (tappedX)
                {
                    if (e.Action == MotionEventActions.Up)
                    {
                        this.Text = string.Empty;

                        if (_listener != null)
                        {
                            _listener.DidClearTextBox();
                        }
                    }
                    return true;
                }
            }
            return true;
        }

        private void Init()
        {
            xD = GetCompoundDrawables()[2];
            if (xD == null)
            {
                xD = Resources.GetDrawable(Android.Resource.Drawable.PresenceOffline);
            }
            xD.SetBounds(0, 0, xD.IntrinsicWidth, xD.IntrinsicHeight);
            this.SetClearIconVisible(true);
        }

        public void OnFocusChange(Android.Views.View v, bool hasFocus)
        {
            SetClearIconVisible(hasFocus);
            if (f != null)
            {
                f.OnFocusChange(v, hasFocus);
            }
        }

        protected void SetClearIconVisible(bool visible)
        {
            bool wasVisible = (GetCompoundDrawables()[2] != null);
            if (visible != wasVisible)
            {
                Drawable x = visible ? xD : null;
                SetCompoundDrawables(GetCompoundDrawables()[0],
                        GetCompoundDrawables()[1], x, GetCompoundDrawables()[3]);
            }
        }
    }
}