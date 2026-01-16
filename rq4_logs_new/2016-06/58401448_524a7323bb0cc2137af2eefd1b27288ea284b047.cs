//---------------------------------------------------------------------------------------
// Copyright 2016 North Carolina State University
//
// Center for Educational Informatics
// http://www.cei.ncsu.edu/
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//   * Redistributions of source code must retain the above copyright notice, this 
//     list of conditions and the following disclaimer.
//   * Redistributions in binary form must reproduce the above copyright notice, 
//     this list of conditions and the following disclaimer in the documentation 
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
// GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
// OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//---------------------------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using System;

namespace IntelliMedia
{
	[RequireComponent(typeof(Animator), typeof(CanvasGroup))]
	public class MultiView<TViewModel> : UnityView<TViewModel> where TViewModel:ViewModel
	{
		public string selectorPropertyName;
		public UnityView[] views;

		private UnityView currentView;
		public UnityView CurrentView 
		{ 
			get { return currentView; } 
			private set 
			{ 
				if (value != currentView) 
				{ 
					UnityView prev = currentView; 
					currentView = value; 
					OnCurrentViewChanged(prev, currentView);
				}
			}
		}

		protected virtual void OnCurrentViewChanged(UnityView oldValue, UnityView newValue)
		{
			if (oldValue != null)
			{
				oldValue.Hide(false, (view) =>
				{
					if (newValue != null)
					{
						newValue.BindingContext = this.ViewModel;
						newValue.Reveal(false);
					}
				});
			}
			else
			{
				if (newValue != null)
				{
					newValue.BindingContext = this.ViewModel;
					newValue.Reveal(false);
				}				
			}			
		}

		protected override void OnInitialize()
		{
			base.OnInitialize();

			// Bind ViewModel properties to OnChanged methods
			Binder.Add<string>(selectorPropertyName, OnSelectorChanged);
		}

		private void OnSelectorChanged(string oldValue, string newValue)
		{
			DebugLog.Info("MultiView.OnSelectorChanged: old='{0}' new='{1}'", 
				oldValue != null ? oldValue : "null",
				newValue != null ? newValue : "null");

			DebugLog.Info("MultiView.OnInitialize: view='{0}'", this.name);

			if (views == null || views.Length == 0)
			{
				throw new Exception("No views defined");
			}

			if (views.Any(v => v == null))
			{
				throw new Exception("View is null");
			}

			CurrentView = views.FirstOrDefault(v => string.Compare(v.name, newValue) == 0);
			if (CurrentView == null)
			{
				DebugLog.Error("{0} MultiView does not contain view with name = '{1}'", this.name, newValue);
			}
		}

		public void NextView()
		{
			if (views == null || views.Length == 0)
			{
				return;
			}

			int currentIndex = GetCurrentViewIndex();
			CurrentView = views[(currentIndex + 1)%views.Length];
		}

		public void PreviousView()
		{
			if (views == null || views.Length == 0)
			{
				return;
			}

			int currentIndex = GetCurrentViewIndex();
			int nextIndex = currentIndex > 0 ? (currentIndex - 1): (views.Length - 1);
			CurrentView = views[nextIndex];
		}

		private int GetCurrentViewIndex()
		{
			if (CurrentView != null)
			{
				for (int index = 0; index < views.Length; ++index)
				{
					if (CurrentView == views[index])
					{
						return index;
					}
				}
			}

			return -1;
		}
	}
}
