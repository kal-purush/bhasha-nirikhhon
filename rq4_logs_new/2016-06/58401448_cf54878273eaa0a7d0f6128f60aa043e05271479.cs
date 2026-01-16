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
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;

namespace IntelliMedia
{
	/// <summary>
	/// Add tab and return key navigation to selectable controls that are children of the GameObject
	/// that this component is applied to.
	/// </summary>
	// Code originally copied from: http://forum.unity3d.com/threads/tab-between-input-fields.263779/
	public class TabAndEnterKeyNavigation : MonoBehaviour
	{
		public bool enableTabKeyNavigation;
		public bool enableEnterKeyNavigation;

		private void Update()
		{
			if (EventSystem.current.currentSelectedGameObject == null
				|| ((!enableTabKeyNavigation || !Input.GetKeyDown(KeyCode.Tab))
					&& (!enableEnterKeyNavigation || !Input.GetKeyDown(KeyCode.Return))))
			{
				return;
			}
				
			Selectable current = EventSystem.current.currentSelectedGameObject.GetComponent<Selectable>();
			if (current == null)
			{
				return;
			}

			bool childSelected = false;
			for (Transform parent = current.transform; parent != null; parent = parent.transform.parent)
			{
				if (parent == this.gameObject.transform)
				{
					childSelected = true;
					break;
				}
			}

			if (!childSelected)
			{
				return;
			}

			bool up = Input.GetKey(KeyCode.LeftShift) || Input.GetKey (KeyCode.RightShift);
			Selectable next = up ? current.FindSelectableOnUp() : current.FindSelectableOnDown();

			// We are at the end or the beginning, go to either, depends on the direction we are tabbing in
			// The previous version would take the logical 0 selector, which would be the highest up in your editor hierarchy
			// But not certainly the first item on your GUI, or last for that matter
			// This code tabs in the correct visual order
			if (next == null)
			{
				next = current;

				Selectable pnext;
				if(up) while((pnext = next.FindSelectableOnDown()) != null) next = pnext;
				else while((pnext = next.FindSelectableOnUp()) != null) next = pnext;
			}

			// Simulate Inputfield MouseClick
			InputField inputfield = next.GetComponent<InputField>();
			if (inputfield != null) inputfield.OnPointerClick(new PointerEventData(EventSystem.current));

			// Select the next item in the taborder of our direction
			EventSystem.current.SetSelectedGameObject(next.gameObject);
		}
	}
}