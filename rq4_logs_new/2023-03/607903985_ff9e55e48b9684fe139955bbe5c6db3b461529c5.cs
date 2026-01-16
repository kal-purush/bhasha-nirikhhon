#if UNITY_EDITOR
using UnityEngine;
using UnityEditor;
using UnityEditor.Animations;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace Galaktikos.AnimationTools
{
	public class AnimationGenerator : EditorWindow
	{
		private string Name;
		private AnimatorController Animator;
		private string Group;
		private Transform ParentObject;

		public enum AnimationType { Toggle, Selector }
		private AnimationType Type;

		// Toggle
		private List<Transform> ToggleObjects;

		// Selector
		private class SelectorItem
		{
			public string Name;
			public Transform Transform;
			public bool BaseState;
		}

		private bool SelectorNoneOption;
		private List<SelectorItem> SelectorItems;

		[MenuItem("Window/Galaktikos/Animation Tools/Animation Generator")]
		private static void Init()
		{
			AnimationGenerator window = (AnimationGenerator)GetWindow(typeof(AnimationGenerator));
			window.titleContent = new GUIContent("Animation Generator");
			window.Show();
		}

		private void OnGUI()
		{
			Name = EditorGUILayout.TextField("Name", Name);
			Group = EditorGUILayout.TextField("Group (optional)", Group);
			Animator = (AnimatorController)EditorGUILayout.ObjectField("Animator", Animator, typeof(AnimatorController), true);
			ParentObject = (Transform)EditorGUILayout.ObjectField("Parent Object", ParentObject, typeof(Transform), true);
			GUILayout.Space(20);

			Type = (AnimationType)EditorGUILayout.EnumPopup("Type", Type);

			switch (Type)
			{
				case AnimationType.Toggle:
					{
						if (ToggleObjects == null)
							ToggleObjects = new List<Transform>();

						int newCount = Mathf.Max(0, EditorGUILayout.IntField("Objects", ToggleObjects.Count));
						while (newCount < ToggleObjects.Count)
							ToggleObjects.RemoveAt(ToggleObjects.Count - 1);
						while (newCount > ToggleObjects.Count)
							ToggleObjects.Add(null);

						for (int i = 0; i < ToggleObjects.Count; i++)
							ToggleObjects[i] = (Transform)EditorGUILayout.ObjectField(ToggleObjects[i], typeof(Transform), true);

						GUILayout.Space(20);
						if (GUILayout.Button("Create"))
						{
							if (string.IsNullOrEmpty(Name))
								Debug.LogError("Name cannot be empty.");
							else if (Animator == null)
								Debug.LogError("Animator cannot be empty.");
							else
							{
								string animatorDirectory = Path.GetDirectoryName(AssetDatabase.GetAssetPath(Animator));
								string path = string.IsNullOrEmpty(Group) ? Name : Path.Combine(Group, Name);

								AnimatorStateMachine stateMachine = new AnimatorStateMachine
								{
									name = path,
									hideFlags = HideFlags.HideInHierarchy,
									entryPosition = Vector3.zero,
									anyStatePosition = new Vector3(0, 50, 0),
									exitPosition = new Vector3(0, 100, 0)
								};

								AssetDatabase.AddObjectToAsset(stateMachine, AssetDatabase.GetAssetPath(Animator));

								AnimatorControllerLayer layer = new AnimatorControllerLayer
								{
									name = path,
									blendingMode = AnimatorLayerBlendingMode.Override,
									defaultWeight = 1,
									stateMachine = stateMachine
								};

								Animator.AddParameter(path, AnimatorControllerParameterType.Bool);

								// Validate animation directory
								string animationDirectory = animatorDirectory;

								if (!string.IsNullOrEmpty(Group))
									foreach (string folder in Group.Split('\\'))
									{
										string newDirectory = Path.Combine(animationDirectory, folder);

										if (!AssetDatabase.IsValidFolder(newDirectory))
											AssetDatabase.CreateFolder(animationDirectory, folder);

										animationDirectory = newDirectory;
									}

								// Find object path
								string[] objectPaths = new string[ToggleObjects.Count];

								for (int i = 0; i < ToggleObjects.Count; i++)
								{
									Transform transform = ToggleObjects[i];
									objectPaths[i] = ToggleObjects[i].name;

									while (transform.parent != null && transform.parent != ParentObject)
									{
										transform = transform.parent;
										objectPaths[i] = transform.name + "/" + objectPaths[i];
									}
								}

								// Off state
								AnimatorState offState = stateMachine.AddState("Off", new Vector3(200, 0, 0));
								offState.writeDefaultValues = false;
								stateMachine.defaultState = offState;

								AnimationClip animationclip = new AnimationClip
								{
									name = Name + " Off",
									frameRate = 1
								};

								foreach (string objectPath in objectPaths)
									animationclip.SetCurve(objectPath, typeof(GameObject), "m_IsActive", AnimationCurve.Constant(0, 0, 0));

								AssetDatabase.CreateAsset(animationclip, Path.Combine(animationDirectory, animationclip.name + ".anim"));
								offState.motion = animationclip;

								// On state
								AnimatorState onState = stateMachine.AddState("On", new Vector3(200, 50, 0));
								onState.writeDefaultValues = false;

								animationclip = new AnimationClip
								{
									name = Name + " On",
									frameRate = 1
								};

								foreach (string objectPath in objectPaths)
									animationclip.SetCurve(objectPath, typeof(GameObject), "m_IsActive", AnimationCurve.Constant(0, 0, 1));

								AssetDatabase.CreateAsset(animationclip, Path.Combine(animationDirectory, animationclip.name + ".anim"));
								onState.motion = animationclip;

								AnimatorStateTransition offTransition = new AnimatorStateTransition()
								{
									name = "On",
									destinationState = onState,
									hasExitTime = false,
									exitTime = 0,
									hasFixedDuration = false,
									duration = 0,
									conditions = new AnimatorCondition[] { new AnimatorCondition() {
										parameter = path,
										mode = AnimatorConditionMode.If
									}},
									hideFlags = HideFlags.HideInHierarchy
								};
								AssetDatabase.AddObjectToAsset(offTransition, AssetDatabase.GetAssetPath(Animator));
								offState.AddTransition(offTransition);

								AnimatorStateTransition onTransition = new AnimatorStateTransition()
								{
									name = "Off",
									destinationState = offState,
									hasExitTime = false,
									exitTime = 0,
									hasFixedDuration = false,
									duration = 0,
									conditions = new AnimatorCondition[] { new AnimatorCondition() {
										parameter = path,
										mode = AnimatorConditionMode.IfNot
									}},
									hideFlags = HideFlags.HideInHierarchy
								};
								AssetDatabase.AddObjectToAsset(onTransition, AssetDatabase.GetAssetPath(Animator));
								onState.AddTransition(onTransition);

								Animator.AddLayer(layer);
							}
						}
					}
					break;

				case AnimationType.Selector:
					{
						SelectorNoneOption = GUILayout.Toggle(SelectorNoneOption, "None Option");

						if (SelectorItems == null)
							SelectorItems = new List<SelectorItem>();

						int newCount = Mathf.Max(0, EditorGUILayout.IntField("Objects", SelectorItems.Count));
						while (newCount < SelectorItems.Count)
							SelectorItems.RemoveAt(SelectorItems.Count - 1);
						while (newCount > SelectorItems.Count)
							SelectorItems.Add(new SelectorItem());

						for (int i = 0; i < SelectorItems.Count; i++)
						{
							GUILayout.Space(10);
							SelectorItems[i].Name = EditorGUILayout.TextField("Name", SelectorItems[i].Name);
							SelectorItems[i].Transform = (Transform)EditorGUILayout.ObjectField(SelectorItems[i].Transform, typeof(Transform), true);
							SelectorItems[i].BaseState = EditorGUILayout.Toggle("Base State", SelectorItems[i].BaseState);
						}

						GUILayout.Space(20);
						if (GUILayout.Button("Create"))
						{
							if (SelectorItems.Count <= 0)
							{
								Debug.LogError("Must have at least one item");
								return;
							}

							List<string> names = new List<string>();
							foreach (SelectorItem selectorItem in SelectorItems)
							{
								if (string.IsNullOrEmpty(selectorItem.Name))
								{
									Debug.LogError("All items must have a name");
									return;
								}

								foreach (string name in names)
									if (name == selectorItem.Name)
									{
										Debug.LogError("Items cannot have duplicate names");
										return;
									}

								names.Add(selectorItem.Name);
							}

							string animatorDirectory = Path.GetDirectoryName(AssetDatabase.GetAssetPath(Animator));
							string path = string.IsNullOrEmpty(Group) ? Name : Path.Combine(Group, Name);

							Animator.AddParameter(path, AnimatorControllerParameterType.Int);

							// Base layer
							AnimatorStateMachine baseStateMachine = new AnimatorStateMachine
							{
								name = path + " Base",
								hideFlags = HideFlags.HideInHierarchy,
								entryPosition = Vector3.zero,
								anyStatePosition = new Vector3(0, 50, 0),
								exitPosition = new Vector3(0, 100, 0)
							};

							AssetDatabase.AddObjectToAsset(baseStateMachine, AssetDatabase.GetAssetPath(Animator));

							AnimatorControllerLayer baseLayer = new AnimatorControllerLayer
							{
								name = path + " Base",
								blendingMode = AnimatorLayerBlendingMode.Override,
								defaultWeight = 1,
								stateMachine = baseStateMachine
							};

							// Selector layer
							AnimatorStateMachine selectorStateMachine = new AnimatorStateMachine
							{
								name = path,
								hideFlags = HideFlags.HideInHierarchy,
								entryPosition = Vector3.zero,
								anyStatePosition = new Vector3(0, 50, 0),
								exitPosition = new Vector3(0, 100, 0)
							};

							AssetDatabase.AddObjectToAsset(selectorStateMachine, AssetDatabase.GetAssetPath(Animator));

							AnimatorControllerLayer selectorLayer = new AnimatorControllerLayer
							{
								name = path,
								blendingMode = AnimatorLayerBlendingMode.Override,
								defaultWeight = 1,
								stateMachine = selectorStateMachine
							};

							// Validate animation directory
							string animationDirectory = animatorDirectory;

							List<string> folders = string.IsNullOrEmpty(Group) ? new List<string>() : Group.Split('\\').ToList();
							folders.Add(Name);

							foreach (string folder in folders)
							{
								string newDirectory = Path.Combine(animationDirectory, folder);

								if (!AssetDatabase.IsValidFolder(newDirectory))
									AssetDatabase.CreateFolder(animationDirectory, folder);

								animationDirectory = newDirectory;
							}

							// Find object path
							string[] objectPaths = new string[SelectorItems.Count];

							for (int i = 0; i < SelectorItems.Count; i++)
							{
								Transform transform = SelectorItems[i].Transform;
								objectPaths[i] = transform.name;

								while (transform.parent != null && transform.parent != ParentObject)
								{
									transform = transform.parent;
									objectPaths[i] = transform.name + "/" + objectPaths[i];
								}
							}

							// Base state
							AnimatorState baseState = baseStateMachine.AddState("Base", new Vector3(200, 0, 0));
							baseState.writeDefaultValues = false;
							baseStateMachine.defaultState = baseState;

							AnimationClip baseAnimation = new AnimationClip
							{
								name = "Base",
								frameRate = 1
							};

							for (int i = 0; i < SelectorItems.Count; i++)
								baseAnimation.SetCurve(objectPaths[i], typeof(GameObject), "m_IsActive", AnimationCurve.Constant(0, 0, SelectorItems[i].BaseState ? 1 : 0));

							AssetDatabase.CreateAsset(baseAnimation, Path.Combine(animationDirectory, baseAnimation.name + ".anim"));
							baseState.motion = baseAnimation;

							Animator.AddLayer(baseLayer);

							// Selector states
							if (SelectorNoneOption)
							{
								AnimatorState noneState = selectorStateMachine.AddState("None", new Vector3(200, 0, 0));
								noneState.writeDefaultValues = false;
								baseState.motion = baseAnimation;

								AnimatorStateTransition transition = selectorStateMachine.AddAnyStateTransition(noneState);
								transition.name = "None";
								transition.hasExitTime = false;
								transition.exitTime = 0;
								transition.hasFixedDuration = false;
								transition.duration = 0;
								transition.conditions = new AnimatorCondition[] { new AnimatorCondition() {
									parameter = path,
									mode = AnimatorConditionMode.Equals,
									threshold = 0
								}};
								transition.hideFlags = HideFlags.HideInHierarchy;
							}

							for (int i = 0; i < SelectorItems.Count; i++)
							{
								int iOffset = i + (SelectorNoneOption ? 1 : 0);

								AnimatorState itemState = selectorStateMachine.AddState(SelectorItems[i].Name, new Vector3(200, iOffset * 50, 0));
								itemState.writeDefaultValues = false;

								AnimationClip itemAnimation = new AnimationClip
								{
									name = SelectorItems[i].Name,
									frameRate = 1
								};

								itemAnimation.SetCurve(objectPaths[i], typeof(GameObject), "m_IsActive", AnimationCurve.Constant(0, 0, SelectorItems[i].BaseState ? 0 : 1));

								AssetDatabase.CreateAsset(itemAnimation, Path.Combine(animationDirectory, itemAnimation.name + ".anim"));
								itemState.motion = itemAnimation;

								AnimatorStateTransition transition = selectorStateMachine.AddAnyStateTransition(itemState);
								transition.name = SelectorItems[i].Name;
								transition.hasExitTime = false;
								transition.exitTime = 0;
								transition.hasFixedDuration = false;
								transition.duration = 0;
								transition.conditions = new AnimatorCondition[] { new AnimatorCondition() {
									parameter = path,
									mode = AnimatorConditionMode.Equals,
									threshold = iOffset
								}};
								transition.hideFlags = HideFlags.HideInHierarchy;
							}

							Animator.AddLayer(selectorLayer);
						}
					}
					break;
			}
		}
	}
}
#endif