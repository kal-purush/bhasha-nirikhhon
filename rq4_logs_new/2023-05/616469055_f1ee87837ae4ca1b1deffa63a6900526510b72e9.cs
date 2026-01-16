using System.Collections.Generic;
using Runtime.Interaction;
using Unity.Netcode;
using UnityEngine;
using UnityEngine.Events;
using NetworkEvent = Runtime.Networking.NetworkEvent.NetworkEvent;

namespace Runtime.Misc
{
     [RequireComponent(typeof(NetworkObject))]
     public class SequenceInteraction : NetworkBehaviour
     {
          [SerializeField, Tooltip("Should be added in the right sequence")] private List<NetworkedInteractable> interactables = new();

          private NetworkVariable<int> _currentTarget = new();

          public NetworkEvent onValidInteraction = new NetworkEvent();
          public NetworkEvent onInvalidInteraction = new NetworkEvent();

          public NetworkEvent onSequenceComplete = new NetworkEvent();

          public UnityEvent<NetworkedInteractable> onNewTargetInteractable = new();
          
          public override void OnNetworkSpawn()
          {
               InitializeEvents();
               foreach (var networkedInteractable in interactables)
               {
                    networkedInteractable.onInteract.AddListener(() =>
                    {
                         HandleInteractableInteract(networkedInteractable);
                    });
               }

               _currentTarget.OnValueChanged += (_, _) => HandleNewTargetInteractable();
          }

          private void InitializeEvents()
          {
               onInvalidInteraction.Initialize(this);
               onValidInteraction.Initialize(this);
               onSequenceComplete.Initialize(this);
               
          }

          private void HandleNewTargetInteractable()
          {
               if (_currentTarget.Value >= interactables.Count)
               {
                    if (IsServer) onSequenceComplete?.Invoke();
                    return;
               }
               onNewTargetInteractable?.Invoke(interactables[_currentTarget.Value]);
          }

          public override void OnDestroy()
          {
               onValidInteraction?.Dispose();
               onSequenceComplete?.Dispose();
               onInvalidInteraction?.Dispose();
               
               base.OnDestroy();
          }

          private void HandleInteractableInteract(NetworkedInteractable interactable)
          {
               if (interactables[_currentTarget.Value] == interactable)
               {
                   HandleValidInteraction();
               }
               else
               {
                    HandleInvalidInteraction();
               }
          }

          private void HandleInvalidInteraction()
          {
               _currentTarget.Value = 0;
               onInvalidInteraction?.Invoke();
          }

          private void HandleValidInteraction()
          {
               _currentTarget.Value += 1;
               onValidInteraction?.Invoke();
          }

          public List<NetworkedInteractable> Interactables
          {
               get => interactables;
          }

          public NetworkedInteractable TargetInteractable => interactables[_currentTarget.Value];
     }
}