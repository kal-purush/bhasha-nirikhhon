using Sim;
using Sim.Building;
using UnityEngine;

namespace AI.States {
    public class CharacterInteract : IState {
        
        private readonly Character character;

        private Props interactedProps;

        public CharacterInteract(Character character) {
            this.character = character;
        }

        public Props InteractedProps {
            get => interactedProps;
            set => interactedProps = value;
        }

        public void OnEnter() {
            Debug.Log($"Start to interact with {this.interactedProps.name}");
        }

        public void Tick() {
        }

        public void OnExit() {
            this.interactedProps.StopInteraction();
        }
    }
}