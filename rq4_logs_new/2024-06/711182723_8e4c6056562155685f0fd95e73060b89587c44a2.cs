using K3.Physics;
using UnityEngine;

namespace K3.Mech {
    public class Amortization : MechComponent {
        [SerializeField] Transform target;
        public Transform AmortizerLayer { get; private set; }
        void Start() {
            var p = target.parent;
            var bob = new GameObject("[Amortization Layer]");
            bob.transform.parent = p;
            bob.transform.SetLocalPositionAndRotation(Vector3.zero, Quaternion.identity);
            target.SetParent(bob.transform, false);
            AmortizerLayer = bob.transform;
        }

        Vector3 torque = default;
        
        [SerializeField] float springForce;
        // [SerializeField] float angleForceCutoff;
        [SerializeField][Range(1f, 10f)]float springExponent;
        [SerializeField][Range(0f, 0.5f)]float damping;

        [SerializeField] float newtonsToDegreesPerSecondConversion;

        public void ApplyLocalTorque(Vector3 lt) {
            torque += lt;
        }

        public void ApplyWorldForcePulse(Vector3 position, Vector3 force, bool useMass) {
            // var localForcePulse = AmortizerLayer.InverseTransformVector(worldspacePulse);
            var rb = Mech.GetSystem<IMechPhysics>().MechRigidbody;
            var pulse = MechUtils.CalculatePulse(AmortizerLayer.transform.position, position, force);
            var localPulseAxis = AmortizerLayer.InverseTransformVector(pulse.impartedRotationAxis);

            // Debug.Log($"Pulse translation dir: {pulse.translationDirection}");
            var translationNewtons = force.normalized * pulse.translationAmount;
            var addedTorque = localPulseAxis * pulse.tangentialAmount * newtonsToDegreesPerSecondConversion / (useMass ? rb.mass : 1f) ;
            torque += addedTorque;
            rb.AddForce(translationNewtons, useMass ? ForceMode.Impulse : ForceMode.VelocityChange);
        }

        public Quaternion RotationEquilibrium { get; set; } = Quaternion.identity;

        private void FixedUpdate() {
            var equilibriumRotation = RotationEquilibrium;
            RotationEquilibrium = Quaternion.identity;

            var rotationToAchieveEquilibrium = Quaternion.Inverse(AmortizerLayer.localRotation) * equilibriumRotation;
            rotationToAchieveEquilibrium.ToAngleAxis(out var displacement, out var axis);

            // displacement = Mathf.Min(displacement, angleForceCutoff);
            // springs react to displacement
            var reactionAmount = Mathf.Pow(displacement, springExponent) * Time.fixedDeltaTime * springForce;

            torque += axis * reactionAmount; // maybe minus?

            torque *= (1f - damping);
        }

        private void LateUpdate() {
            AmortizerLayer.localRotation = Quaternion.Euler(torque * Time.deltaTime) * AmortizerLayer.localRotation;
        }
    }
}