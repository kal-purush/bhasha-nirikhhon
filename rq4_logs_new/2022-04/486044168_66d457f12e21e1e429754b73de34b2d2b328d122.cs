using System.Collections;
using System.Collections.Generic;
using RPG.Core;
using RPG.Movement;
using UnityEngine;

namespace RPG.Combat
{
    public class Fighter : MonoBehaviour, IAction
    {
        // GR: Configurable variables
        [SerializeField] float weaponRange = 2f;
        [SerializeField] float timeBetweenAttacks = 1f;
        [SerializeField] float weaponDamage = 5f;

        // GR: State variables
        Health target;
        float timeSinceLastAttack = Mathf.Infinity; // GR: So the very first attack happens immediately, not after timeBetweenAttacks

        // GR: Referenced variables
        Mover mover;
        Animator animator;

        private void Start()
        {
            mover = GetComponent<Mover>();
            animator = GetComponent<Animator>();
        }

        private void Update()
        {
            timeSinceLastAttack += Time.deltaTime;

            if (!target) return;
            if (target.IsDead()) return;

            if (!GetIsInRange()) // GR: Check if enemy is in range of the weapon, and if not, move towards enemy.
            {
                mover.MoveTo(target.transform.position, 1f);
            }
            else // GR: If in range, stop moving toward enemy and start attacking.
            {
                mover.Cancel();
                AttackBehaviour();
            }
        }

        private void AttackBehaviour()
        {
            transform.LookAt(target.transform); // GR: Face the character at the target it is going to attack.
            if (timeSinceLastAttack > timeBetweenAttacks)
            {
                TriggerAttackAnimations();
                timeSinceLastAttack = 0f;
            }
        }

        private void TriggerAttackAnimations()
        {
            // GR: This was added to remove the bug where you might set stopAttack trigger in Cancel, but not actually let it be consumed by the Animator Controller (by calling Cancel early). This would then cause the stopAttack animation to trigger when attacking again.
            animator.ResetTrigger("stopAttack");
            // GR: This will also trigger the Hit() event, if reached and not cancelled before it happens, attached to the animation (originally standing melee attack downward_OnSpot_HitEvent.anim)
            animator.SetTrigger("attack");
        }

        private bool GetIsInRange()
        {
            return (Vector3.Distance(transform.position, target.transform.position) < weaponRange);
        }

        public bool CanAttackTarget(GameObject combatTarget)
        {
            if (combatTarget == null) return false;

            Health targetToTest = combatTarget.GetComponent<Health>();
            return ((targetToTest != null) && (!targetToTest.IsDead()));
        }

        public void Attack(GameObject combatTarget)
        {
            GetComponent<ActionScheduler>().StartAction(this);
            target = combatTarget.GetComponent<Health>();
        }

        public void Cancel()
        {
            CancelAttackAnimations();
            target = null;
            mover.Cancel(); // GR: If you were in the middle of running to the enemy to fight and cancelled, then the actual movement is also cancelled so you don't carry on running to the enemy.
        }

        private void CancelAttackAnimations()
        {
            animator.ResetTrigger("attack"); // GR: Just a safeguard to ensure that the trigger is "fresh" the next time you attack something, and not possibly not consumed yet by the Animator Controller.
            animator.SetTrigger("stopAttack");
        }

        // GR: ANIMATION EVENT BELOW. This was created in the animation (originally standing melee attack downward_OnSpot_HitEvent.anim)
        void Hit()
        {
            if (target == null) return;

            target.TakeDamage(weaponDamage);
        }
    }
}
