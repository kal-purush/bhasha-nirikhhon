using System;
using UnityEngine;
using Core;
using Data;

namespace Behaviour
{
    /**
     * <summary>
     * Control the display of a lifebar
     * </summary>
     */
    [RequireData(typeof(CharacterStatData))]
    public class ControlLifeBarDisplay : MonoDataBehaviour
    {
        # region Properties

        /**
         * <summary>
         * The concerned game object
         * </summary>
         */
        [SerializeField]
        private GameObject _subject = null;

        /**
         * <summary>
         * The show animation trigger name
         * </summary>
         */
        [SerializeField]
        private string _showTriggerName = "Show";

        /**
         * <summary>
         * A reference to the character stat
         * </summary>
         */
        private CharacterStatData _character = null;

        /**
         * <summary>
         * A reference to the animator
         * </summary>
         */
        private Animator _animator = null;

        # endregion

        # region PropertyAccessors
        # endregion

        # region PublicMethods
        # endregion

        # region PrivateMethods

        /**
         * <inheritdoc/>
         */
        protected override void Init(MonoDataContainer container)
        {
            if (null == _subject)
                throw new Exception("You must attach a subject to the lifebar display.");

            _character = GetData<CharacterStatData>();
            _animator  = _subject.GetComponent<Animator>();
        }

        /**
         * <inheritdoc/>
         */
        private void OnEnable()
        {
            // Attach the damage event
            _character.onTakingDamage.AddListener(DisplayLifeBar);
        }

        /**
         * <inheritdoc/>
         */
        private void OnDisable()
        {
            _character.onTakingDamage.RemoveListener(DisplayLifeBar);
        }

        /**
         * <summary>
         * Display the life bar
         * </summary>
         */
        private void DisplayLifeBar(GameObject subject, CharacterStatData.Damage damage)
        {
            _animator.SetTrigger(_showTriggerName);
        }

        # endregion
    }
}