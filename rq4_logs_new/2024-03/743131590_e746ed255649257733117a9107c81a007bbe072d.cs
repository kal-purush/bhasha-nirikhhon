using UnityEngine;

namespace SoulRunProject.InGame
{
    /// <summary>
    /// レベルアップ時の獲得アイテムの管理所持
    /// </summary>
    public class LevelUpItemManager : MonoBehaviour
    {
        [Header("スキルレベルアップ系")]
        [SerializeField] private SkillLevelUpItem[] _skillLevelUpItems;
        [Header("ステータスアップ系")]
        [SerializeField] private StatusUpItem[] _statusUpItems;

        public SkillLevelUpItem[] SkillLevelUpItems => _skillLevelUpItems;
        public StatusUpItem[] StatusUpItems => _statusUpItems;
    }
}