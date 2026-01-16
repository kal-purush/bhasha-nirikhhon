using Godot;
using System;

namespace NewGameProject.scripts.Skills
{
    public class Skill
    {
        public string Name { get; set; }          // Tên của kỹ năng
        public float Cooldown { get; set; }       // Thời gian hồi chiêu
        public bool IsActive { get; set; }        // Trạng thái mở khóa
        public string IconPath { get; set; }
        public Action OnUse { get; set; }         // Delegate để thực hiện logic kỹ năng

        // Constructor
        public Skill(string name, float cooldown, bool isActive, string iconPath, Action onUse = null)
        {
            Name = name;
            Cooldown = cooldown;
            IsActive = isActive;
            IconPath = iconPath;
            OnUse = onUse;
        }

        // Phương thức thực thi kỹ năng
        public void Use()
        {
            if (OnUse != null)
            {
                OnUse.Invoke(); // Gọi logic thực thi
            }
            else
            {
                GD.Print($"Skill {Name} has no implementation yet.");
            }
        }
    }
}