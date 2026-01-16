using System.Security;
using System.Security.Permissions;
using BepInEx;
using RoR2;
using UnityEngine;
using MPEventSystemManager = On.RoR2.MPEventSystemManager;

#pragma warning disable CS0618 // Type or member is obsolete
[assembly: SecurityPermission(SecurityAction.RequestMinimum, SkipVerification = true)]
#pragma warning restore CS0618 // Type or member is obsolete
[module: UnverifiableCode]

namespace StopStealingMyMouse
{
    [BepInPlugin(Guid, ModName, Version)]
    public sealed class StopStealingMyMousePlugin : BaseUnityPlugin
    {
        private const string
            ModName = "Stop stealing my mouse!",
            Author = "rune580",
            Guid = "com." + Author + "." + "stopstealingmymouse",
            Version = "1.0.0";
        
        private void Awake()
        {
            MPEventSystemManager.Update += MPEventSystemManagerOnUpdate;
        }

        private void MPEventSystemManagerOnUpdate(MPEventSystemManager.orig_Update orig, RoR2.MPEventSystemManager self)
        {
            if (!RoR2Application.loadFinished)
            {
                Cursor.lockState = CursorLockMode.None;
                Cursor.visible = true;
                return;
            }
            
            orig(self);
        }
    }
}