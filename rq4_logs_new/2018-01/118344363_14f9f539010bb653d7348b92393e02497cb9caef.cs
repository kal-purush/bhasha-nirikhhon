using System;
using System.Diagnostics;
using GrandTheftMultiplayer.Server.API;
using GrandTheftMultiplayer.Server.Constant;
using GrandTheftMultiplayer.Server.Elements;
using GrandTheftMultiplayer.Server.Managers;
using GrandTheftMultiplayer.Shared;
using GrandTheftMultiplayer.Shared.Math;

namespace EnhancedAdminCommands
{
    public class Commands : Script
    {
        private bool IsDebugMode { get; }
        private VehicleConfig VehSpawnConfig { get; }
        private string GroupName { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="Commands"/>
        /// </summary>
        public Commands()
        {
#if DEBUG
            if (Debugger.IsAttached)
            {
                Debugger.Launch();
            }

            IsDebugMode = true;
#else
            IsDebugMode = API.getSetting<bool>("debugmode");
#endif

            VehSpawnConfig = API.getSetting<VehicleConfig>("vehicleconfig");
            GroupName = API.getSetting<string>("groupname");

            if (IsDebugMode)
            {
                API.consoleOutput(LogCat.Debug, "Script started...");
            }
        }
        
        /// <summary>
        /// This method checks if the client has the required ACL permissions.
        /// </summary>
        /// <param name="player"></param>
        /// <returns></returns>
        private bool CanUseCommand(Client player)
        {
            var canUse = API.getPlayerAclGroup(player) == GroupName && API.isPlayerLoggedIn(player);
            if (!canUse)
            {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
            }

            return canUse;
        }
        
        /// <summary>
        /// Spawns a vehicle by a given <param name="hash">hash</param> for the current player
        /// </summary>
        /// <param name="player"></param>
        /// <param name="hash"></param>
        [Command("veh")]
        public void VehCommand(Client player, VehicleHash hash)
        {
            if (!CanUseCommand(player))
            {
                return;
            }
            
            var rotation = API.getEntityRotation(player);
            var vehicle = API.createVehicle(hash, player.position, new Vector3(0, 0, rotation.Z), 0, 0);
            if ((VehSpawnConfig == VehicleConfig.SpawnNextDelete || VehSpawnConfig == VehicleConfig.SpawnPutDelete) && player.isInVehicle)
            {
                API.deleteEntity(player.vehicle);
            }

            if (VehSpawnConfig == VehicleConfig.SpawnPut || VehSpawnConfig == VehicleConfig.SpawnPutDelete)
            {
                API.setPlayerIntoVehicle(player, vehicle, -1);
            }

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Requested Hash is: {hash}");
                API.sendChatMessageToPlayer(player, $"~g~ [DEBUG] {hash} has been spawned!");
            }
        }
        
        /// <summary>
        /// Gives the current player every weapon available
        /// </summary>
        /// <param name="player"></param>
        [Command("allwep")]
        public void GiveAllWeaponsCommand(Client player)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            foreach (WeaponHash hash in Enum.GetValues(typeof(WeaponHash)))
            {
                API.givePlayerWeapon(player, hash, 99999, true, true);
            }
        }

        /// <summary>
        /// Spawns a weapon by a given <param name="hash">hash</param> for the <param name="target">target</param>.
        /// If <param name="target">target</param> is null, the executing player will be selected.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="hash"></param>
        /// <param name="ammo"></param>
        /// <param name="target"></param>
        [Command("givewep")]
        public void GiveWeaponCommand(Client player, WeaponHash hash, int ammo, Client target = null)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            target = target ?? player;

            API.givePlayerWeapon(target, hash, ammo, true, true);

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Gave {target.name} the Weapon: {hash} with {ammo} Ammo.");
            }
        }
        
        /// <summary>
        /// Sets the health for the <param name="target">target</param> to a specified <param name="value">value</param>.
        /// If <param name="target">target</param> is null, the executing player will be selected.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="value"></param>
        /// <param name="target"></param>
        [Command("healthset")]
        public void SetHealthCommand(Client player, int value, Client target = null)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            target = target ?? player;

            API.setPlayerHealth(target, value);

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Set {target.name}'s Health to: {value}");
            }
        }
        
        /// <summary>
        /// Sets the armor for the <param name="target">target</param> to a specified <param name="value">value</param>.
        /// If <param name="target">target</param> is null, the executing player will be selected.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="value"></param>
        /// <param name="target"></param>
        [Command("armorset")]
        public void SetArmorCommand(Client player, int value, Client target = null)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            target = target ?? player;

            API.setPlayerArmor(target, value);

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Set {target.name}'s Armor to: {value}");
            }
        }
        
        /// <summary>
        /// Sets the health for the <param name="target">target</param> to 100.
        /// If <param name="target">target</param> is null, the executing player will be selected.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="target"></param>
        [Command("heal")]
        public void HealCommand(Client player, Client target = null)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            target = target ?? player;

            API.setPlayerHealth(target, 100);

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Fully healed Player: {target.name}");
            }
        }
        
        /// <summary>
        /// Sets the armor for the <param name="target">target</param> to 100.
        /// If <param name="target">target</param> is null, the executing player will be selected.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="target"></param>
        [Command("armor")]
        public void ArmorCommand(Client player, Client target)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            API.setPlayerArmor(target, 100);

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Gave full Armor to Player: {target.name}");
            }
        }
        
        /// <summary>
        /// Fixes the vehicle of the current player.
        /// </summary>
        /// <param name="player"></param>
        [Command("fix")]
        public void FixVehicleCommand(Client player)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            if (!player.isInVehicle)
            {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not in a Vehicle!");
                return;
            }

            player.vehicle.repair();

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Repaired the current Vehicle!");
            }
        }
        
        /// <summary>
        /// Teleports the current player to <param name="target">target</param>'s position.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="target"></param>
        [Command("tele")]
        public void TeleCommand(Client player, Client target)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            player.position = target.position;

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Teleported to: {target.name}");
            }
        }
        
        /// <summary>
        /// Teleports the <param name="target">target</param> to the current player's position.
        /// </summary>
        /// <param name="player"></param>
        /// <param name="target"></param>
        [Command("tpm")]
        public void TeleportHereCommand(Client player, Client target)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            target.position = player.position;

            if (IsDebugMode)
            {
                API.sendChatMessageToPlayer(player, $"~y~ [DEBUG] Teleported {target.name} to you!");
            }
        }

        /// <summary>
        /// Prints the available commands to the current player.
        /// </summary>
        /// <param name="player"></param>
        [Command("help")]
        public void HelpCommand(Client player)
        {
            if (!CanUseCommand(player))
            {
                return;
            }

            API.sendChatMessageToPlayer(player, "~y~ ---- ADMIN COMMANDS ----");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /veh [MODEL] --- Spawns the specified Car");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /allwep --- Gives the executing Player all Weapons");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /givewep [TARGET] [WEAPON] [AMMO] --- Gives the Target Player the specified Weapon with the specified Ammo");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /healthset [TARGET] [VALUE] --- Sets the Health of the specified Player to the specified Amount.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /armorset [TARGET] [VALUE] --- Sets the Armor of the specified Player to the specified Amount.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /heal [TARGET] --- Fully heals the specified target.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /armor [TARGET] --- Gives the specified target full Armor.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /fix --- Repairs the current Vehicle completely.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /tele [TARGET] --- Teleports executing player to the Target.");
            API.sendChatMessageToPlayer(player, "~b~[HELP] [ADMIN] || /tpm [TARGET] --- Teleports target to the executing Player.");
        }
    }
}