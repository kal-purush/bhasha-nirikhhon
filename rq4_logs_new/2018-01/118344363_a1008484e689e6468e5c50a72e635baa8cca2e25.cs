using System;
using System.IO;
using GrandTheftMultiplayer.Server.API;
using GrandTheftMultiplayer.Server.Constant;
using GrandTheftMultiplayer.Server.Elements;
using GrandTheftMultiplayer.Server.Managers;
using GrandTheftMultiplayer.Shared;
using GrandTheftMultiplayer.Shared.Math;

/* ------------ VEHICLE INFO ------------
* If you want to use other Vehicle Spawn Mathods, set the "vehConfig" int to the desired Method.
* Method 1: Spawns the car next to the Player without Deleting the current one
* Method 2: Spawns the car and the Player automatically is spawned within it without deleting the current one
* Method 3: Like Method 1 just whilst deleting the current car
* Method 4: Like Method 2 just whilst deleting the current car
* Default: "0" (Just that Users need to go into this File and config everything)
* 
* ------------ GIVE ALL WEAPON INFO ------------
* Please specify the Path to the resource Folder, so the script can find the WeaponList (i.E: resources\\cmds\\WeaponList)
* Specify it in the allWepConfig string. 
* Default: "resources\\cmds\\WeaponList"
* 
* ------------ GENERAL INFO ------------
* Currently it's only possible to set target as the specified GTMP Name, will maybe be updated later on to SC.
* 
* ------------ DEBUG INFO ------------
* If you want to enable Debug Messages, set the "debug" bool to "true". 
* Default: "false"
*
* ------------ ACL (ADMIN) INFO ------------
* If your highest group has a diffrent Name and is not default (Admin), set the "groupName" string to your desired ACL Group
* Default: Admin (Case-Sensitive!)
*/


public class Commands : Script
{
    // ---------- Config Values ---------- \\
    public bool debug = false; 
    public int vehConfig = 0; //Set to 0 on purpose. If some users are to lazy to read.
    public string groupName = "Admin";
    public string allWepConfig = "resources\\cmds\\WeaponList";

    //Main Init
    public Commands()
    {
        if (debug) {
            Console.WriteLine("[DEBUG] Script started...");
        }
    }

    // Vehicle Command
    [Command("veh")]
    public void vehCommand(Client player, string request)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            if (vehConfig == 1) {
                int hash = API.getHashKey(request);           
                Vector3 vector = API.getEntityRotation(player.handle);
                Vehicle veh = API.createVehicle(hash, player.position, new Vector3(0, 0, vector.Z), 0, 0);
                if (debug) {                
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Requested Hash is: " + hash);
                    API.sendChatMessageToPlayer(player, "~g~ [DEBUG] " + request + " has been spawned!");
                }
            } else if (vehConfig == 2) {
                int hash = API.getHashKey(request);
                Vector3 vector = API.getEntityRotation(player.handle);
                Vehicle veh = API.createVehicle(hash, player.position, new Vector3(0, 0, vector.Z), 0, 0);
                API.setPlayerIntoVehicle(player, veh, -1);
                if (debug)
                {
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Requested Hash is: " + hash);
                    API.sendChatMessageToPlayer(player, "~g~ [DEBUG] " + request + " has been spawned!");
                }
            } else if (vehConfig == 3) {
                if (player.isInVehicle) {
                    API.deleteEntity(player.vehicle);
                }
                int hash = API.getHashKey(request);
                Vector3 vector = API.getEntityRotation(player.handle);
                Vehicle veh = API.createVehicle(hash, player.position, new Vector3(0, 0, vector.Z), 0, 0);
                if (debug) {
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Requested Hash is: " + hash);
                    API.sendChatMessageToPlayer(player, "~g~ [DEBUG] " + request + " has been spawned!");
                }
            } else if (vehConfig == 4) {
                if (player.isInVehicle) {
                    API.deleteEntity(player.vehicle);
                }
                int hash = API.getHashKey(request);
                Vector3 vector = API.getEntityRotation(player.handle);
                Vehicle veh = API.createVehicle(hash, player.position, new Vector3(0, 0, vector.Z), 0, 0);
                API.setPlayerIntoVehicle(player, veh, -1);
                if (debug) {
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Requested Hash is: " + hash);
                    API.sendChatMessageToPlayer(player, "~g~ [DEBUG] " + request + " has been spawned!");
                }
            } else {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] You did not specify a valid spawning Method in the Config!");
                return;
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //All Weapon Command
    [Command("allwep")]
    public void GiveAllWeaponsCommand(Client player)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            if (File.Exists(allWepConfig)) {
                var weapons = File.ReadAllLines(allWepConfig);
                string name = player.name;
                foreach (var line in weapons) {
                    WeaponHash weapon;
                    weapon = (WeaponHash)System.Enum.Parse(typeof(WeaponHash), line);
                    API.givePlayerWeapon(player, weapon, 9999, true, true);
                    if (debug) {
                        API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Gave " + name + " following Weapon: " + line);
                    }
                }
            } else {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] You did not specify a valid WeaponList Path in the Config!");
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Give Weapon Command
    [Command("givewep")]
    public void GiveWeaponCommand(Client player, Client target, string weapon, int ammo)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            try {
                WeaponHash wep;
                wep = (WeaponHash)System.Enum.Parse(typeof(WeaponHash), weapon);
                API.givePlayerWeapon(target, wep, ammo, true, true);
                if (debug) {
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Gave " + target.name + " the Weapon: " + wep + " with " + ammo + " Ammo.");
                }
            } catch {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] An Error occured. Did you specify a right Weapon Value (Case-Sensitive)?");
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Set Health Command
    [Command("healthset")]
    public void SetHealthCommand(Client player, Client target, int value)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            API.setPlayerHealth(target, value);
            if (debug) {
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Set " + target.name + "'s Health to: " + value);
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Set Armor Command
    [Command("armorset")]
    public void SetArmorCommand(Client player, Client target, int value)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            API.setPlayerArmor(target, value);
            if (debug) { 
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Set " + target.name + "'s Armor to: " + value);
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Heal Command
    [Command("heal")]
    public void HealCommand(Client player, Client target)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            API.setPlayerHealth(target, 100);
            if (debug) { 
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Fully healed Player: " + target.name);
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Armor Command
    [Command("armor")]
    public void ArmorCommand(Client player, Client target)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            API.setPlayerArmor(target, 100);
            if (debug) { 
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Gave full Armor to Player: " + target.name);
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Fix Vehicle Command
    [Command("fix")]
    public void FixVehicleCommand(Client player)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            if (player.isInVehicle) {
                NetHandle veh = API.getPlayerVehicle(player);
                API.repairVehicle(veh);
                 if (debug) {
                    API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Repaired the current Vehicle!");
                }
            } else {
                API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not in a Vehicle!");
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    // Teleport Command
    [Command("tele")]
    public void teleCommand(Client player, Client target)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            player.position = target.position;
            if (debug) {
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Teleported to: "+ target.name);
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Teleport Here Command
    [Command("tpm")]
    public void teleportHereCommand(Client player, Client target)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)) {
            target.position = player.position;
            if (debug) {
                API.sendChatMessageToPlayer(player, "~y~ [DEBUG] Teleported " + target.name + " to you!");
            }
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }

    //Help Command
    [Command("help")]
    public void helpCommand(Client player)
    {
        if (API.getPlayerAclGroup(player) == groupName && API.isPlayerLoggedIn(player)){
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
        } else {
            API.sendChatMessageToPlayer(player, "~r~ [ERROR] You are not authorized or logged in!");
        }
    }
}