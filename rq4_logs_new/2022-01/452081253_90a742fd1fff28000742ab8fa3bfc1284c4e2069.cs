using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Buddy.Coroutines;
using Clio.Utilities;
using ff14bot;
using ff14bot.Managers;
using ff14bot.Objects;
using LlamaLibrary.Helpers;
using LlamaLibrary.RemoteWindows;

namespace CompanyChest
{
    public static class Nav
    {
        private static bool GetNearbyChest(out GameObject companyChest)
        {
            var chestList = new List<GameObject>();
            foreach (var gameObject in GameObjectManager.GameObjects)
            {
                if (gameObject.EnglishName == "Company Chest")
                {
                    chestList.Add(gameObject);
                }
            }

            companyChest = null;
            if (chestList.Count == 0) return false;
            companyChest = chestList.OrderBy(x => x.DistanceSqr(Core.Me.Location)).First();
            return true;
        }

        public static async Task<bool> GetToChest()
        {
            if (GetNearbyChest(out GameObject chest))
            {
                if (chest.IsWithinInteractRange)
                {
                    CompanyChest.Log.Information("Found an FC Chest right next to us!");
                    return await ChestInteract(chest);
                }
                CompanyChest.Log.Information("Found an FC Chest nearby, getting closer.");
                await Navigation.GetTo(WorldManager.ZoneId, chest.Location);
                return await ChestInteract(chest);;
            }
            CompanyChest.Log.Information("No nearby FC Chest found. Teleporting!");

            // TODO: Add more locations instead of just Limsa.
            await Navigation.GetTo(129, new Vector3(-199.661f, 16f, 57.80525f));
            if (GetNearbyChest(out chest))
            {
                if (chest.IsWithinInteractRange) return await ChestInteract(chest);;
                await Navigation.GetTo(WorldManager.ZoneId, chest.Location);
                return await ChestInteract(chest);;
            }

            return false;
        }

        private static async Task<bool> ChestInteract(GameObject chest)
        {
            chest.Interact();
            await Coroutine.Wait(5000, () => FreeCompanyChest.Instance.IsOpen);
            if (!FreeCompanyChest.Instance.IsOpen)
            {
                CompanyChest.Log.Error("Couldn't get the FreeCompanyChest window open.");
                return false;
            }

            return true;
        }
    }
}