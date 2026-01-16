using MapGenerator;
using System.Collections;
using System.Collections.Generic;
using TiledSharp;
using UnityEngine;

public class DebugGameStarter : MonoBehaviour
{
    private void Start()
    {
        List<DungeonRoom> dungeonRooms = new List<DungeonRoom>();

        List<Fast2DArray<int>> roomLayouts = new List<Fast2DArray<int>>();
        List<List<TiledImporter.PrefabLocations>> roomGameObjects = new List<List<TiledImporter.PrefabLocations>>();
        List<RoomType> roomTypes = new List<RoomType>();

        roomLayouts.Add(TiledImporter.Instance.GetReplacableMap("startRoom", out PropertyDict properties, out List<TiledImporter.PrefabLocations> gos));
        if (properties.TryGetValue("roomType", out string value) == false)
            throw new System.Exception("No room type in map: startRoom!");
        if (int.TryParse(value, out int roomType) == false)
            throw new System.Exception("Room type is not an integer in: startRoom!");
        roomTypes.Add((RoomType)roomType);
        roomGameObjects.Add(gos);

        roomLayouts.Add(TiledImporter.Instance.GetReplacableMap("bossRoom", out properties, out gos));
        if (properties.TryGetValue("roomType", out value) == false)
            throw new System.Exception("No room type in map: bossRoom!");
        if (int.TryParse(value, out roomType) == false)
            throw new System.Exception("Room type is not an integer in: bossRoom!");
        roomTypes.Add((RoomType)roomType);
        roomGameObjects.Add(gos);

        int mapCount = 7;
        for (int i = 1; i <= mapCount; i++)
        {
            roomLayouts.Add(TiledImporter.Instance.GetReplacableMap("room" + i.ToString(), out properties, out gos));

            // Example:
            if (properties.TryGetValue("roomType", out value) == false)
                throw new System.Exception("No room type in map: room" + i + "!");

            if (int.TryParse(value, out roomType) == false)
                throw new System.Exception("Room type is not an integer in: room" + i + "!");

            // do something with the roomType here
            roomTypes.Add((RoomType)roomType);

            roomGameObjects.Add(gos);
        }

        DungeonCreator.Instance.dungeon = new MapGenerator.Dungeon(192, 192, roomLayouts.ToArray(), roomGameObjects.ToArray(), roomTypes.ToArray(), 10, int.MaxValue);
    }
}