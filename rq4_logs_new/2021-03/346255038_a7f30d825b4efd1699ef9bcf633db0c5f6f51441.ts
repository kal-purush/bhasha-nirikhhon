/*{
  "users": {
    "id1": {
      "v2": {
        "group": {
          "-": {
            "id": "-",
            "name": "Default",
            "timestamp": 0,
          },
          "gid1": {
            "id": "gid1",
            "name": "hello world",
            "timestamp": 5912039192304,
          },
          "gid2": {
            "id": "gid2",
            "name": "group id 2",
            "timestamp": 5912039192304,
          }
        },
        "points": {
          "pid1": {
            "id": "pid1",
            "value": 100,
            "timestamp": 54919293901,
            "group": "gid1",
          },
          "pid2": {
            "id": "pid2",
            "value": 100,
            "timestamp": 81723918283,
            "group": "gid1",
          },
          "pid3": {
            "id": "pid3",
            "value": 100,
            "timestamp": 38291094861,
            "group": "gid1",
          }
        }
      }
    }
  }
}*/

export const USER_PATH = "users";
export const VERSION_PATH = "v2";

export const GROUP_PATH = "group";
export const POINT_PATH = "points";