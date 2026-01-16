using UnityEngine;
using System.Collections;
using MiniJSON;
using System.Collections.Generic;
using System.IO;


public class BalloonParameter : MonoBehaviour
{

    const string RESOURCE_PATH = "/Pandemic/Resource/JSON/";

    [SerializeField]
    string JSON_PATH = "";

    public enum Attribute
    {
        ANIME,
        IDOL,
        GAME,
        VOCALOID,
        ROBOT,
        ERROR,
    };

    struct BalloonData
    {
        public string _name;
        public float _attacK_power;
        public float _attack_speed;
        public int _id;
        public Attribute _attribute;
    };

    Dictionary<int, BalloonData> _balloon_table = new Dictionary<int, BalloonData>();
    Dictionary<int, string> _balloon_name_table = new Dictionary<int, string>();

    public string GetName(int id)
    {
        if (!_balloon_name_table.ContainsKey(id)) return "error";
        return _balloon_name_table[id];
    }

    public float GetAttackPower(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return 0.0f;
        return _balloon_table[hash_code]._attacK_power;
    }

    public float GetAttackPower(int id)
    {
        return GetAttackPower(GetName(id));
    }

    public float GetAttackSpeed(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return 0.0f;
        return _balloon_table[hash_code]._attack_speed;
    }

    public float GetAttackSpeed(int id)
    {
        return GetAttackSpeed(GetName(id));
    }

    public int GetId(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return -1;
        return _balloon_table[hash_code]._id;
    }

    public Attribute GetAttribute(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return Attribute.ERROR;
        return _balloon_table[hash_code]._attribute;
    }

    public Attribute GetAttribute(int id)
    {
        return GetAttribute(GetName(id));
    }

    public bool IsAnime(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return false;
        if (_balloon_table[hash_code]._attribute != Attribute.ANIME) return false;
        return true;
    }

    public bool IsAnime(int id)
    {
        return IsAnime(GetName(id));
    }

    public bool IsIdol(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return false;
        if (_balloon_table[hash_code]._attribute != Attribute.IDOL) return false;
        return true;
    }

    public bool IsIdol(int id)
    {
        return IsIdol(GetName(id));
    }

    public bool IsGame(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return false;
        if (_balloon_table[hash_code]._attribute != Attribute.GAME) return false;
        return true;
    }

    public bool IsGame(int id)
    {
        return IsGame(GetName(id));
    }

    public bool IsVocaloid(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return false;
        if (_balloon_table[hash_code]._attribute != Attribute.VOCALOID) return false;
        return true;
    }

    public bool IsVocaloid(int id)
    {
        return IsVocaloid(GetName(id));
    }

    public bool IsRobot(string name)
    {
        var hash_code = name.GetHashCode();
        if (!_balloon_table.ContainsKey(hash_code)) return false;
        if (_balloon_table[hash_code]._attribute != Attribute.ROBOT) return false;
        return true;
    }

    public bool IsRobot(int id)
    {
        return IsRobot(GetName(id));
    }


    void Awake()
    {
        var text = File.ReadAllText(Application.dataPath + RESOURCE_PATH + JSON_PATH);

        JsonNode json = JsonNode.Parse(text);
        var balloons = json["Balloons"];

        Dictionary<int, Attribute> balloon_attribute_table = new Dictionary<int, Attribute>();
        balloon_attribute_table.Add(0, Attribute.ANIME);
        balloon_attribute_table.Add(1, Attribute.IDOL);
        balloon_attribute_table.Add(2, Attribute.GAME);
        balloon_attribute_table.Add(3, Attribute.VOCALOID);
        balloon_attribute_table.Add(4, Attribute.ROBOT);



        for (var i = 0; i < balloons.Count; ++i)
        {
            BalloonData balloon_data = new BalloonData();
            var ballon = balloons[i];
            balloon_data._name = ballon["Name"].Get<string>();
            balloon_data._id = (int)ballon["ID"].Get<long>();
            balloon_data._attacK_power = (float)ballon["AttackPower"].Get<double>();
            balloon_data._attack_speed = (float)ballon["AttackSpeed"].Get<double>();

            var enemy_attribute_num = (int)ballon["Attribute"].Get<long>();
            balloon_data._attribute = balloon_attribute_table[enemy_attribute_num];

            _balloon_table.Add(balloon_data._name.GetHashCode(), balloon_data);
            _balloon_name_table.Add(balloon_data._id, balloon_data._name);
        }
    }
}