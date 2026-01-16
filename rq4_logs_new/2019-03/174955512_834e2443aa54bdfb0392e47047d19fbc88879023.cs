using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MissionManager
{
    const int Size = 3;
    const int LvUpByClearCount = 5;
    MissionBase[] nowMissions;
    MissionBase[,] missions;
    public int ClearCount {get; private set;}
    public int MissionLv {get; private set;}

    public MissionManager(){
        MissionBase CreateMission(MissionType type){
            switch(type){
                case MissionType.Chain:
                return new ChainMission();
                case MissionType.Score:
                return new ScoreMission();
                case MissionType.Same:
                return new SameMission();
            }
            throw new System.ArgumentOutOfRangeException();
        }

        MissionLv = 1;
        nowMissions = new MissionBase[Size];
        var size = (int)MissionType.size;
        missions = new MissionBase[size, Size];

        for (int i=0; i<size; ++i){
            for (int j=0; j<Size; ++j){
                missions[i, j] = CreateMission((MissionType)i);
            }
        }
        for (int i=0; i<Size; ++i){
            Pop(false);
        }
    }

    private MissionBase CreateNewMission(int lv){
        MissionBase DecideMission(int type){
            for(int i = 0; i < Size; ++i){
                var target = missions[type, i];
                if(!target.OnUse) return target;
            }
            throw new System.Exception("nanka okashii");
        }
        var targetNum = Random.Range(0, (int)MissionType.size);
        var targetMission = DecideMission(targetNum);
        targetMission.DecideMission(lv);
        return targetMission;
    }

    public void Pop(bool isClear = true){
        if(isClear){
            ClearCount++;
            if(ClearCount % LvUpByClearCount == 0){
                MissionLv++;
            }
        }
        nowMissions[0]?.Inactivate();
        for (int i = 0; i < (Size - 1); ++i){
            nowMissions[i] = nowMissions[i+1];
        }
        var newMission = CreateNewMission(MissionLv);
        nowMissions[Size - 1] = newMission;
        newMission.Activate();
    }

    public void MissionCheck(ScoreStruct score){
        var toNext = nowMissions[0].CheckClear(score);
        if(toNext) Pop();
    }
}