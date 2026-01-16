using UnityEngine;
using System.Collections;

public class EventManager : MonoBehaviour {

    //enum
    public enum SpecialType {
        CHILD, STUDENT, UNIVERSITY, UNEMPLOYED, WORKER, CHICKEN, SENIOR, END
    }

    public enum BranchType {
        CSAT, JOBHUNT, DARWINISM, MARRIAGE, END
    }

	//variable
    delegate void CallForNormal();
    event CallForNormal EventForNormalStart;
    event CallForNormal EventForNormalEnd;
    delegate void CallForSpecial(SpecialType eventType);
    event CallForSpecial EventForSpecialStart;
    event CallForSpecial EventForSpecialEnd;
    delegate void CallForBranch(BranchType eventType);
    event CallForBranch EventForBranchStart;
    event CallForBranch EventForBranchEnd;

    IEnumerator Start() {
        yield return null;
    }

    public void AddEventForNormalStart(CallForNormal normalEvent) {
        EventForNormalStart += normalEvent;
    }

    public void AddEventForNormalEnd(CallForNormal normalEvent) {
        EventForNormalEnd += normalEvent;
    }

    public void AddEventForSpecialStart(CallForSpecial specialEvent) {
        EventForSpecialStart += specialEvent;
    }

    public void AddEventForSpecialStart(CallForSpecial specialEvent) {
        EventForSpecialEnd += specialEvent;
    }

    public void AddEventForBranchStart(CallForBranch branchEvent) {
        EventForBranchStart += branchEvent;
    }

    public void AddEventForBranchEnd(CallForBranch branchEvent) {
        EventForBranchEnd += branchEvent;
    }
}