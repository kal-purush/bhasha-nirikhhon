/**
 * Class that holds the distroGroups for chooseSplitIndex Algorithm
 */
class Distribution {
    private DistroGroup firstGroup; // The first distribution group
    private DistroGroup secondGroup; // The second distribution group

    Distribution(DistroGroup firstGroup, DistroGroup secondGroup) {
        this.firstGroup = firstGroup;
        this.secondGroup = secondGroup;
    }

    DistroGroup getFirstGroup() {
        return firstGroup;
    }

    DistroGroup getSecondGroup() {
        return secondGroup;
    }
}