export class Penalty {
    id : number;
    penalty : string;
}

export class Packet {
    id : number;
    fullName : string;
    icon : string;
    primaryInquiries : InvestigatorInquiry[];
    secondaryInquiries : InvestigatorInquiry[];
    roleCards : SuspectRole[]
}

export class RoleType {
    id : number;
    flavourName : string;
    name : string;
    info : string;
}

export class SuspectRole {
    id : number;
    flavour : string;
    type : number;
    goals : string[];
}

export class SuspectNote {
    id : number;
    name : string;
}

export class InvestigatorInquiry {
    id : number;
    inquiry : string;
    questions : string[]; 
}