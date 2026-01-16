interface InterfaceTypeA {
    getMembers(): { memberA: number; };
}
interface InterfaceTypeB {
    getMembers(): { memberB: number; };
}
interface InterfaceTypeAB extends InterfaceTypeA, InterfaceTypeB {
    getMembers(): { memberA: number; memberB: number; };
}
class X implements InterfaceTypeAB {
    getMembers(): { memberA: number; memberB: number; } {
        return { memberA: 123, memberB: 456 };
    }
}
var a: InterfaceTypeA = new X(); // 型がひつよう
var b = a.getMembers();
console.log(b.memberA);
var c: InterfaceTypeB = new X(); // 型がひつよう
var d = c.getMembers();
console.log(d.memberB);

