package com.imodule.sort;

public enum KinshipEnum {
    husband(0,"丈夫"),
    wife(1, "妻子"),
    son(2, "儿子"),
    daughter(3, "女儿"),
    father(4,"父亲"),
    mother(5,"母亲"),
    fatherinlaw(6, "岳父"),
    motherinlaw(7, "岳母"),
    family1(8,"公公"),
    family2(9,"家公"),
    familyWife1(10, "婆婆"),
    familyWife2(11, "家婆"),
    brother(12,"哥哥"),
    sister(13, "姐姐"),
    youngBrother(14, "弟弟"),
    youngSister(15, "妹妹"),
    grandson(16, "孙子"),
    granddaughter(17, "孙女"),
    grandfather(18, "祖父"),
    grandmother(19, "祖母"),
    maternalGrandfather(20, "外祖父"),
    maternalGrandmother(21, "外祖母"),;
    private int sequence;
    private String cName;
    KinshipEnum(int sequence, String cName) {
        this.cName = cName;
        this.sequence = sequence;
    }

    public int getSequence() {
        return sequence;
    }

    public String getcName() {
        return cName;
    }

    public static int toValue(String cName) {
        for (KinshipEnum kinshipEnum : KinshipEnum.values()) {
            if (kinshipEnum.getcName().equals(cName)) {
                return kinshipEnum.getSequence();
            }
        }
        return 99;
    }
}