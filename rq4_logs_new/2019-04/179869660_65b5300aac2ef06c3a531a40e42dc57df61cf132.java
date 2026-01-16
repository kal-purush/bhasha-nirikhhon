package com.example.zombiegame;

public class FillInNode {

    Node start;

    public FillInNode(){
        this.start = NodeStart();
        fillNode1();
    }

    public void fillNode1(){
        Node c2 = Choice2();
        Node c1 = Choice1();
        start.setChoice1(c1);
        start.setChoice2(c2);

        Node c3 = Choice3();
        Node c4 = Choice4();
        c2.setChoice1(c3);
        c2.setChoice2(c4);

        c3.setChoice1(start);
        c4.setChoice1(start);

        Node c5 = Choice5();
        Node c6 = Choice6();

        c1.setChoice1(c5);
        c1.setChoice2(c6);

        Node c7 = Choice7();
        Node c8 = Choice8();

        c5.setChoice1(c7);
        c5.setChoice2(c8);

        c8.setChoice1(start);

        Node c9 = Choice9();
        Node c10 = Choice10();

        c7.setChoice1(c9);
        c7.setChoice2(c10);

        Node c11 = Choice11();
        Node c12 = Choice12();

        c10.setChoice1(c11);
        c10.setChoice2(c12);

        Node c13 = Choice13();

        c12.setChoice1(c13);
        c12.setChoice2(c13);

        Node c15 = Choice15();
        Node c16 = Choice16();

        c13.setChoice1(c15);
        c13.setChoice2(c16);

        c16.setChoice1(start);

        Node c17 = Choice17();
        Node c18 = Choice18();
        Node c19 = Choice19();

        c15.setChoice1(c17);
        c15.setChoice2(c18);
        c15.setChoice3(c19);

        c17.setChoice1(start);
        c19.setChoice3(start);

        Node c20 = Choice20();
        Node c21 = Choice21();

        c18.setChoice1(c20);
        c18.setChoice2(c21);

        c20.setChoice1(start);
        c21.setChoice2(start);

    }

    public Node getStart(){
        return this.start;
    }

    public Node NodeStart(){
        Node node = new Node(
                "Quietly check the perimeter to escape",
                "Look for a weapon to fight",
                null,
                "You wake up in the middle of the night to zombies banging on your window"
        );
        return node;
    }

    public Node Choice1(){
        Node node = new Node(
                "Head to nearby friend’s house",
                "Head to library (bigger, safer building)",
                null,
                "Discover no zombies at the back of the house, escape (no weapon) "
        );
        return node;
    }

    public Node Choice2(){
        Node node = new Node(
                "Head to the convenience store (treat wound with a first aid kit)",
                "Head to the library (bigger, safer building)",
                null,
                "Find a baseball bat, break window, kill one zombie, the other bites you but you fight it off and escape"
        );
        return node;
    }

    public Node Choice3(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Encounter hunters looting the store, they see your bitten arm and one of them shoots you. You died... Tragic"
        );
        return node;
    }

    public Node Choice4(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Lock yourself in the records room with a set of cabinets, sleep there for the night, wake up craving brains. You died... Tragic"
        );
        return node;
    }

    public Node Choice5(){
        Node node = new Node(
                "Run",
                "Sneak past",
                null,
                "Encounter zombies on the way"
        );
        return node;
    }

    public Node Choice6(){
        Node node = new Node(
                "Block door with cabinets",
                "Block door with table",
                null,
                "You get to the library without incident and decide to sleep in the record’s room"
        );
        return node;
    }

    public Node Choice7(){
        Node node = new Node(
                "Head to new building",
                "Figure out how to get to friend’s house",
                null,
                "You’re too fast for the zombies, but you get lost while running and see a new building"
        );
        return node;
    }

    public Node Choice8(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "You trip on a tree root and the zombies hear you and attack while you try to get up, you get eaten. You died... Tragic"
        );
        return node;
    }

    public Node Choice9(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Encounter other survivors, a girl your age and 2 younger boys, they're suspicious of you, you try to sleep on another floor of the building and they kill you in your sleep. You died... Tragic"
        );
        return node;
    }

    public Node Choice10(){
        Node node = new Node(
                "Complain about hunger",
                "Take friend’s advice (go to the library without breakfast)",
                null,
                "navigate back to friend’s house, going further up the path to avoid zombies, greet your friend and spend the night, when you wake up they ask if you want to go to the library (bigger, safer building). You died... Tragic"
        );
        return node;
    }

    public Node Choice11(){
        Node node = new Node(
                "Try to sneak past",
                "Try to run past",
                "Tell your friend to stay back while you fight with the hammer",
                "On the way to the library you see 4 zombies directly in your path, they haven’t noticed you and your friend yet"
        );
        return node;
    }

    public Node Choice12(){
        Node node = new Node(
                "Cereal",
                "Toast",
                null,
                "They offer breakfast "
        );
        return node;
    }

    public Node Choice13(){
        Node node = new Node(
                "Keep weapon",
                "Give weapon ",
                null,
                "While you both eat, they talk about the library more, mention safety at the church across town, you both head to the library (you see some Zs on the way but they’re ahead of you and to the side, they don’t notice you and you pass by safely), at the library your friend finds a survival book and flips through it, the book says to find weapons, you find a hammer in a set of drawers"
        );
        return node;
    }

    public Node Choice15(){
        Node node = new Node(
                "Stay together and check the perimeter",
                "Go in blind",
                "split up and check the perimeter",
                "Encounter a Zombie as you leave the library (it catches you off guard), you beat it in the head with the hammer and run, it dies and your friend takes the bloody hammer. They catch up with you (keeping the hammer?) and you both head to the safe point (fade out). You both get to the school, night is falling"
        );
        return node;
    }

    public Node Choice16(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Encounter a zombie as you leave the library. Since you’re weaponless, you die, your friend survives and you see them run as you. You died... Tragic"
        );
        return node;
    }

    public Node Choice17(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "You see zombies in the cafeteria and decide not to go to the school and head straight to the safe zone, arrive at safe zone, meet others. YOU WIN!"
        );
        return node;
    }

    public Node Choice18(){
        Node node = new Node(
                "Go back (sneak around/find a new way in)",
                "Ignore",
                null,
                "Enter front door, go to the gym, friend goes to bathroom, you hear a scream and they run back yelling “Run!” you follow seeing a bloody horde of former teachers, you hear someone yell for."
        );
        return node;
    }

    public Node Choice19(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Check perimeter, both say all clear, go to cafeteria for food and get ambushed both die (friend checked cafeteria and missed it during perimeter checks). You died... Tragic"
        );
        return node;
    }

    public Node Choice20(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "Head to the cafeteria, see the stranger who called for help getting eaten, run away, get ambushed and eaten. You died... Tragic"
        );
        return node;
    }

    public Node Choice21(){
        Node node = new Node(
                "Restart",
                "Home",
                null,
                "As you leave the building you run into a horde of Zombies with no escape. You get eaten but your friend survives. You died... Tragic"
        );
        return node;
    }


}