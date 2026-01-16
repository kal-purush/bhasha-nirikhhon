//
// Define classes used by tsBot et. al.
//
class command {

    EventCode: string;    // API_CHAT, etc.
    CommandCode: string;  // !dothis
    UserFunction: string; // function ptr by name

    constructor(ec: string, cc: string, uf: string) {
        this.EventCode = ec;
        this.CommandCode = cc;
        this.UserFunction = uf;
    }

};

class commandTable {

    // allocate Table as an empty array of the command class
    Table: command[] = new Array;

    // Build the CommandTable when it is first allocated
    constructor() {
        // thus, command !xyz is at the end of the table
        //this.Table.push(new command(API_CHAT, "!xyz", "xyz_command"));



        // and command !abc is at the front...
        //this.Table.push(new command(API_CHAT, "!abc", "abc_command"));

    }

}