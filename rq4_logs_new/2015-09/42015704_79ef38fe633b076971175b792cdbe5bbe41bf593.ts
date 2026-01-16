///<reference path="../globals.ts" />

/* ------------
 CPU.ts

 Requires global.ts.

 Routines for the host CPU simulation, NOT for the OS itself.
 In this manner, it's A LITTLE BIT like a hypervisor,
 in that the Document environment inside a browser is the "bare metal" (so to speak) for which we write code
 that hosts our client OS. But that analogy only goes so far, and the lines are blurred, because we are using
 TypeScript/JavaScript in both the host and client environments.

 This code references page numbers in the text book:
 Operating System Concepts 8th edition by Silberschatz, Galvin, and Gagne.  ISBN 978-0-470-12872-5
 ------------ */

module TSOS {

    export class Memory {

        //public static _MemMax = 256;

        constructor(public programMemory  = new Array(256) ) {
            this.programMemory = new Array();
            Control.updateHostStatus("Constructor");
            this.zeroMemory();
        }

        public zeroMemory(): void {
            for( var i = 0; i < 256; i++)
            {
                this.programMemory[i] = 0;
                Control.updateHostStatus(i.toString());
            }
        }

        public setAddress( address : number , value : number) : boolean
        {
            if( address > 255 || address < 0)
                return false;

            if( value > 255 || value < 0)
                return false;

            this.programMemory[address] = value;

            TSOS.Control.updateMemoryDisplay();

            return true;
        }



        public setAddressHexStr( address : number , valueHex : string) : boolean
        {
            var value : number = parseInt(valueHex,16);

            if( address > 255 || address < 0)
                return false;

            if( value > 255 || value < 0)
                return false;

            this.programMemory[address] = value;

            return true;
        }

        public getAddress( address : number ) : number
        {
            var ret : number = -1;

            if( address >= 0 && address < 256 )
                ret = this.programMemory[address];

            return ret;
        }

        public getAddressHexStr( address : number ) : string
        {
            var ret : string = "";

            if( address >= 0 && address < 256 )
                ret = Utils.padString(this.programMemory[address].toString(16),2);

            return ret;
        }

        public getDWordBigEndian( address : number ) : number
        {
            var dword = -1;

            if( address + 1 < 256 && address > 0)
            {
                dword = parseInt(this.programMemory[address + 1].toString(16) + this.programMemory[address].toString(16),16);
            }

            return dword;
        }

        public loadMemory( source : string)
        {
            var s = new RegExp("[ ]+");
            var data = source.toUpperCase().split(s);
            
            for( var i = 0; (i < data.length) && (i < 256); i++)
            {
                this.programMemory[i] = parseInt(data[i],16);
            }
        }
    }
}