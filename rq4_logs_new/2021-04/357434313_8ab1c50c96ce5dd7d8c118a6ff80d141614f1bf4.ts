/*******************************************************************************
 * LCM1602-14 LCD Micro:Bit extension
 *
 * Company: Cytron Technologies Sdn Bhd
 * Website: http://www.cytron.io
 * Author: Arif Haikal
 * Email:   support@cytron.io
 *******************************************************************************/
/** 
* Board initialization and helper function. 
*/ 

//Instruction Set 
const CLEARDISPLAY = 0x01; 
const CURSORSHIFT = 0x10; 

//Display Entry Mode 
const ENTRYMODESET = 0x04; 
const ENTRYLEFT = 0x02; 
const ENTRYRIGHT = 0x00; 
const ENTRYSHIFTINCREAMENT = 0x01; 
const ENTRYSHIFTDECREMENT = 0x00; 

//Flags for display/cursor shift  
const DISPLAYMOVE = 0x08; 
const CURSORMOVE = 0X00; 
const MOVERIGHT = 0X04; 
const MOVELEFT = 0X00; 

//display control 
const DISPLAYCONTROL = 0X08; 
const DISPLAYON = 0X04; 
const DISPLAYOFF = 0X00; 
const CURSORON = 0X02; 
const CURSOROFF = 0X00; 
const BLINKON = 0X01; 
const BLINKOFF = 0X00; 
const SETCGRAMADDR = 0X40; 
const SETDDRAMADDR = 0X80;  

//for functionset 
const FUNCTIONSET = 0X20; 
const TENDOTS = 0X04; 
const EIGHTDOTS = 0X00; 
const ONELINE = 0X00; 
const TWOLINE = 0X08; 
const EIGHTBITMODE = 0X10; 
const FOURBITMODE = 0X00;  

/** 
* LCD_i2c blocks 
*/ 
//% weight=100 color=#326fa8 icon="\uf108" 
namespace LCD_i2c  
{ 
    let addrs: number 

    //% blockId="I2C_LCM1602_LCD_INITIALIZE" block="LCD Initialize with Address %Addr" 
    //% weight=100 blockGap=8 
    //% parts=LCM1602_I2C trackArgs=0 

    export function LcdBegin(Addr: number)  
    { 
        addrs = Addr;  
        basic.pause(50); 
        command(FUNCTIONSET | TWOLINE); 
        basic.pause(10); 
        display(); 
        basic.pause(0.04); 
        clear(); 
        basic.pause(20); 
        command(ENTRYMODESET | ENTRYLEFT | ENTRYSHIFTDECREMENT); 
        setCursor(0,0); 
    } 

    export function display() : void  
    { 
        command(DISPLAYCONTROL | DISPLAYON | CURSOROFF | BLINKOFF); 
    } 

    export function noDisplay() : void  
    { 
        command(DISPLAYCONTROL | DISPLAYOFF | CURSOROFF | BLINKOFF); 
    } 

    export function clear() : void 
    { 
        command(CLEARDISPLAY); 
        basic.pause(2); 
        setCursor(0,0); 
    } 

    // Set cursor 
    //% blockId="I2C_LCM1602_SET_CURSOR" block="Enter Row at %line and Column at %column" 
    //% weight=90 blockGap=8 
    //% line.min=0 line.max=2 
    //% column.min=0 column.max=16 
    //% parts=LCD1602_I2C trackArgs=0 

    export function setCursor(line: number, column: number)  
    { 
        const offsets = [0x00, 0x40, 0x14, 0x54]; 
        command(SETDDRAMADDR | (offsets[line] + column)); 
    } 

    // Print any text 
    //% blockId="I2C_LCM1602_PRINT_TEXT" block="Write Your Text Here %s" 
    //% weight=90 blockGap=8 
    //% parts=LCD1602_I2C trackArgs=0 
    export function printText(s: string): void 
    { 
        for(let i = 0; i < s.length; i++) 
        { 
            write(s.charCodeAt(i)) 
        } 
    } 

    //% blockId="I2C_LCM1602_SCROLL_RIGHT" block="LCD Display Scroll Right" 
    //% weight=100 blockGap=8 
    //% parts=LCM1602_I2C trackArgs=0 
    export function scrollDisplayRight() : void  
    { 
        command(CURSORSHIFT | DISPLAYMOVE | MOVERIGHT); 
    } 

    //% blockId="I2C_LCM1602_SCROLL_LEFT" block="LCD Display Scroll Left" 
    //% weight=100 blockGap=8 
    //% parts=LCM1602_I2C trackArgs=0 
    export function scrollDisplayLeft() : void  
    { 
        command(CURSORSHIFT | DISPLAYMOVE | MOVELEFT); 
    } 

    function command(value: number): void 
    { 
        let buffer = pins.createBuffer(2); 
        buffer[0] = 0x80; 
        buffer[1] = value; 
        pins.i2cWriteBuffer(0x3E, buffer); 
        basic.pause(1); 
    } 

    function write(value: number): void 
    { 
        let buffer = pins.createBuffer(2); 
        buffer[0] = 0x40; 
        buffer[1] = value; 
        pins.i2cWriteBuffer(0x3E, buffer); 
        basic.pause(1); 
    } 

} 

 
 

 