const fs = require('fs');

class MouseHID{
    constructor(hidfile){
        this.hidfile = hidfile;
        this.currentX = 0;
        this.currentY = 0;
    }

    start(){
        return new Promise((resolve, reject) => {
            resolve()
        })
    }

    write(buttonCode, newPosition){
        return new Promise((resolve, reject) => {
            // Get x movement
            let xCode = newPosition.x - this.currentX;

            // Get y movement
            let yCode = newPosition.y - this.currentY;

            // Create array with the codes
            let codes = [buttonCode, xCode, yCode];

            // Write codes
            this.writeCodes(codes).then(() => {
                this.currentX = newPosition.x;
                this.currentY = newPosition.y;

                resolve();
            }).catch(reject)
        });
    }

    writeCodes(codes){
        return new Promise((resolve, reject) => {
            // Convert the code values into byte values
            let bytes = '';
            for (let code of codes){
                bytes += String.fromCharCode(code);
            }

            // Write bytes
            // console.log(codes, bytes.length);
            fs.writeFile(this.hidfile, bytes, 'binary', (err) => {
                if (err){
                    reject(err)
                }
                else{
                    resolve();
                }
            })
        })
    }
}

module.exports = MouseHID;