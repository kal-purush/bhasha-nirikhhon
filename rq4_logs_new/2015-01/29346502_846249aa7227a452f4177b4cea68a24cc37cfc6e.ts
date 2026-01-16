/**
 * Created by shpandrak on 11/14/14.
 *
 */

/***
 * ShpanText Plugin interface for implementing external plugins
 */
interface ShpanTextPlugin{
    (shpanText: ShpanText, params:string[]);
}

interface ShpanTextCallback{
    ();
}



class ShpanText{
    private htmlElement : HTMLElement;
    private delay : number = 40;
    private text:string = null;
    private pos : number = 0;
    private printDoneCallback:ShpanTextCallback = null;

    // Active timer handle
    private timerHandle:number = 0;

    // Initializing default plugins
    plugins: { [s: string]: ShpanTextPlugin; } = {

        // Wait - pauses for x millis
        'w': function (shpanText: ShpanText, params: string[]) {

            var charPause:number = parseInt(params[1]);
            shpanText.setTextAnimationTimeout(charPause, function () {
                shpanText.nextCallback();
            });
        },



        /***
         * Delete       - deletes x characters
         * Parameters   1: [mandatory] number of characters to delete
         *              2: [optional] milliseconds to wait before deleting, default is 0
         */
        'd': function (shpanText: ShpanText, params: string[]) {

            var charactersToRemove:number = parseInt(params[1]);

            // Checking if timeToWait param supplied
            if (params.length > 2){
                var timeToWait:number = parseInt(params[2]);
                shpanText.setTextAnimationTimeout(timeToWait, ()=>{
                    shpanText.deleteCharactersAndContinue(charactersToRemove);
                });

            }else{
                shpanText.deleteCharactersAndContinue(charactersToRemove);
            }
        },


        /***
         * Delete-All   Deletes all already typed characters
         * Parameters   1: [optional] milliseconds to wait before deleting, default is 0
         */
        'da': function (shpanText: ShpanText, params: string[]) {

            // Checking if timeToWait param supplied
            if (params.length > 1) {
                var timeToWait:number = parseInt(params[1]);
                shpanText.setTextAnimationTimeout(timeToWait, ()=> {

                    // Using pos as number of character since its the minimum
                    // When output text is empty deletion will finish anyway...
                    shpanText.deleteCharactersAndContinue(shpanText.pos);
                });
            }else{

                // Using pos as number of character since its the minimum
                // When output text is empty deletion will finish anyway...
                shpanText.deleteCharactersAndContinue(shpanText.pos);
            }
        }

    };

    constructor(htmlElement:HTMLElement){
        this.htmlElement = htmlElement;
    }

    addExternalPlugin(pluginId:string, externalPlugin:ShpanTextPlugin){
        this.plugins[pluginId] = externalPlugin;
    }

    printShpanText(text:string, callback:ShpanTextCallback = null) {
        this.clearTimerHandle();
        this.printDoneCallback = null;
        this.text = text;
        this.pos = 0;
        this.htmlElement.innerHTML = '';
        if (callback != null){
            this.printDoneCallback = callback;
        }
        this.nextCallback();
    }


    setTextAnimationTimeout(delay:number, handler:any){
        this.clearTimerHandle();
        this.timerHandle = window.setTimeout(handler, delay);
    }

    private clearTimerHandle() {
        if (this.timerHandle != 0) {
            window.clearTimeout(this.timerHandle);
            this.timerHandle = 0;
        }
    }

    private prvPrint(){
        // Checking if done
        if (this.pos >= this.text.length) {
            // We are done cleaning timer if somehow set
            this.clearTimerHandle();

            // Calling "done" callback if set
            if (this.printDoneCallback != null){
                this.printDoneCallback();
            }

        }else{
            var subStr = this.text.substr(this.pos);
            if (subStr.charAt(0) === '$' &&
                subStr.charAt(1) === '[') {

                var paramsMatch:RegExpExecArray = /\$\[(?:\w+)(?:,\w+)*\]/.exec(subStr);
                if (paramsMatch != null){
                    var paramsStr:string = subStr.substr(2,paramsMatch[0].length-3);
                    var paramsArr:string[] = paramsStr.split(',');
                    var pluginKey:string = paramsArr[0];
                    var plugin:ShpanTextPlugin = this.plugins[pluginKey];
                    this.pos += paramsStr.length + 3;
                    if (plugin != null) {
                        //todo:else-warn
                        plugin(this, paramsArr);
                    }

                }

                // Finish execution, plugins will lead the next step
                return;
            }


            var nextChar:string = this.text.charAt(this.pos);
            this.htmlElement.textContent += nextChar;
            this.pos +=1;
            this.nextCallback();
        }
    }

    nextCallback() {
        this.setTextAnimationTimeout(this.delay, () =>{
            this.prvPrint()
        });
    }

    deleteCharactersAndContinue(numberOfCharactersToDelete:number) {
        this.setTextAnimationTimeout(this.delay, () => {
            this.prvDelete(numberOfCharactersToDelete);
        });
    }

    private prvDelete(numberOfCharactersToDelete:number){
        if (numberOfCharactersToDelete > 0 && this.htmlElement.textContent.length > 0){
            this.htmlElement.textContent = this.htmlElement.textContent.substr(0, this.htmlElement.textContent.length - 1);
            this.deleteCharactersAndContinue(numberOfCharactersToDelete - 1);
        }else if (this.text != null){
            this.nextCallback();
        }
    }
}