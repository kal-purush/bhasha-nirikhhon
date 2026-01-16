/**
 * Responsible for
 * * Encoding and decoding Steps
 * 
 * @class Encoder
 */
class Encoder {

    /**
     * Encodes the Steps provided for exchange between front and back-end
     * 
     * @static
     * @param {Step[]} steps The Steps to be encoded
     * @returns {string} A JSON-string containing the Steps encoded in Format
     * 
     * @memberOf Encoder
     */
    static EncodeSteps(steps: Step[]): string {
        var readySteps: Step[] = new Array();
        var steps: Step[] = new Array().concat(steps);
        steps.forEach(step => {
            //Add information about Form Class
            var formclass = Encoder.getFormClass(step);
            step['FormClass'] = formclass;
            //Add the ready Step to be encoded
            readySteps.push(step);
        });

        //Encode and return the Steps
        return JSON.stringify(readySteps);
    }


    /**
     * Decodes the JSON-string into Step objects
     * 
     * @static
     * @param {string} json
     * @returns {Step[]}
     * 
     * @memberOf Encoder
     */
    static DecodeSteps(json: string): Step[] {
        var objs: Object[] = JSON.parse(json);
        var steps = [];

        objs.forEach(obj => {
            var type = obj['FormClass'];

            //Decode the Form
            var outform;
            var objform = obj['form'];
            switch (type) {
                case "Select":
                    var options: FormOption[] = [];
                    //Decode FormOptions
                    objform.options.forEach(option => {
                        options.push(new FormOption(option.text, option.value));
                    });

                    outform = new Select(objform.title, objform.text, options);
                    break;

                case "Information":
                    outform = new Information(objform.title, objform.text);
                    break;

                case "Checkbox":
                    outform = new Checkbox(objform.title, objform.text, objform.checked);
                    break;

                case "FormRange":
                    outform = new FormRange(objform.title, objform.text, objform.min, objform.max, objform.step, objform.defaultValue);
                    break;
                    
            }

            //Add the finished Step to the array
            steps.push(new Step(obj['id'], outform));
        });

        return steps;
    }

    private static getFormClass(step: Step) {
        return step.form.constructor.name;
    }


}