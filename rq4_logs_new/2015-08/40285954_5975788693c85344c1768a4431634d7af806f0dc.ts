declare module "Mareframe.DST" {
    export class Model {

    }
}

module Mareframe.DST {
    export class Model {
        private elementArr: Mareframe.DST.Element[] = [];
        private connectionArr: Mareframe.DST.Connection[] = [];
        private modelName: string = "untitled";
        private modelPath: string = "./";
        private modelChanged: boolean = true;
        private dataMatrix: any[][] = [];
        private mainObjective: Mareframe.DST.Element;
        //constructor() { };

        setMainObj(goalElmt: Mareframe.DST.Element): void {
            this.mainObjective = goalElmt;
        }
        getMainObj(): Mareframe.DST.Element {
            return this.mainObjective;
        }
        getDataMatrix(): any {
            return this.dataMatrix;
        }
        setDataMatrix(Matrix: any[][]): void {
            this.dataMatrix = Matrix;
        }
        getWeights(elmt: Mareframe.DST.Element): number[][]{
            var weightsArr: number[][] = [];
            if (elmt.getType() != 0) {
                var total: number = 0.0;
                elmt.getData()[1].forEach(function (val: number) { total += val; });
                for (var i = 0; i < elmt.getData()[0].length; i++) {
                    var childWeights:number[][] = this.getWeights(this.getConnection(elmt.getData()[0][i]).getInput());
                    for (var j = 0; j < childWeights.length; j++) {
                        childWeights[j][1] *= (elmt.getData()[1][i] / total);
                    }
                    weightsArr = weightsArr.concat(childWeights);
                }
            } else {
                weightsArr.push([elmt.getData()[0], 1]);
            }
            return weightsArr;
        }
        getFinalScore(): number[][]{
            var tempMatrix = JSON.parse(JSON.stringify(this.dataMatrix));
            var weightsArr = this.getWeights(this.mainObjective);
    	

            //console.log(tempMatrix);
            for (var i = 0; i < weightsArr.length; i++) {
                var elmtData = this.getElement(this.dataMatrix[0][i + 1]).getData();

                //set minimum and maximum values
                var maxVal = elmtData[5];
                var minVal = elmtData[4];

                //check if data is within min-max values, and expand as necessary
                for (var j = 1; j < tempMatrix.length - 1; j++) {
                    if (tempMatrix[j][i + 1] > maxVal) {
                        maxVal = tempMatrix[j][i + 1];
                    }
                }

                for (var j = 1; j < tempMatrix.length - 1; j++) {
                    if (tempMatrix[j][i + 1] < minVal) {
                        minVal = tempMatrix[j][i + 1];
                    }
                }

                //var currentMax = 0;
                //for (var j = 1; j < tempMatrix.length; j++) {
                //    if (tempMatrix[j][i + 1] > currentMax) {
                //        currentMax = tempMatrix[j][i + 1];
                //    }
                //}
            
                for (var j = 1; j < tempMatrix.length - 1; j++) {


                    tempMatrix[j][i + 1] = Mareframe.DST.Math.getValueFn(Math.abs(elmtData[3] - ((tempMatrix[j][i + 1] - minVal) / (maxVal - minVal))), Math.abs(elmtData[3] - ((elmtData[1] / 100))), 1 - (elmtData[2] / 100));
                    //console.log(getValueFn(tempMatrix[j][i + 1] / currentMax, elmtData[1]/100, elmtData[2]/100));
                    //console.log(tempMatrix[j][i + 1] / currentMax);
                    tempMatrix[j][i + 1] *= weightsArr[i][1];
                    tempMatrix[j][i + 1] = (Math.round(1000 * tempMatrix[j][i + 1])) / 1000;
                }

            }
            for (var i = 1; i < tempMatrix.length - 1; i++) {
                tempMatrix[i][0] = this.getElement(tempMatrix[i][0]).getName();
            }


            return tempMatrix;
        }

        getWeightedData(elmt: Mareframe.DST.Element, addHeader: boolean): any[][] {
            var tempMatrix = [];
            if (addHeader) {
                tempMatrix.push(['string', 'number']);
            }
            switch (elmt.getType()) {
                case 2: //scenario
                    for (var i = 1; i < this.dataMatrix[0].length; i++) {
                        tempMatrix.push([this.dataMatrix[0][i], this.dataMatrix[elmt.getData()[0]][i]]);
                    }
                    break;
                case 0: //attribute
                    //set minimum and maximum values
                    var maxVal = elmt.getData()[5];
                    var minVal = elmt.getData()[4];

                    //check if data is within min-max values, and expand as necessary
                    for (var i = 1; i < this.dataMatrix.length - 1; i++) {
                        if (this.dataMatrix[i][elmt.getData()[0]] > maxVal) {
                            maxVal = this.dataMatrix[i][elmt.getData()[0]];
                        }
                    }

                    for (var i = 1; i < this.dataMatrix.length - 1; i++) {
                        if (this.dataMatrix[i][elmt.getData()[0]] < minVal) {
                            minVal = this.dataMatrix[i][elmt.getData()[0]];
                        }
                    }


                    //calculate weights according to valueFn
                    for (var i = 1; i < this.dataMatrix.length - 1; i++) {

                        var toAdd = [this.getElement(this.dataMatrix[i][0]).getName(), this.dataMatrix[i][elmt.getData()[0]]];
                        if (!addHeader) {
                            toAdd.push(Mareframe.DST.Math.getValueFn(Math.abs(elmt.getData()[3] - ((this.dataMatrix[i][elmt.getData()[0]] - minVal) / (maxVal - minVal))), Math.abs(elmt.getData()[3] - ((elmt.getData()[1] / 100))), 1 - (elmt.getData()[2] / 100)));
                        }
                        //console.log(elmt.getData()[1]);
                        tempMatrix.push(toAdd);
                    }
                    break;
                case 1: //sub-objective
                    var total = 0.0;
                    elmt.getData()[1].forEach(function (val) { total += val; });
                    //console.log(total + " : " + elmt.getName());
                    for (var i = 0; i < elmt.getData()[0].length; i++) {
                        //console.log(elmt.getData());
                        var tempEl = this.getConnection(elmt.getData()[0][i]).getInput();

                        var tempArr = this.getWeightedData(tempEl, false);
                        //console.log(tempArr);


                        var result = 0;
                        for (var j = 0; j < tempArr.length; j++) {

                            result += tempArr[j][1];

                        }
                        //console.log(result + " " + elmt.getName()+"; "+tempArr+" "+tempEl.getName());
                        tempMatrix.push([tempEl.getName(), result * (elmt.getData()[1][i] / total)]);
                    }
                    break;
            }
            return tempMatrix;
        }

        createNewElement(): Mareframe.DST.Element {
            var e = new MareFrame.DST.Element();
            this.elementArr.push(e);
            return e;

        }
        getElement(id: string): Mareframe.DST.Element {
            return this.elementArr[this.getObjectIndex(id)];
        }
        private getObjectIndex(id: string): number {
            var key = 0;
            if (id.substr(0, 4) == "elmt") {
                this.elementArr.every(function (elm) {
                    if (elm.getID() === id)
                        return false;
                    else {
                        key = key + 1;
                        return true;
                    }
                });
            } else if (id.substr(0, 4) == "conn") {
                this.connectionArr.every(function (conn) {
                    if (conn.getID() === id)
                        return false;
                    else {
                        key = key + 1;
                        return true;
                    }
                });
            } else {
                throw DOMException.NOT_FOUND_ERR;
            }
            return key;
        }
        getConnectionArr(): Mareframe.DST.Connection {
            return this.connectionArr;
        }
        getConnection(id: string): Mareframe.DST.Connection {
            return this.connectionArr[this.getObjectIndex(id)];
        }
        getElementArr(): Mareframe.DST.Element {
            return this.elementArr;
        }
        deleteElement(id): void {
        }
        setName(name: string): void{
            this.modelName = name;
        }
        getName(): string {
            return this.modelName;
        }
        addConnection(connection: Mareframe.DST.Connection): boolean {
            var validConn = true;
            this.connectionArr.forEach(function (conn) {

                if (conn === connection)
                { validConn = false; }
                else if ((connection.getOutput().getID() === conn.getOutput().getID() && connection.getInput().getID() === conn.getInput().getID()) || (connection.getOutput().getID() === conn.getInput().getID() && connection.getInput().getID() === conn.getOutput().getID())) {
                    validConn = false;
                }
            });
            if (validConn) {
                this.connectionArr.push(connection);

                connection.getInput().addConnection(connection);
                connection.getOutput().addConnection(connection);
                return true;
            } else {
                return false;
            }
        }
        toJSON(): any {
            return { elements: this.elementArr, connections: this.connectionArr, mdlName: this.modelName, mainObj: this.mainObjective, dataMat: this.dataMatrix };

        }
        fromJSON(jsonElmt: any): void {

            $("#modelHeader").html(jsonElmt.mdlName);
            $("#model_header").append(jsonElmt.mdlName);
            this.modelName = jsonElmt.mdlName;




            var maxX = 0;
            var maxY = 0;

            jsonElmt.elements.forEach(function (elmt) {
                var e = h.gui.addElementToStage();
                e.fromJSON(elmt);
                h.gui.updateElement(e);
                if (elmt.posX > maxX)
                    maxX = elmt.posX;

                if (elmt.posY > maxY)
                    maxY = elmt.posY;

            });

            jsonElmt.connections.forEach(function (conn) {
                var inpt = h.getActiveModel().getElement(conn.connInput);
                var c = new MareFrame.DST.Connection(inpt, this.getElement(conn.connOutput));
                c.fromJSON(conn);
                if (this.addConnection(c)) {
                    h.gui.addConnectionToStage(c);
                }
            });
            this.mainObjective = this.getElement(jsonElmt.mainObj);

            h.gui.setSize(maxX + 80, maxY + 20);

            this.dataMatrix = jsonElmt.dataMat;
            h.gui.updateTable(this.dataMatrix);
        }




    }


}