module Mareframe {
    export module DST {
        export class Connection {
            private inputElement: Element;
            private outputElement: Element;
            private m_id: string;
            public easelElmt: createjs.Container;

            constructor(p_inputElmt: Element, p_outputElmt: Element, p_connID) {
                this.inputElement = p_inputElmt;
                this.outputElement = p_outputElmt;
                this.m_id = p_connID;
            }

            getID(): string {
                return this.m_id;
            }
            setID(p_id: string): string {
                if (p_id.substr(0, 4) == "elmt")
                { this.m_id = p_id; }
                else { this.m_id = "elmt" + p_id; }
                return this.m_id;

            }

            getInputElement(): Element {
                return this.inputElement;
            }
            setInputElement(p_inputElmt): void {
                this.inputElement = p_inputElmt;
            }
            getOutputElement(): Element {
                return this.outputElement;
            }
            setOutputElement(p_outputElmt): void {
                this.outputElement = p_outputElmt;
            }
            
            flip(): void {
                var e = this.inputElement;
                this.inputElement = this.outputElement;
                this.outputElement = e;

                this.inputElement.deleteConnection(this.m_id);
                this.outputElement.addConnection(this);
            }

            toJSON(): any {
                return { connInput: this.inputElement.getID(), connOutput: this.outputElement.getID(), connID: this.m_id };
            }

            fromJSON(jsonElmt): void {
                this.m_id = jsonElmt.connID;
            }
        }
    }
}