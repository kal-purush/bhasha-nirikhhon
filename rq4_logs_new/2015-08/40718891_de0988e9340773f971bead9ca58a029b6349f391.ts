module Mareframe {
    export module DST {
        export class Connection {
            private m_inputElement: Element;
            private m_outputElement: Element;
            private m_id: string;
            public m_easelElmt: createjs.Container = new createjs.Container();
            private m_color = "black";



            constructor(p_inputElmt: Element, p_outputElmt: Element, p_bbnMode?: boolean, p_connID?: string) {

                this.m_inputElement = p_inputElmt;
                this.m_outputElement = p_outputElmt;
                if (p_connID) {
                    this.m_id = p_connID;
                }

                if (p_bbnMode&&p_inputElmt.getType() === 1 && p_outputElmt.getType() === 1) {
                    this.m_color = "gray";
                }
            }


            getColor() {
                return this.m_color
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
                return this.m_inputElement;
            }
            setInputElement(p_inputElmt): void {
                this.m_inputElement = p_inputElmt;
            }
            getOutputElement(): Element {
                return this.m_outputElement;
            }
            setOutputElement(p_outputElmt): void {
                this.m_outputElement = p_outputElmt;
            }
            
            flip(): void {
                var e = this.m_inputElement;
                this.m_inputElement = this.m_outputElement;
                this.m_outputElement = e;

                this.m_inputElement.deleteConnection(this.m_id);
                this.m_outputElement.addConnection(this);
            }

            toJSON(): any {
                return { connInput: this.m_inputElement.getID(), connOutput: this.m_outputElement.getID(), connID: this.m_id };
            }

            fromJSON(p_jsonElmt): void {
                this.m_id = p_jsonElmt.connID;
            }
        }
    }
}