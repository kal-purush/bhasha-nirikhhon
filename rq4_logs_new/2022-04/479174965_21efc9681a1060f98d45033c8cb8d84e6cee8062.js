

//OUTRO OBJETO==============
//Pega os valores do exercicio custom
let CustomValueGetter = {

    //Atributes -----

    inspValue: 0,
    PauseValue: 0, 
    expV: 0,
    ExpValue: 0,

    
    //Getter and Setter ----

    setInspValue(){
        this.InspValue = document.querySelector("#InspValue")
    },
    getInspValue(){    
        return this.inspValue
    },


    setPauseValue(){
        this.PauseValue = document.querySelector("#PauseValue")
    },
    getPauseValue(){
        return this.PauseValue
    },


    setExpValue(){
        this.ExpValue = document.querySelector("#ExpValue")
    },
    getExpValue(){
        return this.ExpValue
    }


}
//OUTRO OBJETO =============
//Este cria os arrays dos segundos 
//e pega os valores dos atributos
let SecondMaker = {

    //Atributes ==========

    Inspiracao:[0],
    Pause:[0],
    Expiracao:[0],


    //Methods ============

    Contador(Value) {
        //Cria um array do número selecionado até 0
        //Cria um array de 0 até o número selecionado
        let ValueList = []      
        ValueList[Value] = Value
        //let Count = ValueList[0]    
        for (let i = Value; i = 0; i--){
            ValueList[i] = i  
            console.log(i)          
        }     
        console.log(Value)
        return ValueList   
    },


    //Getters and Setters  -------
    ValueGetterSetter(){       
        //Pega os valores do exercício selecionado,
        //joga na função que cria um array e
        //define o resultado como os valores do obj
        let Selected = Exercicios.Selector()
        let InspValue = Selected.Inspiracao
        let PauseValue = Selected.Pausa
        let ExpValue = Selected.Expiracao
        //Setter
        this.Inspiracao = this.Contador(InspValue)
        this.Pause = this.Contador(PauseValue)
        this.Expiracao = this.Contador(ExpValue)
        console.log(InspValue)
    },

    getInspiracao(){
        return this.Inspiracao
    },

    setPause(nContado){
        let nContadoV = nContado.getPauseValue()
        let PauseValueF = this.Contador(nContadoV)
        this.Pause = PauseValueF
    },
    getPause(){
        return this.Pause
    },

    setExpiracao(nContado){
        let nContadoV = nContado.getExpValue()
        let ExpValueF = this.Contador(nContadoV)
        this.Expiracao = ExpValueF
    },
    getExpiracao(){
       return this.Expiracao
    },


}





//OUTRO OBJETO ===============
//Este define os atributos para os exercicios
let Exercicios = {

    //Atributes ----
    Default: {
        ExercName:"Default",
        Inspiracao: 5,
        Pausa: 5,
        Expiracao: 20
    },

    Exercicio01: {
        ExercName:"Exercicio 01",
        Inspiracao: 10,
        Pausa: 10,
        Expiracao: 40
    },

    Custom: {
        ExercName:"Custom",
        Inspiracao: CustomValueGetter.InspValue,
        Pausa: CustomValueGetter.PauseValue,
        Expiracao: CustomValueGetter.ExpValue,
    },

    Selected:"",

    //METHODS ----
    Selector(){
        //Verifica e retorna os valores do exercicio selecionado
        //pelo usuario
       let Select = document.querySelector("#SelectExerc").value
       let SelectV 
       switch (Select) {

            case "Padrão":
                SelectV = this.Default
                this.Selected = SelectV.ExercName.value
                break;

            case "Custom":
                SelectV = this.Custom
                console.log(SelectV.ExercName)
                this.Selected = SelectV.ExercName.value
                this.customVisibility()             
                break;
           
            case "1":
                SelectV = this.Exercicio01
                this.Selected = SelectV.ExercName
                break;    

           default:
               SelectV = undefined
               this.Selected = "none"
               break;
       }
      
        return SelectV

    },

    SelectRepeat(){
        //Verifica e retorna os valores da repetição
        //do exercicio selecionado pelo usuário
        let Selected = this.Selector()
        let RepeatNumber
        switch (Selected) {
            case this.Default:
                RepeatNumber = 4
                break;

            case this.Custom:
                RepeatNumber = document.querySelector("#RepeatNumber").value

            default:
                
                break;
        }
        
        return RepeatNumber
    },

    //Adiciona as opções para customizaçao
    customVisibility(){
        let CustomDiv = document.querySelectorAll(".Custom").classList

        function customShow(Custom){
            Custom.remove("CustomHidden")
            console.log("oi")
        }

        function customHide(Custom){
            Custom.add("CustomHidden")
            console.log("ei")    
        }    
        if (this.Selected == "Custom"){
            customShow(CustomDiv)
        } else {
            customHide(CustomDiv)
        }
    },


    //Getters ---
    getInspValue(){
        return this.Selector().Inspiracao
    },
    getPauseValue(){
        return this.Selector().Pause
    },
    getExpValue(){
        return this.Selector().Expiracao
    }
}

//OUTRO OBJETO ===============
//Seleciona os textos de inspiracao, expiracao e pausa
let SecondsSelector = {
    Inspiracao: document.querySelector("#InspText"),
    Pause: document.querySelector("#PauseText"),
    Expiracao: document.querySelector("#ExpText"),
    
    //Methods ------
    //Troca o valor da inspiração, expiração e pause
    Changer(Value, Value2, Value3){
        this.Inspiracao.innerText = Value
        this.Pause.innerText = Value2
        this.Expiracao.innerText = Value3
        
    },
}

let Cutdown = {
    //Methods
    //Conta do valor do objeto passado até 0
    SecondSubtract(object){
        let id = setInterval(Subtract, 1000, object)
        function Subtract(object){
            object.innerText = String(Number(object.innerHTML)-1)
            if(Number(object.innerHTML) <= 0){
                    clearInterval(id)
                    listener.function = 1
            }
        }
    }

}

//OUTRO OBJETO========================

//Verifica se os contadores terminaram
let listener = {
    status: "",
    function:1,
    presentObj:SecondsSelector.Inspiracao,
    FEnd(){
        let listener = setInterval(checker, 1000)
        function checker(){
            console.log("oi")
            let PresentFunction = this.function
            let aObj = this.presentObj
            switch (PresentFunction) {
                case 1:
                    Cutdown.SecondSubtract(aObj)
                    break;
            
                default:
                    break;
            }
        }
        checker()
        //limpa o setinterval ao clickar em "pare"
        if(this.status == "close"){
            clearInterval(listener)
        }
    }
}
//Carrega as funções necessárias para o funcionamento do programa
LoadFunction = function(){
    Exercicios.Selector()
    //Trocar os parametros por nomes menores.
    SecondsSelector.Changer(Exercicios.Default.Inspiracao, Exercicios.Default.Pausa, Exercicios.Default.Expiracao)
    listener.FEnd()

}
   
LoadFunction()





