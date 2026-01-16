import API from "../../../services/API";

export default function getPecaPorTipo(){

   const tipoPeca = ["ENCONSTO", "BRACO", "ACENTO", "MECANISMO", "BASE", "RODIZIO"];
   let arr = []

   tipoPeca.map(item => {
      API.get("peca", `tipo=${item}`).then(res => {
         if (res.dados.length !== 0) {
            arr.push(res.dados)
         }
      })
   })

   return arr
}