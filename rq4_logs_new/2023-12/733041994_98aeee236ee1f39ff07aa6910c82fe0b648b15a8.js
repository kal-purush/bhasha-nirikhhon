import React,{useState} from "react";
import axios,{ formToJSON } from "axios";



function Formulario(){
const [nome,SetNome] = useState("")
const [email,SetEmail] = useState("")
const [senha, SetSenha] = useState("")
const [profissao,SetProfissao] = useState("")
const [aceitaTermos,SetaceitaTermos] = useState(false)

const handleCadastroFormulario = (event) => {
    event.preventDefault()
    
    try {
        const response = axios.post("http://localhost:3000/usuarios",{
         nome,
         email,
         senha,
         profissao,
         termos:aceitaTermos,       
        })
        console.log("Resposta da Api".formToJSON(response.data))
    } catch  {
        console.error("ERROR AO FAZER SOLICITAÇÃO")
    }
}

return (
    <div>
     <form onSubmit={handleCadastroFormulario}> 
         <div>
            <h1>
                FORMULARIO DE CADASTRO 
            </h1>
         </div>
         <div>
            <h4>
            Nome
            </h4>
            <input
            required 
            type="text"
            placeholder="Nome Completo"
            value={nome}
            onChange={(e) => SetNome (e.target.value) }
            />
            <h4>
            Email
            </h4>
            <input
            required
            type="text"
            placeholder="Insira seu Email"
            value={email}
            onChange={(e) => SetEmail (e.target.value) }
            />
            <h4>
            Senha
            </h4>
            <input 
            required
            type="password"
            placeholder="Insira sua senha"
            value={senha}
            onChange={(e) => SetSenha (e.target.value) }
            />
         </div>
         <div> 
         </div>
         <h1></h1>
         <div>
          <label htmlFor="profissao">Profissao:
          <select
           required
           id="profissao"
           name="profissao"
           value={profissao}
           onChange={(e)=>SetProfissao (e.target.value)}
           >
            <option value="" >Selecione a profissao </option>
            <option value="estudante" >Estudante</option>
            <option value="professor" >Professor</option>
            <option value="outros" >Outros</option>

           </select>
          </label>        
        </div>
        <div>
            <h1></h1>
            <input 
            required
            type ="checkbox"
            id ="aceitaTermos"
            checked={aceitaTermos}
            onChange={()=>SetaceitaTermos(!aceitaTermos)}/>
            <lapel>Eu aceito os termos e condições de serviço</lapel>
        </div>
        <button submit="handleCadastroFormulario">Enviar</button>
     </form>
    </div>
  )
}

export default Formulario;