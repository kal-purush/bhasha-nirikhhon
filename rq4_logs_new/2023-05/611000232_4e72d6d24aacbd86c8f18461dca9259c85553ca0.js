function verificar()
        {
            var enviar = true ;


            var e = document.form1.email.value ;
            if( e.indexOf("@") < 0  )
            {
                enviar = false ;
                document.querySelector("#erroEmail").textContent = "Digite o email" ;
            }
            var n = document.form1.senha.value ;
            if( n.length == 0 )
            {
                enviar = false ;
                document.querySelector("#erroSenha").textContent = "Digite uma senha" ;
            }

            if(enviar)
            {
                document.form1.submit();
            }
        }