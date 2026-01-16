import './App.css';

function App() {
  return (
    <div className="App">
      <div className='i_form'>
            <div className='i_title'>Smart Frut</div>

            <div className='i_icon'>
                <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@48,400,0,0" />
                <span className='material-symbols-outlined'>point_of_sale</span>
            </div>

            <div className='i_text'>
                Digite qual fruta você deseja comprar:
            </div>

            <input type='text' id='produto' className='i_input' placeholder='Ex: morango' required />
            <button type='submit' className='btt_comprar' onClick={venda}>Comprar</button>
        </div>
    </div>
  );

  function venda()
{
    var produ = document.querySelector('#produto');
    var prod = produ.value;

    // alert(`${prod}`);

    if(prod == "laranja" || prod == "Laranja" || prod == "LARANJA")
    {
        alert(`Você comprou ${prod}. Custa R$ 2,99/Kg.`);
    }

    else if (prod == "morango" || prod == "Morango")
    {
        alert(`Você comprou ${prod}. Custa R$ 5,99/Kg.`);
    }

    else if (prod == "banana" || prod == "Banana")
    {
        alert(`Você comprou ${prod}. Custa R$ 2,99/Kg.`);
    }

    else if (prod == "pêssego" || prod == "Pêssego" || prod == "PÊSSEGO" || prod == "pessego" || prod == "Pessego"|| prod == "PESSEGO")
    {
        alert(`Você comprou ${prod}. Custa R$ 6,90/Kg.`);
    }

    else if (prod == "uva" || prod == "Uva" || prod == "UVA")
    {
        alert(`Você comprou ${prod}. Custa R$ 7,90/Kg.`);
    }

    else if (prod == "goiaba" || prod == "Goiaba" || prod == "GOIABA")
    {
        alert(`Você comprou ${prod}. Custa R$ 11,70/Kg.`);
    }

    else if (prod == "abacaxi" || prod == "Abacaxi" || prod == "ABACAXI")
    {
        alert(`Você comprou ${prod}. Custa R$11,98/Kg.`);
    }

    else if (prod == "açaí" || prod == "Açaí" || prod == "AÇAÍ" || prod == "açai" || prod == "Açai" || prod == "AÇAI")
    {
        alert(`Você comprou ${prod}. Custa R$ 86,90/Kg.`);
    }

    else if (prod == "acerola" || prod == "Acerola"|| prod == "ACEROLA")
    {
        alert(`Você comprou ${prod}. Custa R$ 3,20/Kg.`);
    }

    else if (prod == "limão" || prod == "Limão" || prod == "LIMÃO" || prod == "limao" || prod == "Limao" || prod == "LIMAO")
    {
        alert(`Você comprou ${prod}. Custa R$ 2,49/Kg.`);
    }

    else if (prod == "jabuticaba" || prod == "Jabuticaba" || prod == "JABUTICABA")
    {
        alert(`Você comprou ${prod}. Custa R$ 44,09/Kg.`);
    }

    else if (prod == "maçã" || prod == "Maçã" || prod == "MAÇÃ" || prod == "maça" || prod == "Maça" || prod == "MAÇA")
    {
        alert(`Você comprou ${prod}. Custa R$ 6,99/Kg.`);
    }

    else if (prod == "pera" || prod == "Pera" || prod == "PERA")
    {
        alert(`Você comprou ${prod}. Custa R$ 3,99/Kg.`);
    }

    else
    {
        return alert("Não temos essa fruta em estoque");
    }
}
}

export default App;