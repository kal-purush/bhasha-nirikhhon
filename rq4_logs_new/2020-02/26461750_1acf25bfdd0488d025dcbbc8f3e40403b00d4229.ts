import {Injectable} from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class ArtigosService {

    private itens: {  };

    constructor() {
        this.itens = {
            0: {
                title: 'A Bangu',
                description: '1. De qualquer forma, jeito, de forma grosseira, tosca, às pressas. Ex.: O gaudério se pilchou a bangu (se pilchou às pressas, de forma descuidada, mal acabada).\n2. De forma descontrolada, sem limites. Ex.: "Já no final do baile, o bolicheiro começou a me servir canha a bangu!" (servir à vontade).'
            },
            1: {
                title: 'A La Cria',
                description: 'Se mandar a La Cria significa se mandar a Deus-dará, sem rumo, cair no mundo. \n\nEx: Ele se mandou a la cria para os EUA, mas nunca esteve por lá.'
            },
            2: {
                title: 'A la fresca',
                description: 'Exprime admiração, espanto, surpresa, descrença. '
            },
            3: {
                title: 'A la pucha',
                description: 'Segundo o site http://www.alapucha.com.br : Quem é gaúcho certamente já ouviu essa expressão. Usada tradicionalmente quando algo nos pega de surpresa, nos espanta ou nos encanta.'
            },
            4: {
                title: 'A La Pucha, tchê!',
                description: 'Exprime admiração, espanto! Quando algo pega o vivente de revesgueio, de surpresa!'
            },
            5: {
                title: 'A troco?',
                description: 'Usado no sentido de “por quê?”, “para quê?”. \nExemplo de uso: "A troco de que tu fez isso?"'
            },
            6: {
                title: 'A vaca foi pro brejo',
                description: 'Ja era, não vai mais dar certo ou acontecer algo'
            },
            7: {
                title: 'Abagualado',
                description: 'Aquele que é bagual, arisco, sestroso, espantadiço. Adjetivo dado ao indivíduo grosseiro, inculto, rústico, amatutado, selvagem.'
            },
            8: {
                title: 'Abaralhar',
                description: 'Agarrar, pegar não deixar cair ao solo.'
            },
            9: {
                title: 'Abatumado',
                description: 'Um bolo ou pão que não cresceu; pessoa que está desanimada...'
            },
            10: {
                title: 'Abichornado',
                description: 'Sinônimo de chateado, doente, deprimido...'
            },
            11: {
                title: 'Abocanhar',
                description: 'Apoderar-se indevidamente de alguma coisa.'
            },
            12: {
                title: 'Aboletar-se',
                description: 'Instalar-se. Ocupar, indevidamente, determinado lugar.'
            },
            13: {
                title: 'Aborrir-se',
                description: 'Verbo português muito utilizado em certos lugares do Estado. Significa aborrecer-se, ficar triste, tornar-se melancólico.'
            },
            14: {
                title: 'Abostado',
                description: 'Abobado, bobo, que tem bosta na cabeça, sem juízo.'
            },
            15: {
                title: 'Abrir a graxeira',
                description: 'Falar tudo o que sabe ou o ato de vomitar. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            16: {
                title: 'Acabrunhado',
                description: 'Entristecido, desanimado, ...'
            },
            17: {
                title: 'Acadelar',
                description: 'Se esconder, ficar com medo, desistir'
            },
            18: {
                title: 'Acadelhar',
                description: 'Pessoa fica se fazendo para ir em algum lugar ou fazer alguma coisa'
            },
            19: {
                title: 'Acalambrado',
                description: 'Intimidado, amedrontado. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            20: {
                title: 'açoitar',
                description: '= bater, surrar'
            },
            21: {
                title: 'Acoleirado',
                description: 'Unido o animal a outro pela coleira. Andar uma pessoa acoleirada com outra. Significa andar uma pessoa sempre junto de outra'
            },
            22: {
                title: 'Acolherar',
                description: 'Juntar, unir, aparceirar.'
            },
            23: {
                title: 'Acoxambrar',
                description: 'Fazer corpo mole, preguiçoso. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            24: {
                title: 'Acrocar',
                description: 'Acocorar-se, pôr-se de cócoras.'
            },
            25: {
                title: 'Acuar',
                description: 'Latir o cão.'
            },
            26: {
                title: 'Acuierá',
                description: 'Muito usada para bailes. O mesmo que aglomeração, juntamento.\n"Hoje a noite vamos todos nos acuierá no Clube Farrapos". '
            },
            27: {
                title: 'Afilar',
                description: 'Caçoar, dar vaia, troçar com alguém.'
            },
            28: {
                title: 'Afocinhar',
                description: 'Quem “afocinhou” é porque caiu de nariz no chão, tropeçou.'
            },
            29: {
                title: 'Afofar',
                description: 'Exagerar, fazer algo em demasia ou com rapidez. Ex.: "Afofei na bóia" (Exagerei na comida). Pedido para alguém acelerar a velocidade da viagem: "Afofa, senão a gente se atrasa".'
            },
            30: {
                title: 'Afrouxar',
                description: 'recuar por medo, acovardar-se, arrolhar. Usa-se em sentido figurado, afrouxar a ponta, como conceder certas liberdades que, no geral descambam para o abuso. '
            },
            31: {
                title: 'Afudê',
                description: '[subst.] Legal, bom. Exemplo: "Bah, que afude essa música."'
            },
            32: {
                title: 'Agalhas',
                description: 'Velhacaria, trampolinice, parlapatice, fanfarrice.'
            },
            33: {
                title: 'Agalhudo',
                description: 'É o indivíduo esforçado, audacioso, forte, ousado.'
            },
            34: {
                title: 'Agnoline',
                description: 'Capeletti dos gringos da Serra.'
            },
            35: {
                title: 'Aguinaldo',
                description: 'Décimo terceiro salário. \'Já recebeste o Aguinaldo?\'\n\n*Contribuição do Alfabeto de Quaraí". '
            },
            36: {
                title: 'Ajojado',
                description: 'Atado por ajoujo, amarrado junto, unido lado a lado. '
            },
            37: {
                title: 'Ajoujo',
                description: 'Tira fina de couro usada para prender ou atar coisas '
            },
            38: {
                title: 'Ajudanta',
                description: 'Mulher que presta ajuda em qualquer serviço.'
            },
            39: {
                title: 'Ajupe',
                description: 'Termo utilizado por tropeiros para estimular os animais.'
            },
            40: {
                title: 'Ala pucha tchê!',
                description: 'Expressão usada quando se surpreende com algo.'
            },
            41: {
                title: 'alambrado',
                description: 'campo alambrado é o que está fechado por cerca de arame. '
            },
            42: {
                title: 'Alarido',
                description: 'Conversa alta; muita gente falando alto, ao mesmo tempo.'
            },
            43: {
                title: 'Alarifagem',
                description: 'Ação de alarife. Qualidade do alarife. Proeza do alarife. Esperteza, velhacagem, trapaça.'
            },
            44: {
                title: 'Alarife',
                description: 'Sinônimo de: malandro, embusteiro, escorregadio, viscoso, espertalhão.'
            },
            45: {
                title: 'Alarifo',
                description: 'Desconfiado, espantado...'
            },
            46: {
                title: 'alarmento',
                description: 'Pessoa que chega falando gritando em um local.'
            },
            47: {
                title: 'alçada',
                description: 'quando a cadela entra no cio.\nex.: a cadela está alçada.'
            },
            48: {
                title: 'Alcaide',
                description: 'O mesmo que arcaide, que não vale nada pronunciada no interior'
            },
            49: {
                title: 'Alcatra',
                description: 'A parte da rês constituída pelos ossos da bacia e acompanhados de alguma carne. Também significa assento: “... e quando se aproximou com a alcatra esbandalhada...”.'
            },
            50: {
                title: 'Alegrete',
                description: 'Termo utilizado para se referir àquele que, levemente embriagado, manifesta alegria.'
            },
            51: {
                title: 'Alertar os gansos',
                description: 'Dar alarme sobre algo. Despertar a atenção indesejadamente.'
            },
            52: {
                title: 'Alevanta',
                description: 'Corruptela da conjugação "levante" do verbo "levantar". Ex: "Alevanta, guri! Sai desta cama que o sol já nasceu!"'
            },
            53: {
                title: 'Aligeirar',
                description: 'Ir mais rápido, se apressar. Ex: Te aligeira, guri. (Vamos rápido, te apressa). '
            },
            54: {
                title: 'Alpargatas',
                description: 'Alpargata é um calçado do tipo sapatilha, feita em brim ou lona, com solado de corda ou borracha, e que pode ser preso ao pé por meio de tiras de couro, corda ou pano.\nTeve origem entre os trabalhadores das docas, na França (espadrille) e na Espanha (alpargata). Foi moda na década de 1980.\nAs alpargatas podem ser usadas tanto por homens como por mulheres. Pode servir também como substituto da bota no traje típico dos gaúchos.'
            },
            55: {
                title: 'Amanar',
                description: 'Ficar mano a mano no jogo; ficar em igualdade de condições.'
            },
            56: {
                title: 'Amanda bobona',
                description: '"Amanda bobona" significa Menina levada, bobinha, brincalhona e muito lezinha, mas amável e muito especial.'
            },
            57: {
                title: 'Amargo',
                description: 'Como o mate chimarrão é chamado no Rio Grande do Sul. Mate amargo, mate sem açúcar.'
            },
            58: {
                title: 'Amasiado',
                description: 'Diz-se do homem que possui uma relação estável, porém não oficializada pelo matrimônio'
            },
            59: {
                title: 'Amolado',
                description: 'Sinônimo de doente'
            },
            60: {
                title: 'Amuado',
                description: 'Triste. Melancólico.'
            },
            61: {
                title: 'Andaço',
                description: 'Diarréia forte e duradoura'
            },
            62: {
                title: 'Aperriar',
                description: 'Ato de incomodar, apressar'
            },
            63: {
                title: 'Aplastado',
                description: 'Aquele que está cansado, abatido, esmorecido, fatigado, desanimado.'
            },
            64: {
                title: 'aprochega',
                description: '1- se aproximar, chegar perto\nEx: te aprochega ai e toma um mate tche\n'
            },
            65: {
                title: 'Aprochegar',
                description: 'Chegar perto, unir-se.\n"Te aprochega aí vivente."'
            },
            66: {
                title: 'Apruma',
                description: 'Arrumar/Ajeitar.... Te aprume índio veio que o baile hoje começa antes'
            },
            67: {
                title: 'Apurar',
                description: 'apurar\nvtd e vpr Reg (Rio Grande do Sul) 1 Apressar o passo. 2 Dar mais pressa ao que está fazendo. 3 Ficar sobrecarregado de serviço.'
            },
            68: {
                title: 'Aquerenciado',
                description: 'Termo utilizado para se referir a um animal que está acostumando a viver em determinado lugar, ou em companhia de outros animais. Também é utilizado para se referir a pessoas.'
            },
            69: {
                title: 'Aragano',
                description: 'Pessoa sem rumo, preguiçosa, vagabundo.'
            },
            70: {
                title: 'aragem',
                description: 'Em gauchês, sinônimo de neblina fria, "sereno", orvalho. Assim, quando a mãe manda o guri entrar pra dentro de casa por causa do frio, ela diz: "Sai do sereno guri!" ou "Sai dessa aragem, vai pegar uma gripe!".'
            },
            71: {
                title: 'Araqüê',
                description: 'Expressão de indignação muito antiga, com o mesmo significado de "oras pois", "que droga", etc. Pode ser vista no filme "Anahy de las Misiones", que retrata a fala gaúcha na época da Guerra dos Farrapos, no que se chamou de dialeto guasca. Ainda usada em locais interioranos da região das Missões hoje em dia.'
            },
            72: {
                title: 'Arigó',
                description: '[s.m.] Burro; desastrado. (ex.: O arigó deixou cair todos os pratos no chão.)'
            },
            73: {
                title: 'Arrancar os olhos e lamber o buraco',
                description: 'Cobrar muito caro por algum produto ou serviço. '
            },
            74: {
                title: 'Arrancar pessegueiro',
                description: 'Expressao utilizada para dizer que algo é muito bom. Ex. O baile estava de arrancar pessegueiro.'
            },
            75: {
                title: 'Arrastando as alpargatas',
                description: 'Realizando trabalho com exaustão, cansaço.'
            },
            76: {
                title: 'Arrastinada',
                description: 'Muito ofendido, maltratado, humilhado... a resto de nada. '
            },
            77: {
                title: 'Arrear',
                description: 'v. tr.\n1. Realizar algo muito difícil.\n2. Fazer algo muito bem feito (ver também \'se puxar\').\n(ex.: Bah, ele se arreou fazendo aquele golaço no grenal.)'
            },
            78: {
                title: 'Arreganhada',
                description: 'Mulher muito sorridente, muito dada\n\nAplicação\n"Mais arreganhada que gaita de bolicho"\n'
            },
            79: {
                title: 'Arregar',
                description: 'Ficar com medo.'
            },
            80: {
                title: 'arreio',
                description: 'peças com que se arreia um cavalo para montar. '
            },
            81: {
                title: 'Arrepoiado',
                description: 'O vivente caiu de cara no asfalto e ficou todo arrepoiado'
            },
            82: {
                title: 'Arrodião',
                description: 'Derrota vexatória, dificuldade de obter êxito em alguma coisa.\nExemplo: O fulano levou um arrodião naquele jogo!'
            },
            83: {
                title: 'Arrodiar',
                description: 'Girar, fazer rodar.\n'
            },
            84: {
                title: 'Arroio',
                description: 'O que no Brasil é chamado de "riacho", "córrego". Com essa palavra ocorre o mesmo que com "sanga".'
            },
            85: {
                title: 'Arroz de china pobre',
                description: 'Comida típica feita com arroz, linguiça, cebola picadinha, alho e pimentão. É uma comida de origem popular, bem conhecida e apreciada entre a população mais humilde do estado.'
            },
            86: {
                title: 'Asneira',
                description: 'bobagem, algo idiota'
            },
            87: {
                title: 'Ataio',
                description: 'Contração de atalho. "Vai e pega um ataio pra chega la em casa , tchê"'
            },
            88: {
                title: 'Atarantado',
                description: 'Diz-se do sujeito que está com muitas tarefas e, por conta disso, fica atordoado, em dificuldades. Atarantado'
            },
            89: {
                title: 'Atinar',
                description: 'Perceber, dar-se por conta de algo.'
            },
            90: {
                title: 'Atochar',
                description: 'Mentir, fantasiar. Normalmente, mentiras de pequena importância, como exagerar um feito.\nExemplo: "Aqui se reunem caçadores, pescadores e outros atochadores".'
            },
            91: {
                title: 'Atrolhado',
                description: 'Diz-se do sujeito que está cheio de trabalho, cheio de coisas pra fazer.\n\nEx.: "Não vou poder almoçar com vocês porque estou atrolhado de serviço."'
            },
            92: {
                title: 'Atucanada',
                description: 'Nervosa, preocupada, atrapalhada, ocupada, apoquentada.'
            },
            93: {
                title: 'Atucanado',
                description: 'Quando se tem muitas coisas para fazer, vários problemas para resolver, tarefas de trabalho e outras questões, todas em prazos semelhantes, quase ao mesmo tempo, e fica-se agitado fazendo várias coisas de uma só vez, tentando dar conta do recado.\nEx.:\n- Tens um minuto para conversarmos?\n- Agora não dá, tô muito atucanado!'
            },
            94: {
                title: 'Auto',
                description: '[s.m.] Automóvel; carro. (ex.: Não sei onde deixei as chaves do meu auto.)'
            },
            95: {
                title: 'Avio',
                description: 'Isqueiro de pedra, pederneira.'
            },
            96: {
                title: 'Azar é do goleiro',
                description: 'Tipo "doa a quem doer". "Não quero saber, não tô nem aí!".'
            },
            97: {
                title: 'Azedume',
                description: 'Mal humorado, chateado, acabrunhado...ex: mas tá num azedume que só vendo...'
            },
            98: {
                title: 'Azucrinar',
                description: 'Incomodar alguém.\nEx: Para de azucrinar guri.'
            },
            99: {
                title: 'Babuja',
                description: 'Alguma coisa grande'
            },
            100: {
                title: 'Babujera',
                description: 'Uma coisa muito maior que algo babuja'
            },
            101: {
                title: 'Bacanudo',
                description: 'Muito bacana. Muito legal.'
            },
            102: {
                title: 'Bachero',
                description: 'O mesmo usa em cavalos, estrapiado, judiado. Ex: que loco bachero aquele'
            },
            103: {
                title: 'Bacuri',
                description: 'menino, garoto, filho'
            },
            104: {
                title: 'Bagacera',
                description: 'Pessoa incoveniente. Chinelão. Turma de amigos. '
            },
            105: {
                title: 'Bago',
                description: 'Pode ter dois significados:\n1- Testículos. Ex: Ai, me chutaste bem nos bagos, mulher!\n2- Chute muito forte na bola de futebol, que geralmente a manda pra bem longe do campo. Ex: Pra quê dar um bago desses na bola, guri??'
            },
            106: {
                title: 'Bagual',
                description: 'Pessoa grosseira, estúpida.'
            },
            107: {
                title: 'Baguari',
                description: 'Vagaroso, lerdo, mole, pesadão.'
            },
            108: {
                title: 'bah',
                description: 'Oriunda da expressão "bah tchê", sinônimo de "mas que barbaridade".\nA expressão "bah" é uma das mais polivalentes da cultura da República Rio-Grandense. Possui vários significados dependendo da situação, do cenário, o quanto se prolonga ao pronunciar e da entonação com que é pronunciada.\n\nEspanto: "Bah, e é barato!"\nDesapontamento: "baahhh, mas é um animal..."\nIndignação ou inconformidade: "Baah, nem vou te falar é mais nada"\nIndiferença ou desinteresse: "uhum, bah..."\nElogio: "Baaahhhh"\nTristeza: "Bah mas com essa se foi o boi com a corda."\nFelicidade: "Bah, mas é louco de especial !!"\nConcordância: "BAAHHHH, beeem certinho !!!"\nSurpresa: "Baaaahhh mas é uma \'cavala\'(gostosa) !!"\nRevolta: Bã!'
            },
            109: {
                title: 'Baia',
                description: 'Casa ou residência, local aonde pessoas vivem.\nEx. Eu vou para minha "baia". Eu vou para minha casa.'
            },
            110: {
                title: 'Baiacu',
                description: 'Peixe das costas oceânicas do RS. Quando irritado, enche-se de ar, ficando como uma bola, e nadando, então, de ventre pra cima.'
            },
            111: {
                title: 'Baio',
                description: 'Brabo, descontrolado, enfurecido.'
            },
            112: {
                title: 'Baita',
                description: 'Algo muito grande, gigantesco.'
            },
            113: {
                title: 'Balaca/balaqueiro',
                description: 'malandro, cheio de si, pomposo.'
            },
            114: {
                title: 'Balaqueira',
                description: 'Pessoa gabola, sabe tudo, não confiável. Também significa o indivíduo que se exibe demais. '
            },
            115: {
                title: 'Baldoso',
                description: 'Pessoa carente, precisando de carinho. \n\n*Contribuição do Alfabeto de Quaraí'
            },
            116: {
                title: 'balota',
                description: 'mentira'
            },
            117: {
                title: 'Banda',
                description: 'O termo significa lugar, região, paragem. Quando alguém diz: “Sou de outras bandas” quer dizer que é de outro lugar, de outra regiões ou de outras paragens.'
            },
            118: {
                title: 'Banzo',
                description: 'É uma pessoa \'não muito certa\'. \nEx.: Mas tu é banzo né!!'
            },
            119: {
                title: 'Barateza',
                description: 'Sacanagem, baixaria'
            },
            120: {
                title: 'Barbaridade!!',
                description: 'Expressão usada em frases de espanto, susto, alegria, indignação. Exemplo: Tchê mas que barbaridade!'
            },
            121: {
                title: 'barbicacho',
                description: 'tira de couro, de algodão ou similar que prende o chapéu ao queixo.'
            },
            122: {
                title: 'Barde',
                description: 'Manha\nEx: -Espera nós chega em casa pia, quero tira tuas barde!'
            },
            123: {
                title: 'BARRANQUEAR',
                description: 'Praticar a bestialidade (i.e., sexo com animais), usando o barranco quando necessário para nivelar a diferença de altura (p.ex., com éguas e vacas). "Barranquear é verbo xucro | Mais velho que a monarquia | Criado em fodologia | De um modo pampeiro e franco"'
            },
            124: {
                title: 'Bateclô',
                description: 'Local destinado a receber dejetos oriundos da veia bostérea, latrina. Derivado do vocábulo inglês Water-Closet'
            },
            125: {
                title: 'Batendo água',
                description: 'Sinônimo de chuva forte, torrencial.\nExemplo de uso: "Se eu fosse tu não saia agora, tá batendo água."'
            },
            126: {
                title: 'Bater as botas',
                description: 'Morrer, falecer...'
            },
            127: {
                title: 'Bater cola',
                description: 'Sair de casa sem um bom motivo. Ex: Essa guria não pára de sair pra bater cola no shopping!'
            },
            128: {
                title: 'Batocaço',
                description: 'Golpe que o galo dá com os batoques, quando rinhado.'
            },
            129: {
                title: 'baús',
                description: 'muito grande, muita coisa, sinônimo de muito, aplica-se numa frase como: "tinha baús de coisas para fazer"'
            },
            130: {
                title: 'Baus de tri',
                description: 'Muito legal mesmo; demais. '
            },
            131: {
                title: 'Bei',
                description: 'Derivação de Bah.'
            },
            132: {
                title: 'bem capaz!',
                description: 'tem significado de negação à alguma  coisa ou pedido.\nEx: Posso furar a fila do mate? Bem capaz!'
            },
            133: {
                title: 'Bem nessas',
                description: 'Significa dizer " sim, isso mesmo, eu concordo contigo".'
            },
            134: {
                title: 'Berba',
                description: 'Pelas beiradas. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            135: {
                title: 'Berga',
                description: 'Diminutivo de "bergamota", ou seja, o mesmo que "tangerina".\nEx. "Aproveitei o sol para passar a tarde na Redenção, lagarteando e comendo uma berga".'
            },
            136: {
                title: 'BERGAMOTA',
                description: 'A tangerina (Citrus reticulata), também laranja-mimosa, mandarina, fuxiqueira, ponkan, laranja-cravo, mimosa, vergamota, clementina, bergamota ou mexerica[1], é uma fruta cítrica de cor alaranjada e sabor adocicado. Parece ser uma antiga espécie selvagem [2], nativa da Ásia (Índia, China e países vizinhos de clima subtropical e tropical úmido).'
            },
            137: {
                title: 'Biaba',
                description: 'significado: tapa, soco.\norigem: termo gauchesco e crioulo argentino.\nEx: Vou te dar umas biabas se não obedecer!'
            },
            138: {
                title: 'Bicheira',
                description: 'Coisa muita velha: normalmente utilizada para especificar automóvel muito velho, caindo aos pedaços.'
            },
            139: {
                title: 'Bicicleteiro',
                description: 'Ciclista, pessoa andando de bicicleta. EX: Quase atropelei um bicicleteiro na rua hoje. '
            },
            140: {
                title: 'Bidê',
                description: 'Mesinha de cabeceira.'
            },
            141: {
                title: 'Bidu',
                description: 'Cachorro quente vendido em trailers. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            142: {
                title: 'Biriquete',
                description: 'Viela, caminho estreito, esconderijo.'
            },
            143: {
                title: 'Bisca',
                description: 'Pessoa nojenta , rabugenta , chata. Também pode significar quem faz biscate. '
            },
            144: {
                title: 'Biscaio',
                description: 'Facão grande usado pelos homens que trabalham no mato.'
            },
            145: {
                title: 'Bixento',
                description: 'Pessoa ralé, medíocre'
            },
            146: {
                title: 'Boca entaipada',
                description: 'Diz-se quando a situação não está boa. Ex.: Vamos embora que estourou um bochincho e é "boca entaipada".'
            },
            147: {
                title: 'Boca floreada',
                description: 'Tolo, atrapalhado, lerdo, distraído\nEx.: O boca floreada esqueceu a carne do churrasco'
            },
            148: {
                title: 'Bochincho',
                description: 'Confusão, troca de xingamentos, discussão acalorada, etc. Ex.: "Qual foi o bochincho dessa vez? Aquela guria tá sempre puxando briga com todo mundo!"'
            },
            149: {
                title: 'Boco',
                description: 'Círculo feito no solo com a pressão do calcanhar enquanto o corpo gira para marcar o local onde deve-se colocar a bola atirada no jogo de "sela"; A quantidade de bocos corresponde ao número de jogadores menos 1; Cada jogador escolhe 1 boco, exceto o lançador.'
            },
            150: {
                title: 'Bodear',
                description: 'Sinônimo de "lagartear", dormir, descansar deitado, etc.\n\nAssim como o lagarto fica ao sol sobre as pedras (curtindo a boa vida) os bodes também aproveitam o sol e o vento enquanto pastam.\n\nEx.: Mas olha lá o fulano! Já tá bodeando! [lagarteando]'
            },
            151: {
                title: 'Bodega',
                description: 'Diz-se de mercado de pequeno porte, venda de secos e molhados pequeno.'
            },
            152: {
                title: 'Bodoque',
                description: 'O mesmo que funda ou estilingue.'
            },
            153: {
                title: 'Boi lerdo toma água toldada!',
                description: 'Aquele que se atrasar perderá a melhor parte.'
            },
            154: {
                title: 'bóia',
                description: 'comida'
            },
            155: {
                title: 'Boiada',
                description: 'Porção de bois mansos, especialmente do serviço de carrêtas.'
            },
            156: {
                title: 'Boleiar',
                description: '"Boleia a perna", quer dizer, levanta e dá uma sacudida na perna. '
            },
            157: {
                title: 'Boleio',
                description: 'Chute forte no futebol. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            158: {
                title: 'Bolicho',
                description: '[s.m.] Pequeno bar ou estabelecimento comercial onde o gaúcho se reúne para tomar uma canha, para ver noticias do povo e comprar um pedaço de fumo em corda. (ex.: Fui no Bolicho jogar uma bocha e tomar um trago de canha!)'
            },
            159: {
                title: 'bolija',
                description: 'bolita, bola de gude. Do cast. bolilla.'
            },
            160: {
                title: 'Bombacha',
                description: 'A bombacha é uma peça de roupa, calças típicas abotoadas no tornozelo, usada pelos gaúchos. O nome foi adotado do termo espanhol "bombacho", que significa "calças largas".\nPode ser feita de brim, linho, tergal, algodão ou tecidos mesclados; de padrão liso, listrado ou xadrez discreto. As cores podem ser claras ou escuras, fugindo-se de cores agressivas, chocantes e contrastantes.\nNo Rio Grande do Sul, a bombacha, juntamente com toda a indumentária característica do gaúcho, é considerada traje oficial desde 1989, quando foi aprovada a Lei Estadual da Pilcha [1] pela Assembléia Legislativa. De acordo com a Lei, a pilcha gaúcha -- o conjunto de vestes tradicionais tanto masculino quanto feminino -- pode substituir trajes sociais -- ex. terno e gravata para os homens e vestidos de tecidos mais nobres para as mulheres -- em qualquer ocasião formal que ocorra no Rio Grande do Sul, inclusive reuniões das Assembléias Legislativas estadual e municipais, desde que se observe as recomendações ditadas pelo Movimento Tradicionalista Gaúcho (MTG).'
            },
            161: {
                title: 'Bombando',
                description: 'Quando tu chegas em um lugar e a o ambiente tá \'loco de especial\', ceva gelada, boa música e todos os amigos. '
            },
            162: {
                title: 'Bombeia',
                description: 'Olhar algo ou para alguém. Ex: Bombeia só, a guria tá de namorado novo. \n'
            },
            163: {
                title: 'Bonachão',
                description: 'Diz-se do sujeito de boa índole, calmo e tranquilo.'
            },
            164: {
                title: 'Bonanchão',
                description: 'Aquele que é bondoso, calmo, cavalheiro, franco.'
            },
            165: {
                title: 'Bordonear',
                description: 'Executar uma música em viola ou violão usando as cordas mais grossas, que emitem sons mais graves.'
            },
            166: {
                title: 'Borracheira',
                description: 'Estado de embriaguez, bebedeira, tragueado, bêbado.'
            },
            167: {
                title: 'borracho',
                description: 'borracho é o índio véio mamado, bêbado. aquele que tá mais pra lá do que pra cá.'
            },
            168: {
                title: 'BRAGUETA',
                description: 'O mesmo que braguilha: parte frontal do vestuário masculino, que, ao ser aberta, permite ao usuário exteriorizar o pênis para urinar.'
            },
            169: {
                title: 'BRAGUILHA',
                description: 'peça do vestuário masculino que se usava nos séculos XV e XVI para cobrir os órgãos genitais. Por extensão o nome é aplicado a frente das calças, calções ou cuecas que se costuma fechar. Permite ao seu portador retirar o pênis para fora na ocasião de urinar.  \nNos séculos XV e XVI a braguilha surge por razões de pudor. '
            },
            170: {
                title: 'Brazino',
                description: '[adjetivo] Qualidade daquilo que assume a coloração da brasa acesa.\n\n  Exemplo:\n - Guri, volta aqui  ou vou te deixar brazino a pau. '
            },
            171: {
                title: 'Brete',
                description: 'Onde vacina o gado.'
            },
            172: {
                title: 'bruaca',
                description: '1. megera, mulher que parece um dragão\n2. Compartimento de carga usado em lombo de cavalos e muares e que no tempo das tropeadas servia para o transporte de especiarias, ferramentas, tecidos etc, para pontos distantes do território brasileiro e ainda hoje usada em regiões de minifúndio, onde há dificuldade de uso de outro meio de transporte; espécie de bolsa de grandes dimensões, usada sempre aos pares penduradas nas cangalhas, equipamento de suporte apoiado diretamente sobre o lombo do animal.'
            },
            173: {
                title: 'Bruxo',
                description: 'Utilizado como forma de tratamento aos flanelinhas. '
            },
            174: {
                title: 'Buchincho',
                description: 's.m.\nGrafia alternativa de \'bochincho\' [veja: \'bochincho\'].\nConfusão; tumulto; briga generalizada.\n(ex.: Mas não é que a chinoca pegou o índio velho traindo ela e o bichincho estava formado.)'
            },
            175: {
                title: 'Bucho',
                description: 'Estômago. Ex: Hoje vou encher o bucho no churras do meu primo!'
            },
            176: {
                title: 'Buchuda',
                description: 'O mesmo que prenha, grávida.'
            },
            177: {
                title: 'Budum',
                description: 'Grande desordem, sujeira, bagunça...\nEm algumas localidades, também significa mau odor. '
            },
            178: {
                title: 'Buenacha',
                description: 'Significa uma coisa boa.'
            },
            179: {
                title: 'Buenas',
                description: '[interj.] Buenas é um cumprimento, pode ser usado em qualquer momento do dia para cumprimentar amigos e conhecidos. (ex.: Buenas, tchê! Como vai a família?)\nEternizado na fala do Capitão Rodrigo, personagem de Érico Verísimo, em O tempo e o vento: "Buenas e me espalho..."'
            },
            180: {
                title: 'Bugio',
                description: 'Pelego curtido e pintado, em geral forrado de pano. Animal da Serra.'
            },
            181: {
                title: 'Buxa',
                description: 'Quem nunca disse "É buxa!!", "Pura Buxa!!"... Palavra usada para expressar algo "complicado".'
            },
            182: {
                title: 'Cabeça de porongo',
                description: 'Cabeção, teimoso, cabeça dura...'
            },
            183: {
                title: 'Caborteira',
                description: 'Cavalo ou outro animal, manhoso, arisco, infiel, velhaqueador, que não se deixa pegar.\nIndivíduo velhaco, esperto, manhoso, mau, mentiroso, trapaceiro, tratante, que vive de expedientes.'
            },
            184: {
                title: 'Cabresto',
                description: 'Peça de couro apresilhada ao buçal ou buçalete para segurar o cavalo ou o muar. É comum o uso da expressão “sentar no cabresto”.'
            },
            185: {
                title: 'Cacaria',
                description: 'Porção de objetos velhos e sem serventia. Cacarecos tem o mesmo significado.'
            },
            186: {
                title: 'Cacetinho',
                description: 'O famoso Pão Frances'
            },
            187: {
                title: 'Cachetada',
                description: 'Bofetada, paulada, soco.'
            },
            188: {
                title: 'Cachorro de baile',
                description: 'Gurizada no baile atrás do conjunto só olhando. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            189: {
                title: 'Cacunda',
                description: 'Costas, ombros. \n'
            },
            190: {
                title: 'Cafundó',
                description: 'Lugar ermo e solitário, grota. É comum o uso da expressão “lá nos cafundós do Judas”.'
            },
            191: {
                title: 'Cagaço',
                description: 'tomar um susto muito grande, vivenciar algo inesperado com medo'
            },
            192: {
                title: 'Cagando e andando',
                description: 'O cara que não tá se importando muito com determinada situação. Mesmo que seja difícil imaginar a cena.'
            },
            193: {
                title: 'Cagar a pau',
                description: 'Bater muito, dar uma surra.'
            },
            194: {
                title: 'Calavera',
                description: 'aquele que compra e não paga. enganador, golpista...'
            },
            195: {
                title: 'Camadona',
                description: 'Palavra de bastante espanto, tipo como "Camadona! Tu viu aquilo?'
            },
            196: {
                title: 'Camareu',
                description: 'Sinônimo - camarão\n\nUtilizado princialmente por vendedores ambulantes que circulam pela cidade de bicicleta vendendo o  crustáceo.'
            },
            197: {
                title: 'Cambona',
                description: 'Recipiente para aquecer água para o mate no fogo de chão. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            198: {
                title: 'Cambota',
                description: 'Cair realizando um giro com o corpo para frente.....Cambalhota, rolamento para frente, rolinho....'
            },
            199: {
                title: 'camoatim',
                description: 'Espécie vespa produtora de um gostoso mel, muito encontrado em colméias suspensas em ávores e arbutos nos campos do Rio grande do Sul.'
            },
            200: {
                title: 'Campear',
                description: 'Procurar algo'
            },
            201: {
                title: 'Camperiar',
                description: 'Sair campo a fora, ou sair pra lide campeira.'
            },
            202: {
                title: 'Cancheiro',
                description: 'Cavalo cancheiro é o já habituado e mestre em correr. Deriv. De cancha.'
            },
            203: {
                title: 'Candanga',
                description: 'Sujeito andarilho, sem morada certa, estradeiro. Alguém que vive pelas estradas pedindo alimento e pousada. Personagem assustadora das crianças. Meus pais diziam: -"Cuidado que o candanga te pega."'
            },
            204: {
                title: 'capaz',
                description: '"interjeição" de espanto - Ex: CAPAZ QUE TU CONSEGUIU FAZER ISTO!?\n2. "interjeição" de negação - Ex: O QUÊ? BEM CAPAZ!'
            },
            205: {
                title: 'Capincho',
                description: 'O mesmo que bajuladores, aqueles que estão sempre adulando alguma pessoa importante.'
            },
            206: {
                title: 'Caramuru',
                description: 'Brasileiro que não nasce no Rio Grande. Usado desde a época da Guerra dos Farrapos, quando designava os membros do Partido Restaurador (que pretendia a volta de D. Pedro I para o governo do Brasil) e logo passou a designar todos os soldados brasileiros, com os quais os gaúchos peleavam.'
            },
            207: {
                title: 'Carneiro',
                description: 'Aquele que cobiça a mulher dos outros. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            208: {
                title: 'Carpeta',
                description: 'Pasta de documentos ou jogo de cartas. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            209: {
                title: 'Carpim',
                description: 'Meia'
            },
            210: {
                title: 'Carreteiro',
                description: '1. Pessoa, geralmente um homem, que transporta produtos, mantimentos e artigos variados em uma carreta de bois.Os carreteiros de hoje transportam as cargas nos carretas movidas a cavalos mecânicos, ou seja, caminhões. \n2. Os carreteiros antigos inventaram o  "arroz de carreteiro", mais conhecido no Rio Grande do Sul como "carreteiro", feito do charque – carne salgada e seca ao sol para não estragar. '
            },
            211: {
                title: 'Carreteiro de charque',
                description: 'Comida típica, que consiste em arroz com charque (carne seca) picadinho.'
            },
            212: {
                title: 'Cascudo',
                description: 'Pancada em algo ou na cabeça de alguém com os nós dos dedos. Sinônimos: coque, cocorote. Exemplo: A menina deu um cascudo no irmão mais novo.'
            },
            213: {
                title: 'Castelhano(castelhana)',
                description: 'Pessoa de nacionalidade estrangeira Argentina ou Uruguaia'
            },
            214: {
                title: 'Catinga',
                description: 'Mau cheiro. \n\n*Contribuição do Alfabeto de Quaraí. '
            },
            215: {
                title: 'Caudilho',
                description: 'Chefe militar. Manda-chuva.'
            },
            216: {
                title: 'causo',
                description: 'o mesmo que caso, história, qualquer narrativa contada à beira do fogo, nos galpões das estâncias. caso, conto, acontecimento'
            },
            217: {
                title: 'Cazul de linha',
                description: 'Nome dado ao retrós de linha.'
            },
            218: {
                title: 'Cerro',
                description: 'Morro ou montanha baixa. No RS se usa a palavra em espanhol, "cerro", resquício deixado pelos primeiros colonizadores da região - os castelhanos.'
            },
            219: {
                title: 'Ceva',
                description: 'Abreviativo para cerveja, usado principalmente em Porto Alegre. No interior do estado é mais observado o uso de "cerva".'
            },
            220: {
                title: 'Cevar',
                description: 'Preparar, cativar.\nCevar o mate: prepará-lo para ser bebido.\nCevar um animal: acostumá-lo a vir comer num lugar, para posteriormente caçá-lo.\nTermo usado também com o mesmo significado nos países latinos (escrito "cebar").'
            },
            221: {
                title: 'Chafundar',
                description: 'Atolar-se, cair n’água.'
            },
            222: {
                title: 'Chairado',
                description: 'Abatido, cansado... Derivado de faca ruim de fio, que já vem sendo amolada na chaira.'
            },
            223: {
                title: 'Chalana',
                description: 'Embarcação ou Lancha grande e chata'
            },
            224: {
                title: 'Chaleirando',
                description: 'Incomodando, se metendo ns assuntos dos outros, atrapalhando...'
            },
            225: {
                title: 'Chamarisco',
                description: 'O mesmo que chamariz; coisa que chama, que atrai.'
            },
            226: {
                title: 'chamarra',
                description: 'A chamarra é um colete escarlate ou preto, também talar e sem mangas, aberto na frente, usado sobre o roquete. A chamarra era, originalmente, uma jaqueta de equitação ao ar livre e suas pregas traseiras facilitavam o montar e desmontar do cavalo.'
            },
            227: {
                title: 'Chamegão',
                description: 'Assinatura; Ex.: Agora é só tu passar o chamegão na escritura para o negócio ficar preto no branco.'
            },
            228: {
                title: 'Chapéu tapiado',
                description: 'Que levou um tapa e ficou com a aba para cima ou pedurado nas costas, pelo baricacho, preso ao pescoço'
            },
            229: {
                title: 'Charla',
                description: 'Palestra, conversa.'
            },
            230: {
                title: 'Chasque',
                description: 'Jornal .'
            },
            231: {
                title: 'Cheba',
                description: 'Mendigo'
            },
            232: {
                title: 'Cheio de balaca!',
                description: 'Pessoa muito exibida. \nChegou aqui “cheio de balaca”!\n'
            },
            233: {
                title: 'Chergão',
                description: 'Espécie de tapete retangular de lã que se coloca sobre o lombo do cavalo antes da sela. No popular também se usa a expressão para falar uma roupa muito usada e encardida.'
            },
            234: {
                title: 'Chibo',
                description: 'Ovino macho, castrado, de até um ano de idade.'
            },
            235: {
                title: 'Chima',
                description: 'Forma abreviada para "chimarrão".\nEx.: e aí, passa lá em casa pra tomar um chima'
            },
            236: {
                title: 'Chimarrão',
                description: 'Do espanhol cimarrón= selvagem, puro.\nMate chimarrão: mate amargo, preparado sem adição de açúcar, leite, rapadura ou outras ervas; apenas água quente.\nGado chimarrão: gado sem dono, criado solto, sinônimo de "alçado" ou "orelhano", comum no século XIX.'
            },
            237: {
                title: 'Chimas',
                description: '[s.m.] Forma reduzida de \'chimarrão\' (consulta: "chimarrão"). Possui outras variações, como "Chima", principalmente (ex.: Mas que sol bem bom pra tomar um chimas na frente de casa!)'
            },
            238: {
                title: 'Chimbear',
                description: 'Gauderiar, vadiar, vagabundear. O mesmo que chimbiar.'
            },
            239: {
                title: 'Chimia',
                description: 'Termo utilizado pelos descendentes de alemães para designar as geléias de frutas\n\nSinônimos: schmier, geléia\nObservação: käs-schmier: geléia de queijo (venerada pela comunidade germânica do sul do mundo) = pronuncia-se "quechimie" ou "quechimia"\n'
            },
            240: {
                title: 'chimpa',
                description: 'esporte de rua dos piazitos, jogado com pedras (similar à bocha)'
            },
            241: {
                title: 'Chinaredo',
                description: 'Grande número de chinas, mulheres'
            },
            242: {
                title: 'Chinelagem',
                description: 'Sacanagem.'
            },
            243: {
                title: 'Chinelão',
                description: 'Pessoa mal arrumada; coisas mal feitas; relaxamento.\nAquela guria está muito chinelona para ir na festa. '
            },
            244: {
                title: 'Chinelo',
                description: 'Diz-se do indivíduo que vive na ralé.'
            },
            245: {
                title: 'Chinfrim',
                description: 'Modesto, sem graça...  Ex: Essa roupa é muito chinfrim para aquela festa.'
            },
            246: {
                title: 'Chinoca',
                description: 'A mulher gaúcha, a prenda.'
            },
            247: {
                title: 'Chiripa',
                description: 'Veste intima feminina, feita normalmente de pano de algodão.'
            },
            248: {
                title: 'chisme',
                description: 'discussão, tumulto, briga...'
            },
            249: {
                title: 'Chispa',
                description: 'Sai, "te arranca", vai embora. '
            },
            250: {
                title: 'Chorna',
                description: 'confusão, rebuliço, desordem.\nExemplo: "Mas que \'chorna\'* é essa?\n*Chorna: no sentido de confusão.\n'
            },
            251: {
                title: 'Chulear',
                description: 'Torcer para que algo aconteça, dar uma chuleada: ficar na expectativa; fazer pontos de costura a grosso modo antes de costurar em definitivo.'
            },
            252: {
                title: 'Chulepento',
                description: 'Aquele que tem chulé.'
            },
            253: {
                title: 'Chumbeado',
                description: 'Embriagado, bêbado; o mesmo que traguado.'
            },
            254: {
                title: 'Chupim',
                description: 'Aproveitador'
            },
            255: {
                title: 'Churero',
                description: 'Quem vendia miúdos de bovinos.\nEram vendidos por carroceiros pelas ruas de São Borja. Isto até o início da década de 70.\nVem do espanhol "achura".'
            },
            256: {
                title: 'Churras',
                description: 'Abreviatura de Churrasco.\n-Onde vai ser o Churras?'
            },
            257: {
                title: 'Churrascada',
                description: 'Reunião de pessoas para participarem de um churrasco.'
            },
            258: {
                title: 'CHURRIO',
                description: 'Diarreia, desarranjo, caganeira. "O xiru não veio porque deu um CHURRIO nele"'
            },
            259: {
                title: 'Churumela',
                description: 'Conversa fiada, mentira, ou papo furado.'
            },
            260: {
                title: 'Cincha',
                description: 'Peça dos arreios que serve para firmar o lombilho ou o serigote sobre o lombo do animal. '
            },
            261: {
                title: 'Cinchado',
                description: 'Apresilhado, apertado.\nDeriva da palavra cincha, parte do arreio que prende a sela ao cavalo.\nEntre os tradicionalistas, é comum o emprego dos termos "abraço bem cinchado" e "um quebra-costelas",como sinônimos de um abraço bem apertado.'
            },
            262: {
                title: 'Coió de mola',
                description: 'Pessoa boba, inútil, "abobada"!'
            },
            263: {
                title: 'Cola',
                description: 'Cabelo preso. \n\n*Contribuição do Alfabeto de Quaraí'
            },
            264: {
                title: 'Cola fina',
                description: 'Quem não usa pilcha e veste-se no estilo "moderninho"'
            },
            265: {
                title: 'Colono',
                description: 'Agricultor, vivente nascido no interior do estado.'
            },
            266: {
                title: 'Com a boca na botija',
                description: 'Quando o índio é pego na hora H, no pulo do gato. Momento em que a pessoa é surpreendida pronta para realizar determinada ação.'
            },
            267: {
                title: 'Conchavar',
                description: 'Contratar serviços. Também significa alugar-se ou entrar para o serviço de uma estância ou de uma casa qualquer.'
            },
            268: {
                title: 'Conjuminar',
                description: 'Fazer nexo, bater ideias.\nExemplo: \nBah! Não conjuminamos e peguei o ônibus errado.'
            },
            269: {
                title: 'Copa',
                description: 'Parte central e alta do chapéu'
            },
            270: {
                title: 'Copar',
                description: 'Vencer; Ter sucesso; Humilhar; Vencer a copa.\n\nExemplos:\n• Cope afú!\n• O Grêmio copou o campeonato.\n• Está muito bom. Você copou!\n• Cope! Cope sempre sem cessar!\n• Copadaço.'
            },
            271: {
                title: 'Corcovear',
                description: 'Pinotear. Quando o animal dá corcovos, saltos, curvando o lombo para lançar o cavaleiro para fora de si.'
            },
            272: {
                title: 'Cordão da calçada',
                description: 'Em São Paulo, chama-se "meio-fio". Mas é a parte da calçada que é pintada de amarelo, branco, etc.'
            },
            273: {
                title: 'Cortar os naipes',
                description: 'Expressão que indica a necessidade de dar limites à alguém. "Bá, tem que cortar os naipes desse guri".'
            },
            274: {
                title: 'corticeira',
                description: 'seivo ou seibo. corticeira do banhado. '
            },
            275: {
                title: 'Costado',
                description: 'O mesmo que "do lado". Exemplo: Me parei no costado do fogão a lenha a tarde inteira.'
            },
            276: {
                title: 'Costeando',
                description: '"Vamo se costeando pro lado dos parceiros de fundamento".\n\nAproximar-se, fica perto. \n\n'
            },
            277: {
                title: 'Cramenha',
                description: 'Segundo a Fernanda Krige, é uma expressão de desaprovação, que vem do dialéto italiano. Também expressa espanto.'
            },
            278: {
                title: 'Crioulo',
                description: 'Natural de um determinado ponto do Estado. Originário do país, da região do lugar onde vive.'
            },
            279: {
                title: 'Crivado',
                description: 'Cheio de alguma coisa. “Morreu crivado de balas”- morreu atingido por muitas balas.'
            },
            280: {
                title: 'Crivo',
                description: 'Cigarro.\nExemplo: "Tchê, acabaram meus crivo, tu tens um pra me dar?"\n"Me vê um quilo de erva e um pacote de crivo"\n'
            },
            281: {
                title: 'Cuca',
                description: 'Espécie de pão doce (pode ter recheio ou não), possuem uma cobertura açucarada.'
            },
            282: {
                title: 'Cudio',
                description: 'Cudio é uma expressão de grande espanto, por exemplo: "Cudio, que frio!"'
            },
            283: {
                title: 'Cueca de Piola',
                description: 'Rapaz inexperiente'
            },
            284: {
                title: 'Cuiúdo',
                description: 'Muito bom. Fodão. Excepcional. Exemplos: "Esse mate tá cuiúdo!"'
            },
            285: {
                title: 'Culhão',
                description: 'Testículos.'
            },
            286: {
                title: 'Cupincha',
                description: 'Companheiro, amigo. '
            },
            287: {
                title: 'cusco',
                description: 'cachorro'
            },
            288: {
                title: 'Dalhe!',
                description: 'Cumprimento, o mesmo que: e aí?\n\n*Contribuição do Alfabeto de Quaraí. \n'
            },
            289: {
                title: 'Dançata',
                description: 'Bailarico, baileco, dança-rola'
            },
            290: {
                title: 'Dar Chá de garfo',
                description: 'Significa, falar mal de alguém através de indiretas'
            },
            291: {
                title: 'Dar no pé',
                description: 'Correr em disparada.'
            },
            292: {
                title: 'Dar nos dedos',
                description: 'Dar uma resposta rápida e bem dada.Fazendo com que a pessoa que recebeu tenha vontade de esconder o rosto.'
            },
            293: {
                title: 'Dar uma banda',
                description: 'Expressão usada para designar passeio sem especificar onde ou com que finalidade. Sair para passear. Equivalente a "dar um rolê". '
            },
            294: {
                title: 'De apa',
                description: 'Muito, aos montes, bastante.'
            },
            295: {
                title: 'De canto',
                description: 'Discretamente, sem chamar a atenção.'
            },
            296: {
                title: 'De cara',
                description: 'Chocado, surpreso e até mesmo magoado. Tudo junto. "Ô meu, o fulano tá de  cara contigo!". Também pode significar o sujeito que não está sob algum estado alterado de consciência.'
            },
            297: {
                title: 'De H',
                description: 'Expressão usada para designar algo inútil, falso.\nExemplo de uso: "Este cabo é de H"'
            },
            298: {
                title: 'De lamber os beiço',
                description: 'O mesmo que dizer: E dê-se por satisfeito!\nOu essa comida está de lamber os beiço: muito saborosa!'
            },
            299: {
                title: 'De Patrão!',
                description: '(Também "De Capa!".) Interjeição de satisfação referente a algo, uma pessoa ou situação. Exemplo: "RadioSemFronteira.com - Essa é de Patrão!"'
            },
            300: {
                title: 'De ponta',
                description: 'Em pé, de pé. \n"-Hoje, tempranito já estava de ponta."'
            },
            301: {
                title: 'De rachar',
                description: 'Geralmente refere-se ao frio muito intenso. "Bah, hoje tá fazendo um frio de rachar os beiços". Mas pode se referir aos efeitos do Sol.'
            },
            302: {
                title: 'De revesgueio!',
                description: '1. Próximo, não exatamente onde deveria acertar/estar.\n    Exemplo: "Essa gripe me pegou, mas de revesgueio".\n2. Olhar de revesgueio é olhar atravessado, meio de lado, de canto de olho.\n    Exemplo: "Ele a olhou de revesgueio".'
            },
            303: {
                title: 'Debandiar',
                description: 'Corruptela de "debandar". Sair, deixar um lugar.'
            },
            304: {
                title: 'Debocheira',
                description: 'Grande troça ou deboche; zombaria, deboche.'
            },
            305: {
                title: 'Degolado',
                description: 'Diz-se do animal de pescoço muito fino no local da junção com a cabeça.'
            },
            306: {
                title: 'Deitar o cabelo',
                description: 'Se mandar, sair correndo...'
            },
            307: {
                title: 'Dentama',
                description: 'Grande quantidade de dentes. Dentadura toda igual.'
            },
            308: {
                title: 'Desacorçoar',
                description: 'Desanimar, desistir.'
            },
            309: {
                title: 'Desatinado',
                description: 'Enlouquecido, desesperado. \n\n*Contribuição do Alfabeto de Quaraí. \n'
            },
            310: {
                title: 'Desbocado',
                description: 'Diz-se do cavalo muito sensível da boca, que com a mais leve pressão do freio, levanta bruscamente a cabeça, não governando bem. Aplica-se também às pessoas que tem o hábito de dizer palavras obscenas ou menos delicadas.'
            },
            311: {
                title: 'Descambada',
                description: 'Declive, descida de uma coxilha ou lomba para uma quebrada ou vale. O mesmo que descambado.'
            },
            312: {
                title: 'Desembarrigar',
                description: 'Ficar delgado, magro, fazer desaparecer o grande volume da barriga.'
            },
            313: {
                title: 'Desembuchar',
                description: 'Confessar segredos, contar tudo o que sabe.'
            },
            314: {
                title: 'Desenchufado',
                description: 'Desligado; Desconectado;\nEx: Ando meio Desenchufado ultimamente; '
            },
            315: {
                title: 'Desgarrar',
                description: 'Retirar as garras (pontas) do couro. Também significa apartar-se dos outros, perder-se dos companheiros.'
            },
            316: {
                title: 'desmaiar o Batista',
                description: 'A expressão "de desmaiar o Batista" é utilizada quando queremos dizer que algo está exagerado, causando mal estar. Por exemplo: "Mas tá calor de desmaiar o Batista!".\n\nA origem da expressão vêm do dia em que o Batista, comentarista da TV COM (rede de comunicação do RS) desmaiou ao vivo em um jogo: http://www.youtube.com/watch?v=Zd7WBUj8PAs'
            },
            317: {
                title: 'Desmereceu',
                description: 'Desmerecer é quando perde a cor, o mesmo que desbotar\n'
            },
            318: {
                title: 'Despacito',
                description: 'adv.\nSem pressa; lentamente; com parcimônia. (ex.: Não \'se apressemo\'. despacito, chegamos lá.)'
            },
            319: {
                title: 'Destornilhado',
                description: 'Diz-se daquele que não tem o juízo muito certo.'
            },
            320: {
                title: 'Deus o livre',
                description: 'Expressão de alívio (Deus o livre, ainda bem que não choveu hoje). '
            },
            321: {
                title: 'Devereda',
                description: 'Rápido, ligeiro.\nEx: Quando ele recebeu a notícia saiu devereda resolver o problema.'
            },
            322: {
                title: 'Diabrete',
                description: 'Criança muito arteira.'
            },
            323: {
                title: 'Diacho',
                description: 'Diabo. “É o dicaho”, ou seja, é o diabo.'
            },
            324: {
                title: 'Dinheirama',
                description: 'Muito dinheiro. O mesmo que dinheiral. “Dei uma dinheirama por essa tropa”, ou seja, essa tropa custou muito dinheiro.'
            },
            325: {
                title: 'Ditas',
                description: 'Hemorróidas.'
            },
            326: {
                title: 'Do tempo do Ariri de gancho',
                description: 'Muito antigo, fora de moda, ultrapassado...'
            },
            327: {
                title: 'Doma',
                description: 'Ato de domar. Ato de amansar um animal xucro.'
            },
            328: {
                title: 'Dormida',
                description: 'Sono. Pouso no fim da jornada.'
            },
            329: {
                title: 'Dos tempos do guaraná de rolha',
                description: 'Algo muito antigo, dos idos de antigamente.'
            },
            330: {
                title: 'É de comer ou passar no pão?',
                description: 'Quando a pessoa não entende nada de nada. Não entende de determinado assunto ou da informação que lhe foi passada. '
            },
            331: {
                title: 'É mais feio que galope de vaca!',
                description: 'A vaca, ao contrário do cavalo, não sabe galopar. Quando por qualquer motivo, precisa correr, não é garbosa, é desajeitada, meio descordenada.'
            },
            332: {
                title: 'Eita!',
                description: 'Interjeição de espanto.'
            },
            333: {
                title: 'Em cima do laço',
                description: 'Com pressa, apressado, com pouco tempo\nEx: Estou em cima do laço pra chegar na reunião.'
            },
            334: {
                title: 'Embelecos',
                description: 'Coisas de pouco valor, bugigangas.'
            },
            335: {
                title: 'Emborcar',
                description: 'Cair de borco, virar a canoa ou o veículo, quando a parte superior fica para baixo.'
            },
            336: {
                title: 'Embretar',
                description: 'Apertar, pressionar, deixar sem escapatória.'
            },
            337: {
                title: 'Embromar',
                description: 'v. Levar muito tempo para fazer algo. Andar devagar, sem pressa.'
            },
            338: {
                title: 'Embrulhão',
                description: 'Diz-se de ou o indivíduo que causa confusão, embaraço, atrapalhação, complicação.'
            },
            339: {
                title: 'Empacado',
                description: 'Calado, sério, taciturno.'
            },
            340: {
                title: 'Empilchado',
                description: 'Diz-se do que tem pilchas. Rico, que tem bons haveres.'
            },
            341: {
                title: 'Empipocar',
                description: 'Criar pústulas, bolhas ou borbulhas. Também significa estalar, arrebentar. O mesmo que espipocar, pipocar, papocar, espocar.'
            },
            342: {
                title: 'encalistrado',
                description: 'constrangido, desconfiado.\n\n"Mais encalistrado que galinha chocando ovo de avestruz"'
            },
            343: {
                title: 'Encarangar',
                description: 'Ficar enregelado a ponto de ficarem hirtas as mãos e os dedos.'
            },
            344: {
                title: 'Encasquetar',
                description: 'Obstinar-se em realizar alguma coisa. Meter uma ideia fixa na cabeça.'
            },
            345: {
                title: 'Encher de osso',
                description: 'Xingar alguém ou ser xingado. "Tchê! O vivente me encheu de osso!"'
            },
            346: {
                title: 'Encher o buxo',
                description: 'Comer demasiadamente. Um pouco além do recomendado.'
            },
            347: {
                title: 'Encherido(a)',
                description: 'Pessoa com ar de superioridade!'
            },
            348: {
                title: 'Enchufar',
                description: 'Plugar. Do Espanhol \'enchufe\'.'
            },
            349: {
                title: 'Encordoado',
                description: 'Consecutivos, em sequência.'
            },
            350: {
                title: 'Encostelar',
                description: 'Emparelhar, ficar rente com outrem, encostar-se a outrem, andar junto.'
            },
            351: {
                title: 'Endomingar-se',
                description: 'Vestir-se bem, com roupa de passeio.'
            },
            352: {
                title: 'Enfastiado',
                description: 'Sem fome, embuchado, de barriga cheia. \n\n*Contribuição do Alfabeto de Quaraí'
            },
            353: {
                title: 'Enforquilhar-se',
                description: 'Prender na forquilha. Em sentido, figurado, assentar-se mal, com deselegância, quando a cavalo.'
            },
            354: {
                title: 'ENGASGA GATO',
                description: 'Carne moída ou picada em tamanho grande ou ensopado com pedaços de charque. Geralmente usa-se os restos do churrasco.\nEx: Hoje a noite vai ter um engasga gato lá em casa.'
            },
            355: {
                title: 'Engatado',
                description: 'Apaixonado, louco pela guria. \n\n*Contribuição do Alfabeto de Quaraí'
            },
            356: {
                title: 'Engate',
                description: '1 - Hora extra. 2 - Ato de estar engatado.\n\nEx. 1: "Vou chegar mais tarde porque estou engatado aqui no serviço"\n\nEx. 2: "Trabalho entrando a essa hora? Certo que é engate" '
            },
            357: {
                title: 'Engrólio',
                description: 'Trapaça, embrulho, embrulhada.'
            },
            358: {
                title: 'Entecada',
                description: 'Doente, com problema de saude'
            },
            359: {
                title: 'Enticar',
                description: 'en.ti.car, transitivo indireto e pronominal\n(Regionalismo) provocar questão, entrar em discussão com; altercar, brigar\nTem a mania de enticar com todo mundo.\nEnticam-se diariamente sobre questões de trabalho.\n(Regionalismo) tomar implicância com (alguém ou algo); aborrecer\nResolveu enticar com o vizinho por causa de um muro.\nEnticou com o vestido assim que o viu.\n(Regionalismo) obstinar-se em; teimar, rabujar\nQuando ele (se) entica e resolve sair, não tem jeito.'
            },
            360: {
                title: 'entrevero',
                description: '1. Confusão, mistura, briga, desentendimento. 2. Espécie de sanduíche recheado com diversos tipos de carnes picadas.'
            },
            361: {
                title: 'Entrouchado',
                description: 'Ato de entrouchar-se. Traduzindo, é o cara que sai bem agasalhado de casa, pronto pra enfrentar o frio de renguiar cusco que costuma fazer no Rio Grande.'
            },
            362: {
                title: 'Epucha!',
                description: 'Expressão de admiração.'
            },
            363: {
                title: 'Ermão',
                description: 'O mesmo que irmão. É a maneira mais comum que se exprime a gente rústica da campanha.'
            },
            364: {
                title: 'Escangalhado',
                description: 'Em estado lamentável. Esbodegado.'
            },
            365: {
                title: 'Escangalhar',
                description: 'Estragar, destruir.'
            },
            366: {
                title: 'Escramuçar',
                description: 'Salto dado pelo cavalo, normalmente quando está sendo montado e sob pressão. Ex.: "Meu cavalo escramuça, e até parece que pulsa sangue crioulo em minhas veias!"'
            },
            367: {
                title: 'Esgualepada',
                description: 'Destroçada, destruída, demolida, bagunçada'
            },
            368: {
                title: 'Esgualepado',
                description: 'Pessoa ou animal mal tratado ou machucado. Cansado ao extremo. (Físico ou emocionalmente)\n\nJoão voltou todo esgualepado da fazenda.\nO cusco velho já tá esgualepado.\n\n'
            },
            369: {
                title: 'esgualepado (a)',
                description: 'Alguém cansado...'
            },
            370: {
                title: 'Eslaque',
                description: 'Usado pelas mulheres, é a mesma coisa que calça comprida. Derivado de slack'
            },
            371: {
                title: 'Esmerar',
                description: 'Caprichar, fazer bem feito, '
            },
            372: {
                title: 'Esparramo',
                description: 'Separação, disseminação de objetos, de animais ou de pessoas. O mesmo que esparrame.'
            },
            373: {
                title: 'Esquaxerengado',
                description: 'Adjetivo que se refere a quem está quebrado, estropiado, manco ou rengo das pernas.'
            },
            374: {
                title: 'Estar com um pé que é um leque',
                description: 'Ser uma pessoa que sai muito, que não para em casa'
            },
            375: {
                title: 'esticada no matambre',
                description: 'Quando um vivente está muito cansado, e vai dormir um pouco!'
            },
            376: {
                title: 'Estilingue',
                description: 'Querido entre a piazada, o estilingue é um instrumento feito geralmente com um pedaço de galho de goiabeira bifurcado, elástico e uma tira de couro. O objetivo é lançar pedras em um alvo, seja ele qual for.\n\nTambém conhecido como bodoque. '
            },
            377: {
                title: 'Estropiado',
                description: 'Diz-se o animal sentido dos cascos, com dificuldade de andar, em conseqüência de marchas por estradas pedregosas.'
            },
            378: {
                title: 'Esturricar',
                description: 'Secar demais, ficar muito tempo no sol. Ex.: aquela roupa que ta secando lá fora no sol vai esturricar se você não for logo lá recolher.'
            },
            379: {
                title: 'Facada',
                description: 'Pedido de dinheiro feito por indivíduo vadio, que é incapaz de trabalhar, que não pretende restituí-lo.'
            },
            380: {
                title: 'Faceiro',
                description: 'alegre, contente'
            },
            381: {
                title: 'Faceiro que nem gordo de camiseta.',
                description: 'Uma pessoa que está muito feliz com algo, afinal é difícil um gordo entrar em uma camiseta. '
            },
            382: {
                title: 'Faixa',
                description: 'Estrada, Rodovia'
            },
            383: {
                title: 'Falquejado',
                description: 'Significado de Falquejar. v.t. Desbastar uma madeira (tronco). Ex: Eu aprendi na escola do mundo não fui falquejado em bancos colegiais.'
            },
            384: {
                title: 'Faniquito',
                description: 'Surto psicótico; distúrbio mental; " A tia Ponciana teve um faniquito quando soube que a Juraci fugiu com o Bonifácio"'
            },
            385: {
                title: 'Fanisco',
                description: 'Pessoa de estatura pequena e magra.'
            },
            386: {
                title: 'Farrear',
                description: 'Sair à pândega, à folia, à troça, para divertir-se, passear, beber.'
            },
            387: {
                title: 'Fatiota',
                description: 'Terno; Conjunto de roupas do homem: calça, colete e paletó.'
            },
            388: {
                title: 'Faz a frente',
                description: 'O mesmo que "Faz a mão". Quer dizer: Pô fulano, dá um jeito aí.  Também pode ser: Vai, toma a iniciativa.'
            },
            389: {
                title: 'Fazer os lados',
                description: 'Quando o amigo está afim de alguém que tu conhece e quer uma ajuda, ele pede para que tu "faça os lados".'
            },
            390: {
                title: 'Fechou o pastel',
                description: 'Combinar algo, concordar com algo. Ex.: Vamos tomar um mate lá em casa? -Vamos sim. -Entao fechou o pastel.\n'
            },
            391: {
                title: 'Fechou o pau',
                description: 'Quer dizer que aconteceu uma briga, uma confusão, um tumulto. Pode ser também "fechou a rosca", ou ainda "fechou o tempo".'
            },
            392: {
                title: 'fedelho',
                description: 'Moleque, pentelho. Pessoa que parece uma criança pentelha.\nExemplo: Foi aquele fedelho que vi hoje no parque atrapalhando as outras crianças.\n'
            },
            393: {
                title: 'Fervo',
                description: 'Luta, tumulto, grande conflito.'
            },
            394: {
                title: 'Fiambre',
                description: 'Carne fria assada ou cozida na panela, que se leva para comer em viagem.'
            },
            395: {
                title: 'Fiasqueira',
                description: 'Pessoa que briga em publico, que vergonha em algum lugar.\n\n'
            },
            396: {
                title: 'Fiasquento',
                description: 'Aquele que faz fiasco. Desastrado. Geralmente quando a pessoa faz algo que chame atenção dos outros, como gritar ou desastrar-se em público. '
            },
            397: {
                title: 'Ficar na moita',
                description: 'Não se manifestar, ficar de fora para ver no que vai dar...'
            },
            398: {
                title: 'Finar de rir',
                description: 'Rir até perder o fôlego. Ex: Fulano se finou de tanto rir.'
            },
            399: {
                title: 'Findi',
                description: 'Forma apocopada para fim-de-semana. "Nesse findi eu vou pra Capão da Canoa".'
            },
            400: {
                title: 'Finesse',
                description: 'Sutileza, astúcia. '
            },
            401: {
                title: 'Fiquei com cara de tacho!',
                description: '...'
            },
            402: {
                title: 'Firme, forte e são de lombo',
                description: 'Expressão usada como resposta quando perguntam: Tudo bem contigo?\nE significa: Tudo bem!'
            },
            403: {
                title: 'Firula',
                description: 'Frescura, mimimi, enfeite demais.'
            },
            404: {
                title: 'Flaco',
                description: 'Fraco, magro, desnutrido.'
            },
            405: {
                title: 'Flete',
                description: 'Montaria, cavalo. Termo empregado também nos países platinos.'
            },
            406: {
                title: 'Floreado',
                description: 'Embriagado, tonto, perturbado; a meia embriaguez.'
            },
            407: {
                title: 'Foguete',
                description: 'Diz-se da criança arteira, traquinas, etc.'
            },
            408: {
                title: 'Forrobodó',
                description: 'Desordem, bochinche, baile popular.'
            },
            409: {
                title: 'Frege',
                description: 'Conflito, bagunça, briga. Também significa hotel ou pensão muito ruim.'
            },
            410: {
                title: 'Fresquear',
                description: 'Ato de abster-se de modo acovardado; desistir por razões sentimentais; fazer birra ou manha ao não continuar ou não prosseguir com a realização de algo ou de alguma coisa em específico.\n\nExemplos: \n1. "Não te fresqueia, guria!" \n2. "Só porque começou a cair uma chuvinha, o guri já se fresqueou e não quis sair de casa."\n'
            },
            411: {
                title: 'Friaca',
                description: 'Sinônimo de frio. \n"Bah, mas que friaca!" '
            },
            412: {
                title: 'Frio de renguear cusco!',
                description: 'Expressão utilizada quando esta muito frio e forma-se a geada, então os cachorros fazem uma especie de revezamento com as patas para andar. '
            },
            413: {
                title: 'Fuçar',
                description: 'Procurar. \n\n\'\'vou fuçar(procurar) na internet até encontrar\'\''
            },
            414: {
                title: 'Fuleiro (a)',
                description: 'De péssima qualidade: "esse baile tá muito fuleiro".'
            },
            415: {
                title: 'Fulistrica',
                description: 'Pessoa insignificante, pessoa sem mérito.'
            },
            416: {
                title: 'Funda',
                description: 'estilingue, bodoque, brinquedo de criança, principalmente do interior do estado, '
            },
            417: {
                title: 'Furungar',
                description: 'Insistir enfadonhamente a respeito de um assunto. Intrigar com mexericos insistentes.'
            },
            418: {
                title: 'Futriquinha',
                description: 'Coisa velha, sem valor.'
            },
            419: {
                title: 'Fuxicar',
                description: 'Coser com grandes pontos, sem muito cuidado, pano ou roupa. Também significa bulir, remexer em qualquer coisa. Intrigar, enredar, furungar.'
            },
            420: {
                title: 'Fuzarca',
                description: 'Confusão; aglomeração de pessoas envolvidas em brincadeiras, brigas ou sacanagens; "botaram fogo na patente do Amâncio e dizem que o Pedro tava metido na fuzarca";'
            },
            421: {
                title: 'Gadeia',
                description: 'Cabelo. ex.: penteei as gadeia! Tá na hora de cortar essas gadeia!'
            },
            422: {
                title: 'Gaitada',
                description: 'Grande risada, gargalhada.'
            },
            423: {
                title: 'Galgo',
                description: 'O mesmo que louco de fome, comilão, usado na década de 1960 a 1970'
            },
            424: {
                title: 'Galo Cinza',
                description: 'Similar a expressão Galo Veio. Amigo de Longa Data. Parceiro em Indiadas'
            },
            425: {
                title: 'Galo Véio',
                description: 'Grande Amigo; Parceiro de Indiadas'
            },
            426: {
                title: 'Garantonhas',
                description: 'Diz-se do aspecto do tempo, quando se apresenta feio ou ameaçador de chuva.'
            },
            427: {
                title: 'Garganta',
                description: '[adj.] Quem gosta de se exaltar através de histórias inventadas ou aumentadas. Mentiroso. Falastrão. (ex.: Não acredito em ti, tu és muito garganta.) 2. [adj.] Quem vive ameaçando ou prometendo sem cumprir. (ex.: Ele vive prometendo a prenda em casamento mas nunca casa. É um garganta mesmo.)'
            },
            428: {
                title: 'Garralho',
                description: 'Bezerro(a) de qualidade inferior, tanto de raça como de constituição física.'
            },
            429: {
                title: 'Gastando a nega',
                description: '1. Fazendo algo muito bem\n'
            },
            430: {
                title: 'Gastura',
                description: 'Azia, mal do estômago, enfarte.'
            },
            431: {
                title: 'Gato muito miador, não é bom caçador',
                description: 'Quem fala muito, mas não cumpre tudo o que anuncia...'
            },
            432: {
                title: 'Gaudério',
                description: '1. Sinônimo de gaúcho.\n2. Sujeito alegre. \n3. Regionalismo gauchesco e da região serrana catarinense para o tipo humano, morador do campo, de hábitos rudes, errante, que acompanha qualquer pessoa até que a deixe para seguir outra.'
            },
            433: {
                title: 'Gavola',
                description: 'Denominação ao gaúcho que exalta exacerbadamente seus feitos, qualidades; gabar-se; vangloriar-se. [Ex.: Sirvam nossas façanhas de modelo a toda Terra.] #quasenunca'
            },
            434: {
                title: 'Gazear',
                description: 'Faltar propositalmente à aula, ou mentir que vai ao colégio e ir pra outro rumo sem os pais saberem. Gíria antiga muito usada nos colégios gaúchos e pouco usada no Brasil hoje em dia.'
            },
            435: {
                title: 'Gentarada',
                description: 'Gentama, gentaria, multidão.'
            },
            436: {
                title: 'Gente fina',
                description: 'Diz-se de indivíduo com qualidades e virtudes. Amigo, parceiro, "dus  meu".'
            },
            437: {
                title: 'Ginete',
                description: 'Pessoa que monta bem, com firmeza e com garbo. Bom cavaleiro domador. Também significa o vulto de um homem a cavalo.'
            },
            438: {
                title: 'Godô',
                description: 'Artimanha, despiste, desculpa esfarrapada. "O fulano me deu um godô".'
            },
            439: {
                title: 'Goleira',
                description: '[s.f.] Trave de futebol.'
            },
            440: {
                title: 'Gordacho',
                description: 'Gorducho, muito gordo, gordíssimo.'
            },
            441: {
                title: 'Grandote',
                description: 'Diz-se do menino já grande, ou do animal muito novo, já de tamanho regular.'
            },
            442: {
                title: 'Grauzento (a)',
                description: 'Esta palavra em nosso vocabulário se refere a uma pessoa que quer ser poderosa, maioral. Resumindo, se acha o bom (a).'
            },
            443: {
                title: 'Gravatá',
                description: 'Cactácea da familia Bromeliae, que se espalham pelos campos, tem espinhos finos que machucam quem anda sem calçados. '
            },
            444: {
                title: 'Graxaim',
                description: 'O graxaim ou sorro (Pseudalopex gymnocercus) é um mamífero carnívoro da família dos canídeos, encontrado nos campos úmidos do Sul do Brasil, chega a medir até 1 metro de comprimento, com pelagem cinza amarelada, o alto da cabeça marrom ferrugíneo, orelhas grandes e focinho afilado. Também é conhecido pelos nomes de graxaim-do-campo, guaraxaim e sorro.'
            },
            445: {
                title: 'Graxeira',
                description: 'Panela grande ou caldeirão onde se fervem os ossos e se derrete o sebo da rês, para extrair a graxa. Também significa o lugar onde é instalado esse caldeirão nas charqueadas ou nas estâncias.'
            },
            446: {
                title: 'Gringada',
                description: 'Grupo de pessoas de origem italiana ou alemã, geralmente originarias da região da Serra Gaúcha. Similar ao termo "alemoada" para o grupo de pessoas de descendência alemã.'
            },
            447: {
                title: 'Grude',
                description: 'Namoro, derriço.'
            },
            448: {
                title: 'Guaiaca',
                description: 'carteira de dinheiro e documentos do gaucho'
            },
            449: {
                title: 'guaipeca',
                description: 'Sinônimo: cusco, cachorro'
            },
            450: {
                title: 'guampa',
                description: 'chifre, corno'
            },
            451: {
                title: 'Guapo',
                description: 'Forte, vigoroso, valente, bravo.'
            },
            452: {
                title: 'Guasca',
                description: '1. Homem rústico, forte, guapo, valente.\n\n2. Tira, corda de couro cru, isto é, não curtido.\n\n3. Pênis.'
            },
            453: {
                title: 'Guaxa',
                description: 'Rebenque curto e grosso usado para doma.'
            },
            454: {
                title: 'Guenzo',
                description: 'Fora de prumo, que foi tirado de sua posição normal, inclinado, torto, pendido para um lado, inseguro, bamboleante.'
            },
            455: {
                title: 'Guincha',
                description: 'Poldra, potranca, égua nova, gueixa. Também significa mulher despudorada.'
            },
            456: {
                title: 'Guisado',
                description: 'Carne moída refogada'
            },
            457: {
                title: 'Guri de Apartamento',
                description: 'Utiliza-se quando queremos nos referir a alguém que não tem muita experiência, criado sempre dentro de casa (ou pela avó).  '
            },
            458: {
                title: 'guria',
                description: 'sinonimo de menina ,feminino de guri '
            },
            459: {
                title: 'Guria do céu',
                description: 'quando se está abismado com alguma coisa, por exemplo: Guria do céu, o que aconteceu contigo?'
            },
            460: {
                title: 'Gurizada',
                description: 'Rapazio, meninada, muchachada, gurizeiro.'
            },
            461: {
                title: 'Gurizear',
                description: '1. Ato de "dar de guri"\n2. Infantilidade\n3. "Deixar alguém na mão"'
            },
            462: {
                title: 'Haedo',
                description: 'Coxilha localizada no sul do município de Livramento.'
            },
            463: {
                title: 'Haraganear',
                description: 'O animal que anda solto por muito tempo, sem prestar serviço algum tornando-se arisco. Também se aplica, em sentido figurado, às pessoas, significando vadiar, gauderiar, vagabundear, andar sem ocupação, passear de um lado para outro sem procurar serviço.'
            },
            464: {
                title: 'Hechor',
                description: 'Asno ou burro que serve de garanhão para fecundar as éguas, a fim de promover a hibridação de que resulta o gado muar, isto é, as mulas.'
            },
            465: {
                title: 'Hep',
                description: 'Usa-se no campo para excitar os animais a andarem.'
            },
            466: {
                title: 'Heréu',
                description: 'Herdeiro, dono, proprietário.'
            },
            467: {
                title: 'Hom',
                description: 'Interjeição ‘Hum!’'
            },
            468: {
                title: 'Horneiro',
                description: 'O mesmo que João-de-Barro.'
            },
            469: {
                title: 'Horteleiro',
                description: 'Hortelão.'
            },
            470: {
                title: 'Hosco',
                description: 'O mesmo que osco.'
            },
            471: {
                title: 'Hospe',
                description: 'Abreviação de hóspede.'
            },
            472: {
                title: 'Ilex paraguariensis',
                description: 'Nome científico da erva mate.'
            },
            473: {
                title: 'Incherido',
                description: 'Presumido, ganjento, convencido, metido.'
            },
            474: {
                title: 'indiada',
                description: 'Grande quantidade de homens do campo.\nMas nós gaúchos usamos mesmo a expressão: " Fizemos uma indiada!" Que quer dizer: fizemos um programa de índio.'
            },
            475: {
                title: 'Índio véio',
                description: 'gaúcho de fibra '
            },
            476: {
                title: 'Índio velho',
                description: '[s.m.] Forma de tratamento utilizada entre pessoas com qualquer nível de conhecimento. Também pode ser utilizada na forma reduzida, apenas como "índio". (ex.: E aí, índio velho, como como é que tu tá?)\n'
            },
            477: {
                title: 'Infernizar',
                description: 'Aborrecer, impacientar, arreliar, enfadar, incomodar, apoquentar, amargurar.'
            },
            478: {
                title: 'Inflador',
                description: 'Bomba de encher bola, pneu, etc.'
            },
            479: {
                title: 'Inhapa',
                description: 'Gorjeta. \n\n*Contribuição do Alfabeto de Quaraí'
            },
            480: {
                title: 'Inhato',
                description: 'Que tem nariz curto e arrebitado; que tem nariz curto e arrebitado; que tem nariz chato.'
            },
            481: {
                title: 'Inhé',
                description: 'Onomatopeia designativa da voz dos sapos e das rãs.'
            },
            482: {
                title: 'Instrovenga',
                description: 'Instrumento ou máquina, de pouco valor.'
            },
            483: {
                title: 'Intanguida, encarangada',
                description: 'Quem sente muito frio'
            },
            484: {
                title: 'Inté',
                description: 'Até. Também significa “até logo”, “até outra vista”.'
            },
            485: {
                title: 'Interter',
                description: 'O mesmo que entreter. '
            },
            486: {
                title: 'Inticá!',
                description: 'Significado: Provocar alguém. Pegar no pé'
            },
            487: {
                title: 'Inticar',
                description: 'Provocar, desafiar, mexer com alguém.'
            },
            488: {
                title: 'Ir aos pés',
                description: 'Defecar, realizar suas necessidades fisiológicas.'
            },
            489: {
                title: 'Isonero',
                description: 'Desassossegado, sem descanso.'
            },
            490: {
                title: 'isqueiro',
                description: 'O ânus; "tá com coceira no isqueiro": aquele que não  para quieto.'
            },
            491: {
                title: 'Isso é falta de laço!',
                description: 'Ampla utilização, desde um chilique de guria até um desfalque no caixa da cooperativa. '
            },
            492: {
                title: 'Ja tah se passando!',
                description: 'Passar dos limites, ofender.'
            },
            493: {
                title: 'Jacu',
                description: '1. Sujeito pouco inteligente, besta, sem noção.\n2. Ave galiforme da família dos cracídeos que alimentam-se de frutas, folhas e brotos.'
            },
            494: {
                title: 'Jaguara',
                description: 'Pejorativo. sem crédito, adpto da lei do menor esforço...'
            },
            495: {
                title: 'Jaguatirica',
                description: 'Carnívoro felídeo, também chamado de maracajá e gato-do-mato-grande.'
            },
            496: {
                title: 'Japona',
                description: 'Casaco de lã ou outro tecido de grande poder de retenção de calor, usado em tempos de frio glacial como há nos Campos de Cima da Serra.'
            },
            497: {
                title: 'Jardineira',
                description: 'Carro de quatro rodas, puxado por cavalos, muito usado nas estâncias. Também significa dança antiga.'
            },
            498: {
                title: 'Jeta',
                description: 'Azar, má sorte.'
            },
            499: {
                title: 'Jiboiar',
                description: 'Ter um momento de descanso após a refeição. Leva esse nome porque enquanto faz a digestão a pessoa fica meio estirada, como uma jiboia após o almoço. Acontece quando a pessoa come demais.'
            },
            500: {
                title: 'Joaninha',
                description: 'Alfinete de segurança. Também significa jacundá.'
            },
            501: {
                title: 'Jogador',
                description: 'Osso do peito das aves, em forma de forquilha.'
            },
            502: {
                title: 'Jorna',
                description: 'Bebedeira, tomar uma jorna: embriagar-se.'
            },
            503: {
                title: 'Jornadear',
                description: 'Viajar a cavalo, caminhar.'
            },
            504: {
                title: 'Jucão',
                description: 'Mas que piá jucão esse.\nSeu jucão.\nO mesmo que bobo, trouxa.'
            },
            505: {
                title: 'Judiado',
                description: 'Machucado, sem forças, acabado, maltratado. \n"Não judia o teu irmão, guri!'
            },
            506: {
                title: 'Judiar',
                description: 'machucar,beliscar, maltratar\n\n\'\'não judia dele\'\'\n\n\'\'não belisca ele\'\''
            },
            507: {
                title: 'Judiaria',
                description: 'Malvadeza, maus tratos.'
            },
            508: {
                title: 'Junco',
                description: 'Planta com que, em alguns lugares, se preparam os acolchoados dos lombilhos.'
            },
            509: {
                title: 'Junta',
                description: 'Parelha de bois mansos que puxam lado a lado.'
            },
            510: {
                title: 'Kakedo',
                description: 'Pessoa que não vale nada.'
            },
            511: {
                title: 'Lá se foi o boi com a corda',
                description: 'Significa: acabou nao tem mais jeito afinal o boi escapou e levou até a corda junto. Se a corda ficasse, tu poderias até tentar laçar o boi, mas como foi-se o boi com a corda, não tem mais jeito.'
            },
            512: {
                title: 'Lábia',
                description: 'Habilidade na conversa.'
            },
            513: {
                title: 'Lacraia',
                description: 'Espécie de canoa.'
            },
            514: {
                title: 'Lagarteando',
                description: 'Diz-se do sujeito que está ao sol, aquecendo-se. "Hoje tá bom pra pegar um chimas e ir pra Redença, lagartear".'
            },
            515: {
                title: 'Lagartear',
                description: 'Sinônimo de dormir, descansar deitado ou expôr-se ao sol preguiçosamente.\nÉ comum que se coma bergamota ao se lagartear.'
            },
            516: {
                title: 'Lambada',
                description: 'Relhada, laçaço.'
            },
            517: {
                title: 'Lambança',
                description: 'Intriga, embrulhada, briga, bate-boca, enredo.'
            },
            518: {
                title: 'Lambão',
                description: 'Porcalhão, imundo, o que anda com roupas maltrapilhas e sujas.'
            },
            519: {
                title: 'Lambe graxa',
                description: 'Cachorro vira-lata, sem raça definida.\nEx: No meio da churrascada me aparece um lambe graxa esperando um osso cair da mão'
            },
            520: {
                title: 'Lambuja',
                description: 'O que se dá de vantagem nas apostas. O partido concedido ao adversário.'
            },
            521: {
                title: 'Lanchão',
                description: 'Pé muito grande. Aumentativo de lancha.'
            },
            522: {
                title: 'lanhado',
                description: 'arranhado, com a pele riscada....\nentrei naquele mato e saí todo lanhado do meio do espinharedo.'
            },
            523: {
                title: 'Lanhar',
                description: 'Machucar, "ralar". Ex.: "Mas eu vô te lanhá a pau, ô guri!"'
            },
            524: {
                title: 'Lasquiado',
                description: 'Chinelão, enxerido, metido a besta\n\n*Contribuição do Alfabeto de Quaraí'
            },
            525: {
                title: 'Légua de beiço',
                description: 'Quando o lugar é distante!  distância incerta que na maioria das vezes, vale bem mais que uma légua, geralmente associada ao gesto de distender o lábio inferior; Ex. Aquele galpão fica uma légua de beiço!'
            },
            526: {
                title: 'Lenquiar o lombo a laço',
                description: 'Surrar alguém até o ponto de ferir.'
            },
            527: {
                title: 'Levar uma tunda de rabo de tatu',
                description: 'Equivalente a apanhar, ou levar uma camassada de pau.'
            },
            528: {
                title: 'Ligeiro',
                description: 'Nome dado ao indivíduo que viaja a pé pela via férrea.'
            },
            529: {
                title: 'Lindeiro',
                description: 'adj. vizinho de terra, terras que fazem fronteira direta uma com a outra'
            },
            530: {
                title: 'Linguiça',
                description: 'Trapaça.'
            },
            531: {
                title: 'Linterna',
                description: 'Lanterna'
            },
            532: {
                title: 'Livreto',
                description: 'Caderno pequeno para anotações. \n*Contribuição do Alfabeto de Quaraí'
            },
            533: {
                title: 'LIXIGUANA',
                description: 'Pequena abelha, parecida com um marimbondo, que produz mel delicioso. "Bah, tá um frio de tirar LIXIGUANA"'
            },
            534: {
                title: 'Loco',
                description: 'Termo vago para designar sujeito do sexo masculino, sem juízo de valor ou de sua saúde mental. Alternativa: lôco'
            },
            535: {
                title: 'Loco de especial',
                description: 'Macanudo,Muito bom, excelente,gostoso, agradável'
            },
            536: {
                title: 'lomba',
                description: 'Terreno em declive.\n"vou descer a lomba!"'
            },
            537: {
                title: 'Lombeira',
                description: 'Preguiça, modorra, moleza no corpo.'
            },
            538: {
                title: 'Lonca',
                description: 'Pessoa que costuma fazer algo errado ou estranho. Ex: Jõao bateu em um poste - "Mas é um lonca mesmo!!"'
            },
            539: {
                title: 'Lonjura',
                description: 'A imensidade do campo, a distância.'
            },
            540: {
                title: 'Lonquear',
                description: 'Raspar, barbear, raspar o couro da rês com faca\n"Rápido de lonquear rato dormindo."'
            },
            541: {
                title: 'Loqueou',
                description: 'Ficou louco.\nAplicação: Tu até loquoeu, o que estás pensando?'
            },
            542: {
                title: 'Lorota',
                description: 'Conversa sem importância, jactância, mentirosa, pabulagem, fanfarrice, banalidade, mentira, embuste.'
            },
            543: {
                title: 'Loscanha',
                description: 'Diz-se do indivíduo aluado, meio louco.'
            },
            544: {
                title: 'Louco de bom',
                description: 'Quando algo é muito bom.'
            },
            545: {
                title: 'Louco de especial',
                description: 'Quando algo é muito especial.'
            },
            546: {
                title: 'Má Tchê',
                description: 'Usado em momentos de indignação. Ex.: má tchê, que que tu ta fazendo parado aí ainda.'
            },
            547: {
                title: 'MACANUDO',
                description: 'Muito bom. Ótimo'
            },
            548: {
                title: 'Maçaroca',
                description: 'Intriga, mexerico.'
            },
            549: {
                title: 'Macega',
                description: 'Arbusto rasteiro que viceja, em geral, nos campos de má qualidade. Pastagem. Capim alto.'
            },
            550: {
                title: 'Machucão',
                description: 'Pisadura; contusão.'
            },
            551: {
                title: 'Magal',
                description: 'Aquele que rebaixa o carro e costuma andar com o vidro do "auto" aberto com o braço para fora ouvindo música alta.\nA Vestimenta típica varia bastante porém sempre apresenta um acessório chamativo como por exemplo: Um belo chapéu ou fivela e, nos urbanos, o maldito boné e correntes no pescoço.\nAs músicas podem variar de acordo com a região: bailão, poperô de rádio e até pagode. Relatos recentes contam que até o "Tecno brega" vem sendo executado pelos magais.'
            },
            552: {
                title: 'Magra lebre',
                description: 'Termo usado para descrever uma situação complicada ou algo que não é satisfatório. Por exemplo: "Só isso? Magra lebre"'
            },
            553: {
                title: 'Magrão',
                description: 'adj.\n1. Pessoa de quem se omite o nome ou lhe é desconhecido. (ex.: Tchê, quem é aquele magrão ali?)\n2. Forma de tratamento que pode ser uitilizada entre pessoas com qualquer nível de conhecimento, inclusive desconhecidas. (ex.: Bah, magrão; me fizeste cair os butiá do bolso.)'
            },
            554: {
                title: 'mais à vontade que bugio em mato de boa fruta',
                description: 'Sentir-se em casa, à vontade. '
            },
            555: {
                title: 'Mais agarrado que carrapato em gado magro',
                description: 'Muito apego, junto, com muita fome.\nEx: Grudado numa ripa de costela. Mais agarrado que carrapato em gado magro...'
            },
            556: {
                title: 'Mais ansiado que anão em comício.',
                description: 'nervoso,inquieto,preocupado .'
            },
            557: {
                title: 'Mais apertado que sardinha em lata',
                description: 'Quando algo está muito cheio, como um ônibus, por exemplo. Quando não cabe mais nada, ninguém.'
            },
            558: {
                title: 'Mais assustado que sapo em cancha de bocha',
                description: 'É uma frase.'
            },
            559: {
                title: 'Mais atravessado que fiofó de lagarto',
                description: 'Coisa mal feita, amadorismo, trabalho mal executado.'
            },
            560: {
                title: 'Mais beijada que anel de bispo',
                description: 'Quando a moça é muito namoradeira. '
            },
            561: {
                title: 'Mais comprido que enterro de rico',
                description: 'Diz-se de um mate muito comprido ou de qualquer coisa muito demorada. '
            },
            562: {
                title: 'Mais curto que coice de porco!',
                description: 'Auto explicativa'
            },
            563: {
                title: 'Mais enrolado que cristal pra viagem',
                description: 'Expressão utilizada no sentido literal, quando algo está enrolado mesmo, fisicamente. '
            },
            564: {
                title: 'Mais estropiado que cavalo de contrabandista',
                description: 'Expressão usada quando o gaúcho está muito doente ou muito cansado!'
            },
            565: {
                title: 'Mais faceiro que bugiu em mato de fruta!',
                description: 'Uma pessoa muito, mas muito feliz! '
            },
            566: {
                title: 'Mais faceiro que vivente embarcando em uma Kombi pro Bororé',
                description: 'Baseada em uma música do Mano Lima, surgiu em uma conversa entre amigos do 3° ano do colégio Farroupilha, após pesquisar, começamos a usar a expressão quando um dos guris falou sobre onde fica o distrito do Bororé, descobrindo que fica distante 470 km aproximadamente da capital da República Riograndense. Adicionamos a Kombi pois é um veículo comum no estado, sendo quase obrigatório em todas as famílias interioranas e também por sua capacidade interna e dirigibilidade.'
            },
            567: {
                title: 'Mais fechado que porta de convento',
                description: 'Pessoa introvertida'
            },
            568: {
                title: 'Mais inútil que cinzeiro em moto',
                description: 'Auto explicativa'
            },
            569: {
                title: 'Mais perdido que cusco em tiroteio',
                description: 'Diz-se de alguém perdido em comparação à desorientação de um cachorro em meio a um tiroteio.'
            },
            570: {
                title: 'Mais perfumado que mão de barbeiro',
                description: 'Pessoa muito perfumada, bem cheirosa.  '
            },
            571: {
                title: 'Mais sujo que pau de galinheiro',
                description: 'Sujeito trapaceiro, larápio, sem vergonha...'
            },
            572: {
                title: 'Malito',
                description: '[adj.] Vem do espanhol "malito", que é o diminutivo de mal. Atribui-se a quem está muito mal, muito debilitado ou em qualquer situação muito ruim. (ex.: Eu trabalhei tanto que estou malito.)'
            },
            573: {
                title: 'Maloqueiro',
                description: 'Aquele que vive em maloca ou em grupo de bandidos. Habitente de maloca.'
            },
            574: {
                title: 'Mamado',
                description: 'Aquele que se encontra embriagado.'
            },
            575: {
                title: 'Manco',
                description: 'Com a perna ou pata dianteira machucada, diferente de rengo, que se refere à pata de trás. '
            },
            576: {
                title: 'Mandinho',
                description: 'Criança pequena. Sinônimo de guri, piá.   '
            },
            577: {
                title: 'Maneta',
                description: 'Diz-se da pessoa que tem falta de um braço.'
            },
            578: {
                title: 'Manga',
                description: '1. Chuva forte e rápida.\n2. Mangueira de plástico, mangueira de jardim.'
            },
            579: {
                title: 'Manguá',
                description: 'Instrumento usado para debulhar feijão feito com uma vara e arames na ponta'
            },
            580: {
                title: 'Manguaça',
                description: 'bebedeira, ficar muito bêbado.\nEx.: ontem ele ficou na maior manguaça'
            },
            581: {
                title: 'Manotaço',
                description: 'Pancada que o cavalo dá com uma das patas dianteiras, ou com ambas.\n\nBofetada, pancada com a mão dada por pessoa. '
            },
            582: {
                title: 'Maomeno',
                description: 'Contração de mais ou menos. Em alguns pontos do estado, se diz "marromeno". '
            },
            583: {
                title: 'Maricas',
                description: 'Homem que se ocupa de trabalhos próprios de mulheres. Homem efeminado. Homem que gosta de intrometer-se em assuntos reservados a mulher'
            },
            584: {
                title: 'Maroto',
                description: 'Mesmo que caborteiro, difícil de domar...'
            },
            585: {
                title: 'Marro',
                description: 'Porrada, Soco, Paulada. ex.: Vou te dar um marro na cara! Dei-lhe um marro nos corno!\n'
            },
            586: {
                title: 'Mas ai sim',
                description: 'espanto, ficar admirado com algo.'
            },
            587: {
                title: 'Mas Daonde',
                description: 'quando não acreditamos em algo, ou duvidamos. é falado bem cada som da sílaba.'
            },
            588: {
                title: 'Mas Nem Te Apresenta!',
                description: 'Quando a pessoa tá com pouca moral, falando algo sem credibilidade.'
            },
            589: {
                title: 'Mascando brim',
                description: 'Com as calça (calção, bermuda, o que for) atolado no rego.'
            },
            590: {
                title: 'mate',
                description: 'bebida resultante da infusão de folhas da Ilex paraguaiensis, nossa conhecida erva mate devidamente preparada, que se toma em cuia com o auxílio de uma bomba ou em taças. '
            },
            591: {
                title: 'matear',
                description: 'Sinônimo de tomar chimarrão.\nEx: Vamos matear na praça?\n'
            },
            592: {
                title: 'Mateira',
                description: 'Espécia de bolsa, feita normalmente de couro, onde se leva os aparatos pra se fazer chimarrão: garrafa térmica, cuia, bomba e a embalagem de erva mate.'
            },
            593: {
                title: 'matreira',
                description: 'que vive no mato, arisca, fugidia, animal que dificilmente se deixa apanhar. Diz-e também de pessoa esquiva, que foge a tudo .'
            },
            594: {
                title: 'Matucho',
                description: 'Gaúcho do Mato Grosso. '
            },
            595: {
                title: 'Matungo',
                description: 'Cavalo vagabundo. É para os cavalos o que o guaipeca é para os cachorros.'
            },
            596: {
                title: 'Maturrengo',
                description: 'Mau cavaleiro.'
            },
            597: {
                title: 'Matusco',
                description: 'Semelhante a "matuto". Adjetivo utilizado para designar uma pessoa desajeitada.'
            },
            598: {
                title: 'Maula',
                description: 'Frouxo, depreciado, sem nenhum valor...'
            },
            599: {
                title: 'Mazah',
                description: '(Também "mazá".) Corruptela de "mas, ah!". Interjeição de satisfação referente a algo, uma pessoa ou situação. Exemplo: "Mazah, galo veio!"\n\n'
            },
            600: {
                title: 'Me abri pra ti',
                description: 'Algo como: " tu é o cara mesmo!". Tirei o chapéu.'
            },
            601: {
                title: 'Me caíram os butiá do bolso!',
                description: 'Expressão que denota espanto, incredulidade.\nOs jurados desclassificaram o declamador porque estava calçando alpargatas? Bah!!! Me caíram os butiá do bolso com essa!!!\n'
            },
            602: {
                title: 'Me caiu a faca da mão!',
                description: 'Ficar sem ação, paralisado. '
            },
            603: {
                title: 'Me caiu as trança!',
                description: 'Fiquei decepcionado!'
            },
            604: {
                title: 'Me caiu os butia do bolso',
                description: 'É uma expressão regionalista típica do Rio Grande do Sul.\nUsa-se quando a pessoa quer dizer que está impressionada, assustada ou estupefada.\nA expressão é usada porque o butiá é uma fruta pequena, pouco maior que uma bola de gude, dando a idéia de que quando se pára de sopetão, eles facilmente caem dos bolsos.\nEx.: \'Quando descobri que até a Samantha participou, me caiu os butiá do bolso!\''
            },
            605: {
                title: 'ME dormi',
                description: 'Dormir além do horário, atrasar-se'
            },
            606: {
                title: 'Me froxa',
                description: 'Quando alguém está te incomodando, se arriando, tu fala: mas me froxa, tchê!'
            },
            607: {
                title: 'Me pisei',
                description: 'Machuquei, me feri'
            },
            608: {
                title: 'Me primeriô',
                description: 'Chegar em primeiro lugar, chegar antes, fazer alguma coisa antes que alguém.'
            },
            609: {
                title: 'Me pula sexta feira',
                description: 'Quando tem um dia especial essa expressão é usada. digamos não vejo a hora desse dia chegar.'
            },
            610: {
                title: 'Me tapei de nojo!',
                description: '"Entrei no elevador e o cidadão, ali, fumando. Bah, mas me tapei de nojo, tchê!"'
            },
            611: {
                title: 'Me tapei de quero-quero',
                description: 'Sair correndo no estilo Usain Bolt.'
            },
            612: {
                title: 'Melena',
                description: 'Cabelo?\nCabelo volumoso e normalmente desajeitado.'
            },
            613: {
                title: 'Menear',
                description: 'Dar golpes com a mão. Executar qualquer coisa com as mãos. Manejar.'
            },
            614: {
                title: 'Mensú',
                description: 'Indio colono, de fora, não habituado com a cidade'
            },
            615: {
                title: 'Mequetrefe',
                description: 'Diz-se do indivíduo vagabundo, tratante, capadócio, acanalhado, fanfarrão.'
            },
            616: {
                title: 'Mercadinho',
                description: 'Pequena casa de comércio onde se faz venda de frutas e cereais.  O que no Rio de Janeiro e São Paulo se chama de quintanda.'
            },
            617: {
                title: 'Merece',
                description: 'Sinônimo de "de nada" em resposta a "muito obrigada".'
            },
            618: {
                title: 'Merecundência',
                description: 'Falta de respeito, sem explicação'
            },
            619: {
                title: 'Metido a facão sem cabo',
                description: 'Parece ser valente, mas na verdade não é de nada.'
            },
            620: {
                title: 'Meu Favor!',
                description: 'Meu Deus! Expressão de espanto.'
            },
            621: {
                title: 'Michar',
                description: 'v int.\n1. Desistir.\n2. Desanimar, esmurecer.\n3. Colocar empecílhos.\n4. Declinar ou rejeitar oportunidades e convites.\n(ex. O time não pode se michar; é difícil mas podemos vencer.)'
            },
            622: {
                title: 'Michorna',
                description: 'Confusão, bagunça, coisas viradas do avesso. '
            },
            623: {
                title: 'Migué',
                description: 'Ato de enganar, encobrir, mentir. "O fulano me deu um migué dizendo que vinha hoje, mas até agora não apareceu". '
            },
            624: {
                title: 'Mijada',
                description: 'Chamar atenção de alguém. Ex. O professor deu uma mijada no aluno porque ele tentou colar na prova.'
            },
            625: {
                title: 'Milicada',
                description: 'Grupo de milicos. O mesmo que milicama.'
            },
            626: {
                title: 'Mimosa (o)',
                description: 'Pessoa querida, bonita, agradável\nEx: Que mimosa essa guria!'
            },
            627: {
                title: 'Mingo',
                description: 'Pequeno, menor, mínimo: “o dedo mingo”, o mínimo; “a costela minga”, a menor costela.'
            },
            628: {
                title: 'minguado',
                description: 'desprovido de corpo (substância).'
            },
            629: {
                title: 'Minuano',
                description: 'referência há vento andino; frio e seco, que sopra do sudoeste no inverno.'
            },
            630: {
                title: 'Mirador',
                description: 'Pequena e elevada construção no alto das casas, para gozo de largas perspectivas.  O mesmo que mirante.'
            },
            631: {
                title: 'Mixaria',
                description: 'Algo com pouco valor, que não é de nada.'
            },
            632: {
                title: 'Môcha',
                description: 'Mulher de seios pequenos.'
            },
            633: {
                title: 'Mocó',
                description: 'Local escondido, esconderijo. "Vou ficar no meu mocó hoje."'
            },
            634: {
                title: 'Mocosado',
                description: 'Ficar escondido. "Tô a fim de ficar mocosado"'
            },
            635: {
                title: 'Mondonguinho',
                description: 'Também conhecido como bucho, ou "dobradinha". É exatamente isso, o bucho do boi. Geralmente preparado com molho vermelho e acompanhado por pão colonial pra "potchar" no molho.'
            },
            636: {
                title: 'Monteador',
                description: 'Quem costuma caçar ou até simplesmente quem vive em região montanhosa.'
            },
            637: {
                title: 'Moquear',
                description: 'Esconder, ocultar. '
            },
            638: {
                title: 'Moquiado',
                description: 'Quando o indivíduo se faz de morto. '
            },
            639: {
                title: 'Morocha',
                description: '[adj.] Forma de tratamento utilizada para uma mulher com quem se tem bastante intimidade, como a cônjuge ou companheira. É uma palavra de origem espanhola, que também quer dizer "morena". (ex.: Mas que saudade eu tava de ti, morocha.)'
            },
            640: {
                title: 'Mosquear',
                description: 'Quando o cavalo movimenta a cola para espantar as moscas que o perseguem, ou fazer tal movimento quando é açoitado ou esporeado.'
            },
            641: {
                title: 'mugre',
                description: 'sujeira. Do cast. mugre'
            },
            642: {
                title: 'Muita mão',
                description: 'Desculpa para se negar a fazer algo por ser muito trabalho. "Bah, muita mão ir buscar lenha"\n\nVariação: alta mão. '
            },
            643: {
                title: 'MULITA',
                description: 'Pequeno tatu (Dasypus hybridus) encontrado nos campos do sul do Brasil. "Pra se segurar no barranco, cravou as unha na terra como MULITA cavando buraco"'
            },
            644: {
                title: 'Munaia',
                description: 'Algo grande.'
            },
            645: {
                title: 'mundaréu',
                description: 'muito, bastante'
            },
            646: {
                title: 'Muquiço',
                description: 'Usado no RS pra se referir a alguém sujo, ou a alguma lugar sujo e bagunçado. '
            },
            647: {
                title: 'Muquifo',
                description: 'Lugar sujo, desagradável.\nEx: Aquele hotel era um muquifo.'
            },
            648: {
                title: 'Na capa da gaita',
                description: 'Quando a pessoa está muito cansada. Diz-se também de indivíduo muito velho ou muito debilitado. '
            },
            649: {
                title: 'Na corrida',
                description: 'Ligeiro, depressa, acelerado. \nSignifica que algo deve ser feito muito rápido, sem perda de tempo.\nExemplos: \nVai lá, na corrida.\nVai compra um refri, na corrida.'
            },
            650: {
                title: 'Na manha',
                description: '[prep. + s.f.] Tranquilo; sem compromissos ou preocupações. (ex.: Ontem eu tive muito trabalho, mas hoje eu estou na manha.) 2. Lentamente; devagar; sem se esforçar. (ex.: Vamos na manha, ainda falta muito tempo para a reunião começar.)'
            },
            651: {
                title: 'Na ponta dos cascos',
                description: 'Quando o indivíduo anda sorrateiramente, com todo o cuidado, tentando não fazer barulho. Também é utilizado para designar o indivíduo que está 100%, no melhor de sua forma.'
            },
            652: {
                title: 'Na raiz',
                description: 'Em cheio, na mosca. Ex: deu um tapa na raiz da orelha'
            },
            653: {
                title: 'Naco',
                description: 'Naca. Pedaço de fumo ou de carne.'
            },
            654: {
                title: 'Namorido',
                description: 'Namorado que vive com a namorada, e se comporta como marido.'
            },
            655: {
                title: 'Naniquice',
                description: 'Pequenez, pouca altura.'
            },
            656: {
                title: 'Não ata nem desata.',
                description: 'Indeciso,  pensativo além do comum, enrolador...'
            },
            657: {
                title: 'Não caga nem desocupa a moita',
                description: 'Expressão usada para o sujeito que não se decide. Não vai nem fica. Não anda nem desanda.'
            },
            658: {
                title: 'Não dá nada',
                description: 'Algo como: "não te preocupa, isso não vai nos trazer problemas futuros". Ou simplesmente: "Não esquenta".'
            },
            659: {
                title: 'Não é farinha de fazer hóstia',
                description: 'Indivíduo dado a andar pelos caminhos tortos da vida.'
            },
            660: {
                title: 'Não emprenha nem sai de cima.',
                description: 'Não se decide, fica no meio termo, indeciso,....'
            },
            661: {
                title: 'Não me faz te pegar nojo!',
                description: 'Quando alguém faz algo nada admirável.'
            },
            662: {
                title: 'Não tá morto quem peleia!',
                description: 'Não está morto quem ainda disputa. Enquanto lutamos temos chance de vencer.'
            },
            663: {
                title: 'Não te acadela',
                description: 'Não te bobeia, não te faz de louco!'
            },
            664: {
                title: 'Nascido',
                description: 'Leicenço, furúnculo. Diz-se, por exemplo: “tenho um nascido no braço”. Termo muito usado na campanha.'
            },
            665: {
                title: 'Nazarenas',
                description: 'Esporas grandes. Usa-se também como adjetivo: esporas nazarenas.'
            },
            666: {
                title: 'Negaça',
                description: 'Esconderijo especial construído com ramos de árvore, em forma de círculo, em cujo o meio o caçador se oculta para atirar nos bandos de marrecões ou marrecas que passam voando.'
            },
            667: {
                title: 'Negalhas',
                description: 'Pequena porção, quantidade insignificante.'
            },
            668: {
                title: 'Negão',
                description: '[adj.] Forma de tratamento utilizada entre pessoas com qualquer nível de conhecimento entre si. (ex.: E aí, negão, como é que tu vais?) 2. [adj.] Pessoa de quem se omite o nome ou lhe é desconhecido, independentemente de qualquer alusão a etnia. (ex.: Bah, mas como é mesmo o nome daquele negão que joga de atacante?)'
            },
            669: {
                title: 'negrinho',
                description: 'sinônimo de brigadeiro doce de chocolate muito usado em festas de aniversario'
            },
            670: {
                title: 'Nem que a vaca tussa!',
                description: 'De jeito nenhum, de maneira alguma...'
            },
            671: {
                title: 'Nhonhô',
                description: 'Tratamento familiar dado aos meninos, pelos escravos.'
            },
            672: {
                title: 'Nicada',
                description: 'Batida de uma bola de vidro em outra, conhecido jogo infantil.'
            },
            673: {
                title: 'Niqueleira',
                description: 'Porta-níqueis, bolsinha pequena.'
            },
            674: {
                title: 'Nó cego',
                description: 'Enrolado, mal pagador, lento, molenga, atrapalhado ruim '
            },
            675: {
                title: 'Nó nas costas',
                description: 'Mania; excessivo; mau costume; esquisitice.. Exemplo: O fulano é cheio de nó nas costas! (Fulano é cheio de manias)'
            },
            676: {
                title: 'Nonato',
                description: 'Não nascido. O terneiro tirado do ventre da vaca que foi carneada.'
            },
            677: {
                title: 'Nos pé da égua',
                description: 'Quando a pessoa está passando por uma dificuldade, está mal'
            },
            678: {
                title: 'Nos pés da égua',
                description: 'Diz-se do indivíduo muito cansado...'
            },
            679: {
                title: 'Noviço',
                description: 'Cavalo novo. Pessoa inexperiente.'
            },
            680: {
                title: 'nulidade',
                description: 'Pessoa sem mérito nenhum, índio inútil, sem serventia, quem não presta para nada.'
            },
            681: {
                title: 'Nútria',
                description: 'Ratão do banhado.'
            },
            682: {
                title: 'Nuvem',
                description: 'Velhaco, vivo, esperto, finório, perspicaz, atilado, pouco escrupuloso. Emprega-se também como substantivo: o que andará fazendo esta nuvem?”,  isto é, este sujeito de vida equívoca e pouco limpa.'
            },
            683: {
                title: 'O ninho está para um lado e a galinha canta para o outro.',
                description: 'Quando alguém tenta enganar o outro, dizer que vai pra um lugar, mas na verdade vai pra outro...'
            },
            684: {
                title: 'O que é um peido pra quem tá cagado',
                description: 'Muito usada para justificar uma atitude inesperada para alguém em situação desfavorável, sobretudo  financeiramente.'
            },
            685: {
                title: 'O risco e o fedor',
                description: 'Diz-se da chinoca mui magra, pois está tão magra que parece ter sido reduzida apenas ao "risco e o fedor", referência à vagina (risco) e ao ânus (fedor). Ex.: "Essa guria está que é só o risco e o fedor!", significando que a tal está muito magra. Expressão erroneamente utilizada também para o gênero masculino atualmente por desconhecimento de sua origem destinada exclusivamente ao gênero feminino.'
            },
            686: {
                title: 'Obedeença',
                description: 'Obediência.'
            },
            687: {
                title: 'Obrar',
                description: 'Evacuar, defecar.'
            },
            688: {
                title: 'Obrigação',
                description: 'A família, os filhos. Diz um amigo a outro: “como vai a obrigação?”, isto é, como passa a família?.'
            },
            689: {
                title: 'Ocalípio',
                description: 'O mesmo que eucalipto.'
            },
            690: {
                title: 'Ôche!',
                description: 'Interjeição usada pelos carreteiros para fazer parar a carreta ou diminuir a marcha. É usada também quando se quer acalmar o boi que se quer pegar.'
            },
            691: {
                title: 'Oigalê',
                description: 'exprime admiração, espanto, alegria.\nOigalê tchê porquera!'
            },
            692: {
                title: 'Onça',
                description: 'Moeda antiga, de ouro.'
            },
            693: {
                title: 'Ontonte',
                description: 'Anteontem, antes de ontem.'
            },
            694: {
                title: 'Orear',
                description: 'Expor ao ar ou ao vento qualquer coisa úmida: “orear a roupa, orear o charque fresco”. Secar por meio do vento: “estamos esperando que oreie um pouco o terreno para seguir viagem”.'
            },
            695: {
                title: 'Orelhano',
                description: 'Gado criado solto, sem dono. Sinônimo de gado "alçado" ou "chimarrão". A expressão deve-se ao nome do jesuíta Cristóbal de Mendoza Orellana, que introduziu o gado vacum na América do Sul.\nA expressão foi popularizada pela música nativista de mesmo nome.\n'
            },
            696: {
                title: 'Orelhar',
                description: 'Segurar o animal, cavalar ou muar, que será domado, por uma ou pelas duas relhas, para facilitar que o domador o monte.'
            },
            697: {
                title: 'Oriental',
                description: 'Adjetivo com que denominamos a República do Uruguai, apesar de ficar ao ocidente da nossa. Termo da antiga colônia espanhola.'
            },
            698: {
                title: 'Orina',
                description: 'Urina.'
            },
            699: {
                title: 'Orizícola',
                description: 'Que cultiva arroz: “países orizícolas”.'
            },
            700: {
                title: 'Orre',
                description: 'Interjeição que exprime satisfação por ter acontecido algo de mau a um adversário ou inimigo.'
            },
            701: {
                title: 'Ossamenta',
                description: 'Carcaça, esqueleto. Em sentido figurado, o corpo da pessoa.'
            },
            702: {
                title: 'Ou caga ou desocupa a moita',
                description: 'Decida-se'
            },
            703: {
                title: 'Ovelha',
                description: 'Em sentido figurado, a pessoa má conselheira, a que, por mau exemplo, põe as outras a perder. “Uma ovelha mestra põe o rebanho a perder.”'
            },
            704: {
                title: 'Ovelheiro',
                description: 'Cusco, cão parceiro da lida de campo, pastor de ovelha.'
            },
            705: {
                title: 'Pacau',
                description: 'Jogo de cartas usado na fronteira. Também significa o indivíduo que não possui um dedo. “Bater o pacau” significa morrer.'
            },
            706: {
                title: 'Pacotilha',
                description: 'Quadrilha feita de malfeitores.'
            },
            707: {
                title: 'Pago',
                description: '[s.m.] Lugar ou região com a qual se tem uma ligação afetiva, por ter sido o local de nascença, de criação, por ter-se radicado ou por qualquer outro motivo que o valha. Lar. Pátria. [consulta também: "rincão" e "querência"] (ex.: Amo o Rio Grande do Sul porque esse é o meu pago.)'
            },
            708: {
                title: 'Pai de fogo',
                description: 'Madeira bem grossa colocada no fogo para que ele não apague por um longo tempo.'
            },
            709: {
                title: 'Paisano',
                description: 'Paisano: homem que não é militar, guerreiro civil, diz-se de militar quando anda sem farda! Também do amigo e companheiro o paisano também aquele que é conhecido de longa data, no Rio Grande do Sul. Expressão provavelmente espanhola, usada na banda oriental (Uruguai) . Uma tropa de paisanos passou a fronteira!\n'
            },
            710: {
                title: 'Pala',
                description: '[s.m.] Poncho de pala é o de fazenda mais fina que o de bichará, e com as pontas arredondadas. É mais leve e mais curto, e na campanha é considerado como traje mais decente. (ex.: Vou colocar o meu pala mais bonito pra ir pra bailanta logo mais.) 2. [s.f.] Amostra. Exemplo. Exibição. Também pode ser encontrada na variação "palha" ou "palhinha". (ex.: Se tu és um bom gaiteiro, dá uma pala pra vermos o que tu sabes.)'
            },
            711: {
                title: 'Palanque',
                description: 'Madeira para cerca'
            },
            712: {
                title: 'Palheiro',
                description: 'Cigarro de fumo crioulo, enrolado em palha de milho.'
            },
            713: {
                title: 'Palpitoso',
                description: 'Desejoso.'
            },
            714: {
                title: 'pampa',
                description: 'possui significados diversos. o mais comum é usar pampa para designar as extensas planícies do Rio Grande do Sul, Uruguai e Argentina, cobertas de passagens verdes onde vive o gado vacum e cavalar. '
            },
            715: {
                title: 'Pança',
                description: 'Indivíduo ridículo. Também significa ventre, abdômen.'
            },
            716: {
                title: 'Pandorga',
                description: 'Pipa, Papagaio.'
            },
            717: {
                title: 'Pandulho',
                description: 'Barriga, pança.'
            },
            718: {
                title: 'Pangaré',
                description: 'Cavalo de pelagem indefinida. Não é comum de ser encontrado. \nex: Este cavalo é um baita de um pangaré!\n'
            },
            719: {
                title: 'Papata',
                description: 'Negociata, roubalheira, mamata.'
            },
            720: {
                title: 'Papudo',
                description: 'Balaqueiro, jactancioso, blasonador.\n\nO termo é empregado para insultar, provocar, depreciar, menosprezar outra pessoa, embora esta não tenha papo.'
            },
            721: {
                title: 'Parada',
                description: 'Conhecido também como ponto de ônibus'
            },
            722: {
                title: 'Passar o relho',
                description: 'Dar uma surra.'
            },
            723: {
                title: 'Pataquada',
                description: 'Trejeitos, expressões de ...'
            },
            724: {
                title: 'Pátio',
                description: 'Como sinônimo de quintal, usado na capital. Área ao redor da casa, normalmente calçada e usada para colocar automóvel, ou como área de estar, para cevar um mate em roda de chimarrão.'
            },
            725: {
                title: 'Pau pra toda a obra',
                description: 'Aquele que faz de tudo um pouco...'
            },
            726: {
                title: 'peão',
                description: 'Homem ajustado para fazer o serviço do campo.'
            },
            727: {
                title: 'Pechada',
                description: 'Batida de trânsito'
            },
            728: {
                title: 'Pelando',
                description: '"Tchê, o mate está pelando"\nMuito quente, de queimar a boca.'
            },
            729: {
                title: 'Pelear',
                description: 'Brigar, lutar, combater, pelejar, teimar, disputar.'
            },
            730: {
                title: 'Pelechar',
                description: 'Trocar de pêlo. Mudança estacional do pelame dos animais, que trocam de um pêlo mais grosso e aspero (para maior proteção do frio do inverno) por um pêlo mais fino e sedoso (para conviver com maior facilidade com o calor do verão). Na maioria das espécies (cavalos, bois, cães, gatos....) trocam de peêo entre o inverno e o verão.'
            },
            731: {
                title: 'Pelêgo',
                description: 'Trata-se de um pedaço de lã de carneiro, colocado sobre a sela e preso por uma tira de couro chamada barrigueira, para que não escorregue. Sua função é amaciar o assento do arreio de lida com o gado'
            },
            732: {
                title: 'Peleia',
                description: 'Briga, luta, desavença, disputa.\n'
            },
            733: {
                title: 'peonada',
                description: 'pionada ou pionagem; porção, grande número ou classe de peões; os empregados de uma estância ou casa ou os condutores de uma tropa. '
            },
            734: {
                title: 'Perau',
                description: 'Precipício, barranco'
            },
            735: {
                title: 'Percanta',
                description: 'Moça bonita, apresentável.'
            },
            736: {
                title: 'Perdi o canto',
                description: 'Oriundo de não escutar o canto do galo.\nSinônimo de se atrasar, perder o despertador.'
            },
            737: {
                title: 'Petiço',
                description: 'Cavalo pequeno, curto, baixo.'
            },
            738: {
                title: 'Pexada',
                description: 'Batida, esbarrada. '
            },
            739: {
                title: 'piá',
                description: 'gurí, garoto, menino'
            },
            740: {
                title: 'pialo',
                description: 'tombo, cair no chão'
            },
            741: {
                title: 'Piazada',
                description: 'Ajuntamento de piás. Gurizada, meninada, garotada.'
            },
            742: {
                title: 'Piazedo',
                description: 'Criançada, jovens. Exemplo, extraído do blog http://www.bombachalarga.org/ver_poesia.php?id=139, poema Payador: "...Deixaste nossos xucros corações num alvoroço;\ndeixaste o piazedo com olhos úmidos\ne falquejou os corações das prendas....". '
            },
            743: {
                title: 'Pichurum',
                description: 'Grupo de pessoas que se juntam para realizar um serviço para outra.'
            },
            744: {
                title: 'Picorrucho',
                description: 'Variável em "gauchês" da palavra em desuso no português "pequerrucho", que significa pequeno, de colo. Se usa geralmente de forma carinhosa pra se referir a uma criança pequena.'
            },
            745: {
                title: 'Piguancha',
                description: 'Chininha, chinoca'
            },
            746: {
                title: 'pila',
                description: 'Dinheiro, reais'
            },
            747: {
                title: 'PILETA',
                description: 'local onde se lava roupas, o mesmo que tanque.\n'
            },
            748: {
                title: 'Pinar',
                description: 'Sinônimo de sair. \nPor exemplo, aqui não é um bom lugar, vamos pinar daqui.\n\n'
            },
            749: {
                title: 'Pinchar',
                description: 'Jogar, atirar, arremessar.\nExemplo: Pinchei fora aquelas tranqueras.'
            },
            750: {
                title: 'pingo',
                description: 'é o cavalo do gaucho'
            },
            751: {
                title: 'Pinguancha',
                description: 'Mulher feia.'
            },
            752: {
                title: 'Pinica',
                description: 'O mesmo que bolita ou bolinha de gude.'
            },
            753: {
                title: 'Pior',
                description: 'Significa "é mesmo". "Tem toda razão"'
            },
            754: {
                title: 'pisar',
                description: 'quando era piá ficava correndo atrás de um matungo múi arisco, e não tinha consciência da periculosidade, aí que a vó gritava: "Guri, sai daí senão tu vai se pisar" - sin. machucar, ferir, quebrar, torcer, rasgar'
            },
            755: {
                title: 'PISCA-PISCA',
                description: 'Sinal luminoso intermitente do automóvel, localizado lateralmente às sinaleiras e aos faróis, que indica a direção para a qual o motorista pretende convergir.'
            },
            756: {
                title: 'Pito',
                description: 'Cigarro, fumo, crivo; Verbo: pitar.\nExemplo: pitava que parecia uma chaminé!'
            },
            757: {
                title: 'Plasta',
                description: 'Pessoa lerda, muito devagar, que não desenvolve uma tarefa com prontidão. '
            },
            758: {
                title: 'Pode tirar o cavalo da chuva!',
                description: 'Quando não concorda com o que a outra pessoa está afirmando.'
            },
            759: {
                title: 'Pollo',
                description: 'Sinônimo de ¨frango¨. Como se diz em espanhol e também nessa região. '
            },
            760: {
                title: 'Pombas (pombos)',
                description: '1. Gurias, guris, namorados... \n2. Também é utilizado quando o indivíduo está incomodado com algo, de saco cheio. "Pombas, tchê! Não fizeste o que te pedi."'
            },
            761: {
                title: 'Porca pipa',
                description: 'Espanto, susto.'
            },
            762: {
                title: 'Porco Dio',
                description: 'expressão de raiva, de desapontamento, de decepção, de desacordo.'
            },
            763: {
                title: 'PORCO ZIO!',
                description: '(do italiano; literalmente "Tio Porco") Interjeição de baixo calão usada em momentos de irritação e/ou indignação. "Má me voltou pra casa pinguço, PORCO ZIO!"'
            },
            764: {
                title: 'Porongo',
                description: 'Cuia. Recipiente de madeira, usado para se tomar mate/chimarrão. '
            },
            765: {
                title: 'Potchar',
                description: 'Geralmente usa-se como sinônimo pra "molhar" o pão em algo, passar/mergulhar no molho do fundo do prato, ou até mesmo no café.'
            },
            766: {
                title: 'Potranca',
                description: 'Substantivo feminino. Ser humano do sexo feminino. Mulher gostosa. Muito gostosa. Mulher provocante, de corpo bonito.'
            },
            767: {
                title: 'Pousar',
                description: 'v. intr. e pron.\nHospedar-se ou passar a noite na casa de alguém. (ex.: Chinoca, faz mais um chimarrão que o compadre vai pousar aqui hoje.'
            },
            768: {
                title: 'Pra fora',
                description: 'Fora = Interior. \nEu moro pra fora, moro no interior.'
            },
            769: {
                title: 'Pra tu vê',
                description: 'Expressão usada para indicar a confirmação e/ou surpresa em determinada situação. "A guria me deu um pé na bunda. Pra tu vê como são as coisas".'
            },
            770: {
                title: 'Prenda',
                description: 'Menina bonita, moça, faz o par do gaúcho. Também pode significar presente, dádiva.Qualidade, dom.\nAptidão; conhecimentos; arte; habilidade.'
            },
            771: {
                title: 'Prender fogo',
                description: 'Ato de incendiar alguma coisa.'
            },
            772: {
                title: 'Prender o grito',
                description: 'Significa falar, dizer, avisar, ou seja, soltar o grito quando se precisa de ajuda.'
            },
            773: {
                title: 'Puchero',
                description: 'Iguaria feita com carne (de preferência peito de boi gordo) e vários tipos de hortaliças. Tudo muito bem temperado com vários condimentos...'
            },
            774: {
                title: 'Pudim de trago',
                description: 'Quem bebe muito, que fica alcoolizado com frequência. '
            },
            775: {
                title: 'Pular a cerca',
                description: 'Trair o cônjuge, sair fora das regras conjugais, se engraçar por outra pessoa...'
            },
            776: {
                title: 'Puteada',
                description: 'Dar um, pito, xingar. \nApl. O guri só parou depois de levar uma puteada.'
            },
            777: {
                title: 'Putz Grila!',
                description: 'Expressão usada quando algo da errado, ou expressão de espanto.'
            },
            778: {
                title: 'Quadrados',
                description: 'Bandeja de grande tamanho, normalmente de folha de zinco, utilizada para quarar ou clarear roupa no sol.'
            },
            779: {
                title: 'Quadrilha',
                description: 'Grupo ou lote de animais cavalares de pelos diferentes, que costumam andar juntos, acompanhando a égua-madrinha.'
            },
            780: {
                title: 'Quarador',
                description: 'Bandeja de grande dimensão, normalmente fabricada com folha de zinco, utilizada para clarear roupa ao Sol. \n'
            },
            781: {
                title: 'Quê Borracheira!',
                description: 'Exclamação usada para dizer que estás de porre.. ou se usada no outro dia.. afirmando a tua ressaca.'
            },
            782: {
                title: 'Que Correriu!',
                description: 'Muito movimento, tumulto.'
            },
            783: {
                title: 'Que ratiada!',
                description: 'individuo "ratão" que fez alguma "bragerada". Ex: mas que ratiaaada!'
            },
            784: {
                title: 'Quebra-costela',
                description: 'Expressão comumente utilizada pelos gaúchos como sinônimo de abraço. "Manda um quebra-costelas lá praquele xiru véio!"'
            },
            785: {
                title: 'Quebra mola',
                description: 'Lombada em via pública. '
            },
            786: {
                title: 'Quebreira',
                description: 'Moleza, preguiça, lassidão, lombeira.'
            },
            787: {
                title: 'Queimar o assado',
                description: 'Atrasar - se; dormir demais. Queimei o assado hoje, cheguei meia hora atrasado!'
            },
            788: {
                title: 'Queixada',
                description: 'Variedade de porco do mato existente no Rio Grande do Sul. Também significa maxilar inferior.'
            },
            789: {
                title: 'Quem atolou o burro que puxe a carroça',
                description: 'Expressão usada quando alguém, por trapalhada sua, acaba atrapalhando os outros.'
            },
            790: {
                title: 'Quengo',
                description: 'Homem sabido, astuto.'
            },
            791: {
                title: 'Quentucho',
                description: 'Quente, para espantar o frio, aquecer, de queimar os beiços.\n\nTambém se usa para definir algo que seja interessante, legal, bom: Quentucho!'
            },
            792: {
                title: 'Queque',
                description: 'Pronuncia-se Quéque - Bolo inglês'
            },
            793: {
                title: 'qüera',
                description: 'Homem, gaúcho, gaudério.'
            },
            794: {
                title: 'querência',
                description: 'lugar ou paragem onde o animal assiste de ordinário ao pasto, ou onde foi criado. o mesmo que pago ou lugar; lugar onde alguém nasceu ou se criou. '
            },
            795: {
                title: 'Querudo',
                description: 'Forte, valente, respeitado, temido.'
            },
            796: {
                title: 'Quibebe',
                description: 'Pirão de abóbora.'
            },
            797: {
                title: 'Quilina',
                description: 'Os pelos compridos do cavalo, que ficam na parte superior do seu pescoço. Ex: Meu pai dizia: - segura nas quilinas para não cair...'
            },
            798: {
                title: 'Quimbombô',
                description: 'Quiabo.'
            },
            799: {
                title: 'Quintal',
                description: 'Como sinônimo de pátio, parte dos fundos do terreno da casa, ajardinado ou não, com árvores frutíferas e criação de galinhas em local urbanizado.'
            },
            800: {
                title: 'Quipoqué',
                description: 'Iguaria de feijão partido e cozido com diversos temperos.'
            },
            801: {
                title: 'Quitute',
                description: 'Comida especial, fora do trivial.'
            },
            802: {
                title: 'Rabada',
                description: 'A carne e o osso da parte superior do rabo da rês.'
            },
            803: {
                title: 'Rabioscas',
                description: 'Rabiscos, letras mal feitas.'
            },
            804: {
                title: 'Raia',
                description: 'Linha. Palavra proveniente do espanhol, como tantas outras faladas no Rio Grande. Ex: A "bulita" de gude passou da raia!'
            },
            805: {
                title: 'Ralhar',
                description: 'Xingar, ou como se diz lá no Brasil, "dar sermão". Ex.: "Minha mãe ralhou comigo hoje porque eu chutei a bola no vidro da janela."'
            },
            806: {
                title: 'Rancho',
                description: 'Compra de itens alimentícios feita por famílias gaúchas na quantidade suficiente para cerca de 30 dias. Compra do mês.'
            },
            807: {
                title: 'Ratão',
                description: 'Palavra para definir alguém que pisa na bola, que faz algo de errado.'
            },
            808: {
                title: 'Rateada',
                description: 'Ato de vacilar, fazer algo errado, cometer um deslize. "Bah, meu, que rateada!"'
            },
            809: {
                title: 'Realengo',
                description: 'Sem dono, público.'
            },
            810: {
                title: 'Rebenque',
                description: 'Chicote curto, com o cabo retovado, com uma palma de couro na extremidade. Pequeno relho.'
            },
            811: {
                title: 'Reboldosa',
                description: 'Desordem, alvoroço, estrupício, barulho, trabalhos. O mesmo que rebordosa.'
            },
            812: {
                title: 'Rebuliço',
                description: 'Confusão causada devido algum desentendimento (confusão, revolta, briga)'
            },
            813: {
                title: 'Recuerdo',
                description: 'Lembrança, recordação.'
            },
            814: {
                title: 'Récula',
                description: 'Conjunto de bandidos, desordeiros.'
            },
            815: {
                title: 'Rédeas',
                description: 'Correia presa ao freio do animal de tiro ou de montaria, e que o cavaleiro segura nas mãos ao cavalgar; brida '
            },
            816: {
                title: 'Redevu',
                description: 'Briga, balbúrdia, bochincho, reunião que acaba mal. Derivado do francês Rendez-vous (reunião)'
            },
            817: {
                title: 'Redomão',
                description: 'Animal que foi mal domado'
            },
            818: {
                title: 'Reinando',
                description: 'Reinando, expressão usada quando se quer definir uma pessoa quando ela esta incomodando muito.\n\n\'\' Essa guria ta reinando hoje! \'\''
            },
            819: {
                title: 'Relampejando',
                description: 'Trovejando'
            },
            820: {
                title: 'Relampiar',
                description: 'Também chamado "relampeio". É o aparecimento de relâmpagos, que no português padrão seria "relampejar". Ex: Bah, mas como tá relampiando lá fora heim, prenda?'
            },
            821: {
                title: 'relho',
                description: 'Tipo de chicote usado pelos gaúchos para bater nos cavalos, bois e outros animais, ou até mesmo para brigar com outras pessoas. Na maioria das vezes esse instrumento é ou era usado sem violência, apenas para incentivar os animais.'
            },
            822: {
                title: 'Rengo',
                description: 'Aquele que está com a perna machucada.'
            },
            823: {
                title: 'Rengueando',
                description: 'Caminhar puxando uma perna devido a algum ferimento ou má-formação.'
            },
            824: {
                title: 'Renguear',
                description: 'Deixar manco; Ex. Hoje está frio de renguear cusco.'
            },
            825: {
                title: 'Repontar',
                description: 'Tocar o gado por diante de um lugar para outro.'
            },
            826: {
                title: 'Resbalar',
                description: 'Derivação de "resvalar" e sinônimo de "escorregar".\n\nAcredita-se que a derivação tenha acontecido devido a proximidade de nossas terras aos povos de descendência espanhola e da influência destes sobre nossa cultura.\n\nNa língua espanhola o som do "b" é frequentemente confundido com o som do "v", e vice versa. O comum engano linguístico foi apropriado à ponto de tornar-se uma nova palavra.'
            },
            827: {
                title: 'retirar',
                description: '...'
            },
            828: {
                title: 'Retoço',
                description: 'Festa; bagunça; esculhambação.'
            },
            829: {
                title: 'Revesgueio',
                description: 'De lado, de canto de olho, de atravessado... Fulana me olhou de revesgueio no baile....Bati na bola meio de revesgueio'
            },
            830: {
                title: 'Revessa',
                description: 'Lugar abrigado do vento.'
            },
            831: {
                title: 'Rincão',
                description: 'Porção de campo naturalmente cercado de mato. Lugar muito abrigado. Trecho de campo onde há arroio. Recanto formado por acidente natural'
            },
            832: {
                title: 'Riscada',
                description: 'Movimento rápido a cavalo, disparada.'
            },
            833: {
                title: 'Roça',
                description: 'significado: campo, lavoura, plantação.\nex: fui trabalhar na roça hoje.'
            },
            834: {
                title: 'Rodela',
                description: 'Mentira, patranha.'
            },
            835: {
                title: 'rodido',
                description: 'Medroso, incapaz, se diz de quem não consegue fazer determinada tarefa EX. Tu é um rodido mesmo, hehehe'
            },
            836: {
                title: 'Rojeira',
                description: 'Utensílio uasdo para guiar os bois na carreta, feita de couro cru ou de corda de linho.'
            },
            837: {
                title: 'Rufião',
                description: 'Cavalo inteiro destinado à reprodução. Garanhão pastor. Figuradamente, indivíduo dado a namoros, femeeiro.'
            },
            838: {
                title: 'Saberete',
                description: 'Sabichão.'
            },
            839: {
                title: 'Sacana',
                description: 'Indivíduo sem caráter. Também significa indivíduo trocista.'
            },
            840: {
                title: 'Saluço',
                description: 'Forma antiga e popular de soluço.'
            },
            841: {
                title: 'Samoco',
                description: 'Boca aberta, bocó, tapado, tonto, troxa'
            },
            842: {
                title: 'Sampar',
                description: 'Enfiar, fincar... Ex: "Te sampo minha adaga".'
            },
            843: {
                title: 'Sanga',
                description: 'Pequeno curso de água.'
            },
            844: {
                title: 'Sangado',
                description: 'Preso na sanga. Enfezado, raquítico.'
            },
            845: {
                title: 'Santo do pau oco',
                description: 'Quem se faz de santo(a), que tentar parecer bom, mas no fundo é um falsário. '
            },
            846: {
                title: 'Saracotear',
                description: 'Fazer bagunça, não parar quieto. Ex.: "Esse guri não pára de saracotear essa noite."'
            },
            847: {
                title: 'Sarna pra se coçar',
                description: 'Situação que poder gerar confusão. "Tu tá é procurando sarna pra se coçar". Procurando encrenca, incomodação.'
            },
            848: {
                title: 'Saudade da cadeia',
                description: 'Significa que você não esta com vontade de fazer nada, ficar na moleza,comer, beber, dormir e não pagar.'
            },
            849: {
                title: 'Se afeitar',
                description: 'Fazer a  barba'
            },
            850: {
                title: 'Se faz de salame',
                description: 'O termo completo é \'se faz de salame pra ser comido em rodelas\'. Refere-se ao indivíduo que finge não ser com ele, ou não saber do que se trata. Sinônimos: falso, mentiroso.'
            },
            851: {
                title: 'Se fazer',
                description: '[prep + v. tr.] Ato de ser omisso, tímido, insuficiente, decepcionante, dúbio, não confiável, não cumpridor, etc. Entende-se que o termo é uma redução de outras expressões como "se fazer de leitão", que têm o mesmo significado. (ex.: Ele já tem o dinheiro, mas está se fazendo para pagar o que me deve.)'
            },
            852: {
                title: 'se foi a gata com a cinta',
                description: 'não tem mais volta; não tem mais o que fazer'
            },
            853: {
                title: 'Se fresqueá',
                description: 'Se fazer louco, marcar passo, vacilar. '
            },
            854: {
                title: 'Se pah',
                description: 'Talvez, pode ser. Ex.: Se pah vamos ao shopping.'
            },
            855: {
                title: 'Se puchar',
                description: '[pron. indef. + v. int] Dar o máximo de si, se empenhar. 2. Surpreender.\n(ex.: Bah, eu tive que me puchar pra dar conta do trabalho acumulado.)'
            },
            856: {
                title: 'Se puxar',
                description: 'Esforçar-se; dedicar-se a algo. Me puxei para terminar a prova na hora.'
            },
            857: {
                title: 'Se sumiu',
                description: 'Desapareceu, foi embora'
            },
            858: {
                title: 'Sefazol',
                description: '[adj.] Quem se faz [procura por: "se fazer"]. (ex.: Ele é um baita de um sefazol; já tem o dinheiro mas não quer pagar o que me deve.)'
            },
            859: {
                title: 'Semostração',
                description: 'semostração = exibição; demonstração de alguma habilidade ou de algum pertence de muita qualidade. Ex: Andava de pé sobre a sela numa semostração que todos aplaudiam.'
            },
            860: {
                title: 'Senador',
                description: 'Cavalo muito velho.'
            },
            861: {
                title: 'Sequilho',
                description: 'Rosquinha de farinha de trigo, seca, revestida de açúcar cristalizado.'
            },
            862: {
                title: 'Serigaita',
                description: 'Mulher muito conversadeira, rampeira, regateira, escandalosa, buliçosa. O mesmo que sirigaita.'
            },
            863: {
                title: 'Serração',
                description: 'Nevoeiro, normalmente pela manhã, em alguns casos se extende por horas, dependendo da humidade e da ausensia de vento .\n\nExemplo: A cidade amanheceu debaixo de serração'
            },
            864: {
                title: 'Sesteada',
                description: 'Descansar; dormir ao sol; ato de dormir a tarde.'
            },
            865: {
                title: 'Sestear',
                description: 'dormir depois do almoço'
            },
            866: {
                title: 'Sílbido',
                description: 'Assobio.'
            },
            867: {
                title: 'Sinaleira',
                description: 'Semáforo. Ex.: Não me passa essa SINALERA tchê, ta fechada!'
            },
            868: {
                title: 'Sirigaita.',
                description: 'Mulher assanhada, sem modos, oferecida...'
            },
            869: {
                title: 'Soga',
                description: 'Corda feita de couro, ou de fibra vegetal, ou ainda de crina de animal, utilizada para prender o cavalo à estaca ou ao pau-de-arrasto, quando é posto a pastar. Corda de couro torcido ou trançado, que liga entre si as pedras das boleadeiras.'
            },
            870: {
                title: 'Solavanco',
                description: 'Baque da carreta nos acidentes da estrada.'
            },
            871: {
                title: 'Solear',
                description: 'Tomar um sol. Quando esta muito frio em casa, por exemplo, e você vai para fora para se esquentar no sol.'
            },
            872: {
                title: 'Sorro',
                description: '(do espanhol zorro)= raposa. Diz-se também da pessoa matreira, velhaca, sorrateira.'
            },
            873: {
                title: 'Sumanta',
                description: 'Apanhar. Tomar um pau. Surra. '
            },
            874: {
                title: 'tá de arreganho',
                description: 'tá de brincadeira'
            },
            875: {
                title: 'Tá louco!',
                description: 'Pronuncia-se “tá loco!”. \nExpressão de espanto ou desapontamento, dependendo do contexto que está sendo empregada. Usada também para chamar atenção.\n"Tá loco! Que jogão de bola!"\n"Taaaaá Louco! Que perda irreparável"\n"Tá loco! Fulano"\n Expressão muito usada na região do Médio Uruguai.'
            },
            876: {
                title: 'Tabacudo',
                description: 'Pessoa otária, sem noção das coisas, "boca aberta" imbecil.\nEi tabacudo !! cuidado pra não derrubar essa lata de tinta !!'
            },
            877: {
                title: 'Tacanho',
                description: 'Tosco, ignorante, panaca, de pouco conhecimento.'
            },
            878: {
                title: 'Taipa',
                description: '1.Represa de leivas, nas lavouras de arroz. 2.Cerca de pedra, na região serrana. 3.Tapado, burro, ignorante.\n" É um taipa mesmo!"'
            },
            879: {
                title: 'TALAGAÇO',
                description: 'quando um gaucho, convida o outro para tomar um gole de cachaça.\n- dá pelo menos um "talagaço" ai.\n- deixa eu dar um "talagaço".'
            },
            880: {
                title: 'Talho',
                description: 'Ferimento, corte.\nEx: O guri levou um talho no braço.'
            },
            881: {
                title: 'Taludo',
                description: 'Crescido, grande, desenvolvido.'
            },
            882: {
                title: 'Tambo',
                description: 'Estábulo onde se ordenham as vacas para a venda de leite.'
            },
            883: {
                title: 'Tamueiro',
                description: 'Utensílio usado pelos carreteiros para ligar os bois à carreta. Feita de couro.'
            },
            884: {
                title: 'Tança',
                description: 'Aquele que cometeu uma gafe, esqueceu algo, se meteu em alguma encrenca.'
            },
            885: {
                title: 'Tanso',
                description: 'Distraído, tolo, trapalhão.\nEx.: João passou em frente à prenda amada, ela lhe dirigiu um olhar interessado, mas ele não a viu. É um tanso mesmo.'
            },
            886: {
                title: 'Tapado de carrapicho',
                description: 'Cheio de problemas.'
            },
            887: {
                title: 'Tapeio',
                description: 'Tapeação, engano, embuste.'
            },
            888: {
                title: 'Tapera',
                description: 'Casa ou qualquer construção velha e abandonada.'
            },
            889: {
                title: 'tás tolo, istepô',
                description: 'Ó, lhó, lhó, rapagi, tás tolo, istepô, intiquento, miserento,digraçado!\n'
            },
            890: {
                title: 'Taura',
                description: 'Diz-se do indivíduo valente, guapo, arrojado, destemido, valoroso, forte'
            },
            891: {
                title: 'Tchê',
                description: 'Reza a lenda que deriva do termo Tupi Guarani Chê, que significa, meu, minha, com relação de pertencimento, e é entendido como sinonimo de "companheiro" ou "amigo". É utilizado como interjeição isoladamente ou em conjunto com outras palavras.'
            },
            892: {
                title: 'Tchê Loco',
                description: '1: Interjeição. chamamento utilizado para homens.\n\n2: Expressão para demonstrar surpresa e espanto.'
            },
            893: {
                title: 'Tchó',
                description: 'Igual ao Tchê, muito falado na região de Caxias do Sul.'
            },
            894: {
                title: 'Tchuco',
                description: 'Bêbado.'
            },
            895: {
                title: 'Te arranca',
                description: 'Sinônimo de mexa-se. \nPor exemplo, te arranca daí e vem logo.\n\nPode ser usado para o verbo levantar, sair, etc.'
            },
            896: {
                title: 'Te arremanga e vem',
                description: 'Para que o indivíduo vá rápido, de qualquer jeito, a todo custo, '
            },
            897: {
                title: 'Te larguei pras cobra!',
                description: 'Usado quando alguém faz ou fala algo de que tu discorda. Tipo, teu amigo fica com alguém que tu considera feio, daí tu fala: Ba, te larguei pras cobra!\nE não se pode falar pras cobras, é sem o s mesmo, para ser fiél ao modo com que a expressão sempre foi empregada =]'
            },
            898: {
                title: 'Te liga, bico de luz!',
                description: 'Expressão que se diz para a pessoa se orientar, se focar.'
            },
            899: {
                title: 'te liga cusco',
                description: 'chamar a atenção'
            },
            900: {
                title: 'Te manda',
                description: 'Sai daqui, vai embora.'
            },
            901: {
                title: 'Tejo',
                description: 'Jogo em que se atiram moedas sobre uma faca cravada no solo.'
            },
            902: {
                title: 'Telefone de arreio',
                description: 'Também conhecido como telefone celular.'
            },
            903: {
                title: 'Tendél',
                description: 'Confusão, escândalo, bagunça. Exemplo: A cumadre fez um tendél depois que o cumpadre pisou-lhe o pé.'
            },
            904: {
                title: 'Tentear',
                description: 'Entreter, enrolar. Exemplo: Não me tenteia morena, que tu é flor cheia de espinho...'
            },
            905: {
                title: 'Terça Lidio , depois da chuva !',
                description: 'Uma coisa tratada que nunca será cumprida , a pessoa trata e não cumpre'
            },
            906: {
                title: 'Termo',
                description: 'Térmica, recipiente para água quente'
            },
            907: {
                title: 'Terreiro',
                description: 'Local sem vegetação ao redor das casas de campanha.'
            },
            908: {
                title: 'Teto preto',
                description: 's.m.\n\nAcontecimento inesperado, fora do padrão.\n\n'
            },
            909: {
                title: 'Teu',
                description: 'É uma expressão para se referir à alguém. Em Porto Alegrte falam "véio", em São Paulo "meu" e em Tapejara "teu". Exemplo: E daí teu, beleza? \n\nTambém pode ser usada para repreender alguém  que falou alguma bobagem. Exemplo: "Ó o teu" ou "mas é um teu mesmo." A fina flor da grossura.'
            },
            910: {
                title: 'Tiorga',
                description: 'Embriaguez, borracheira, bebedeira, carraspana.'
            },
            911: {
                title: 'Tirada',
                description: 'Gauchada, agachada, compadrada.'
            },
            912: {
                title: 'Tiritando de frio',
                description: 'Quando a pessoa está com muito frio. '
            },
            913: {
                title: 'Tô de balde',
                description: 'Estar sem fazer nada. Variação: tô de valde. \nEx: Tchê, to de balde, vamos fazer alguma coisa?'
            },
            914: {
                title: 'Tô verde',
                description: 'Quando ja está com a barriga cheia de tanto tomar chimarrão, não aguenta mais chimarrão.'
            },
            915: {
                title: 'Tocaia',
                description: 'Espera, emboscada, espreita ao inimigo ou à caça.'
            },
            916: {
                title: 'Tomar um pileque',
                description: 'O mesmo que tomar um porre, embebedar-se...'
            },
            917: {
                title: 'Tombo',
                description: 'Prejuízo. “Dar o tombo em alguém” significa causar prejuízo a alguém.'
            },
            918: {
                title: 'Topeira',
                description: 'Pessoa mal vista, que não faz as coisas certas, ou cabeça dura'
            },
            919: {
                title: 'Tormenta',
                description: 'Temporal. Outra palavra proveniente do espanhol, muito usada pelos mais velhos.'
            },
            920: {
                title: 'Toró',
                description: 'Uma chuva extremamente forte. Ex.: Vai cair um baita toró hoje!'
            },
            921: {
                title: 'torrão',
                description: 'Determinada porção de terra, de acordo com o complemento do termo. Ex.: Torrão Gaúcho = Estado do Rio Grande do Sul ou ainda toda região dos povos gauchos (Rio Grande do Sul, Uruguay, partes da Argentina e Paraguay).'
            },
            922: {
                title: 'Torta de gostosa',
                description: 'Uma mulher excepcionalmente curvilínea e atraente.'
            },
            923: {
                title: 'Trampo',
                description: 'Trabalho, emprego.'
            },
            924: {
                title: 'Tramposo',
                description: 'Intrometido, trapaceiro, velhaco.'
            },
            925: {
                title: 'Tranco',
                description: 'Passo largo, firme e seguro, do cavalo ou do homem. Empurrão. Rapidamente, bruscamente.\n" Fiz o carro pegar no tranco."'
            },
            926: {
                title: 'Tranqueira',
                description: 'Muito carro na estrada. Lentidão no trânsito.'
            },
            927: {
                title: 'Traste',
                description: 'Diz-se da pessoa que incomoda, não serve quando precisamos, de animal, ou de algo em serventia. '
            },
            928: {
                title: 'treco',
                description: 'Treco é simplesmente um objeto. Treco pode ser também expressão para mal súbito.'
            },
            929: {
                title: 'Tremedeira',
                description: 'Tremura, tremor, estertores de moribundo.'
            },
            930: {
                title: 'Tremer',
                description: '1. Tirar sarro, sacanear. (ex.: Eu cai do cavalo e ficaram me tremendo.)'
            },
            931: {
                title: 'Tri',
                description: 'Muito'
            },
            932: {
                title: 'Tri legal',
                description: '1.Gíria utilizada para definir algo que é muito mais do que legal.'
            },
            933: {
                title: 'Trigo limpo',
                description: 'Quando se quer dizer que uma pessoa não tem boa índole;\n"Ele não é trigo limpo"'
            },
            934: {
                title: 'Trocentos',
                description: 'Usada pra representar uma quantidade muito grande de alguma coisa, sem especificar. Ex.: "Não te vejo há trocentos anos."'
            },
            935: {
                title: 'Troço',
                description: 'Alguma coisa, algo, um negócio. Usado quando a pessoa quer apontar algo mas não sabe oque é. Coisa indefinida.'
            },
            936: {
                title: 'Tromba d\'água',
                description: 'Temporal, chuva forte. Ex. Hoje vai cair uma tromba d\'água.\n'
            },
            937: {
                title: 'Tronqueira',
                description: 'Tronco grosso de árvore ex; mais atirado que pica-pau em tronqueira'
            },
            938: {
                title: 'Troteando',
                description: '1.Sinônimo de andar rápido, andar apressado.\nEx: Perdi o canto e tive que ir troteando para trampo.'
            },
            939: {
                title: 'trova',
                description: 'Ato de falar e tentar convencer a outra pessoa da sua idéia.\n\nFalar abobrinhas e fazer com que a pessoa acredite.\n\nPode-se também dizer que seria como um xaveco.\nTambém e um tipo de musica monostrófico tradicional do rio grande do sul'
            },
            940: {
                title: 'Trovar',
                description: 'Semelhante a flertar, conquistar. \n\nA pessoa que "trova" é chamada de "trovador"'
            },
            941: {
                title: 'truco',
                description: 'Jogo de baralho'
            },
            942: {
                title: 'Trupicar',
                description: 'V. Tropeçar em algo. Desequilibrar-se. Perder o rumo. Aplicação: "Bah, loco! Dei uma trupicada afú naquela pedra..."'
            },
            943: {
                title: 'Tubuna',
                description: 'Imbecil, tabacudo... Também é uma variedade de abelhas silvestre que produz mel de má qualidade.'
            },
            944: {
                title: 'Tuna',
                description: 'cactos, típicos da fronteira oeste, aplica-se numa frase do tipo "vá se coçar numa tuna", isto é "vá embora" \n'
            },
            945: {
                title: 'Tunda',
                description: 'Bater, surra.\nEx: Se tu continuares a me incomodar te darei uma tunda!'
            },
            946: {
                title: 'Tunda de laço',
                description: 'Camaçada de pau, sumanta de relho, apanhar.'
            },
            947: {
                title: 'Tungar',
                description: 'Molhar (submergir) alguma coisa em algo líquido. "Tungar o pão no café", por exemplo.'
            },
            948: {
                title: 'Tupetudo',
                description: 'Chimarrão'
            },
            949: {
                title: 'Ué',
                description: 'Interjeição que exprime admiração, espanto.'
            },
            950: {
                title: 'Uia!',
                description: 'Interjeição que expressa espanto, surpresa'
            },
            951: {
                title: 'Um pé lá e outro cá',
                description: 'Advertência, geralmente usada pelas mães, para avisar a criatura para não se demorar, porque senão vai ter.'
            },
            952: {
                title: 'Unhar',
                description: 'Roubar, furtar, surrupiar, levar o que não lhe pertence. Também significa correr, fugir à disparada, azular.'
            },
            953: {
                title: 'Untura',
                description: 'Remédio feito com sebo, carvão moído e outros ingredientes, usado para curar as matas dos animais de montaria ou de carga.'
            },
            954: {
                title: 'Urco',
                description: 'Diz-se do cavalo grande e belo.'
            },
            955: {
                title: 'Urucungo',
                description: 'Cavalo ruim, imprestável.'
            },
            956: {
                title: 'Urumbeba',
                description: 'Espécie de tuna, do gênero Cactus.'
            },
            957: {
                title: 'Usted',
                description: 'Você. Usado somente na fronteira.'
            },
            958: {
                title: 'Vá lamber sabão',
                description: 'Diz-se para uma pessoa que está falando bobagens, incomodando...'
            },
            959: {
                title: 'Vá te afumentar',
                description: 'Quando uma pessoa está te enchendo a paciência; o mesmo que "Vai te catar!".'
            },
            960: {
                title: 'Vá tomar onde as galinhas tomam',
                description: 'Xingamento, desprezo '
            },
            961: {
                title: 'Vai à fava',
                description: 'Sai daqui, não enche a paciência, pára com isso...'
            },
            962: {
                title: 'Vai ao gaio (vai ao galho)',
                description: 'Se dar mal ao fazer alguma coisa. Correr risco de perder algo importante (a vida) por causa de ações intencionais que geram risco.\nA expressão foi originada pelos suicídios por enforcamento na região. ("Fulano foi ao gaio" = morreu).'
            },
            963: {
                title: 'Vai catá coquinho!',
                description: 'Se diz á pessoa que esta molestando ou insistindo em uma coisa que nao tem fundamento.'
            },
            964: {
                title: 'Vanera',
                description: '[s. f.] Sinônimo de vanerão enquanto dança. 2. [s. f.] Sinônimo de vanerão enquanto estilo musical. [procura por "vanerão"]'
            },
            965: {
                title: 'Vanerão',
                description: '[s. m.] Dança de origem alemã desenvolvida no Rio Grande do Sul, onde se tornou uma das danças mais populares do estado. Também pode ser utilizada como sinônimo de vanera. (ex.: Vamos dançar um vanerão?) 2. [s. m.] Estilo musical que acompanha a dança. (ex.: Abre a gaita e toca um vanerão, gaitero bueno.)'
            },
            966: {
                title: 'Vão cordiando, indiada',
                description: 'Sirvam-se, vão repondo o prato com mais comida, vão cortando mais churrasco...'
            },
            967: {
                title: 'Vaquilhona',
                description: 'Vaca nova que ainda não pariu. Novilha.'
            },
            968: {
                title: 'Varado',
                description: 'Definição: com muita, louco, desesperado.\nEx: o fulano esta "varado" de fome.\n\nLocalidade: Porto Alegre/RS\nAutor: Fabio de Deus'
            },
            969: {
                title: 'Vareio',
                description: 'Susto, sova, surra, repreensão.\n\nDiz-se também da derrota vexatória de um time. "tomou um vareio" ou uma saraivada. Uma variação possível é "varei": que varei tu tomou hoje, hein?'
            },
            970: {
                title: 'Varejeira',
                description: 'Mosca que deposita seus ovos nas feridas dos animais, produzindo bicheiras.'
            },
            971: {
                title: 'Vareta',
                description: 'Desapontamento, atrapalhação, perturbação, encalistração, encabulação.'
            },
            972: {
                title: 'Vazio',
                description: 'O vazio, conhecido fora do Rio Grande do Sul como fraldinha, é um tipo de corte de carne bovina, localizada entre a parte traseira e a costela do animal.'
            },
            973: {
                title: 'Velhaco',
                description: 'Diz-se do animal que perde o costume de velhaquear, dar pinotes, corcovear.'
            },
            974: {
                title: 'Vergamota',
                description: 'Bergamota.'
            },
            975: {
                title: 'Vermelhão',
                description: 'Nome dado ao condimento \'colorau\'. Pigmento que se adiciona a comida para dar cor.'
            },
            976: {
                title: 'Vespra',
                description: 'Véspera.'
            },
            977: {
                title: 'viajou',
                description: 'enluqueceu, pirou'
            },
            978: {
                title: 'Vianda',
                description: 'Refeição fornecida a domicílio em marmitas. Marmita.'
            },
            979: {
                title: 'Vina',
                description: 'Vina, o mesmo que salsicha! Exemplo: Quero um cachorro quente com duas "vinas"!'
            },
            980: {
                title: 'Viso',
                description: 'Saia de baixo.'
            },
            981: {
                title: 'Viva o luxo e encolha o bucho',
                description: 'Mesmo que não tenha o que comer, mostre seu perfil em grande estilo, nunca perca a pose...'
            },
            982: {
                title: 'Vivente',
                description: 'Aplic. Saudação.Termo comumente usado para se dirigir a uma pessoa que está na roda ou se aproxima da mesma. Ex : Te aprochega vivente!! '
            },
            983: {
                title: 'Voar as tranças',
                description: 'Sair correndo, andar rápido, com pressa!!  Exemplo:(a) Voei as trança atrás de um laço novo!  (b) A menina voou as trança pra pegar o brinquedo novo!'
            },
            984: {
                title: 'Vortiada',
                description: 'Passeio'
            },
            985: {
                title: 'Vozerio',
                description: 'Vozeria, vozearia, vozeada, clamor de muitas vozes juntas.'
            },
            986: {
                title: 'xamixunga',
                description: 'Pessoa pegajosa, que não desgruda.'
            },
            987: {
                title: 'Xepa',
                description: 'Comida.'
            },
            988: {
                title: 'Xerenga',
                description: 'Faca velha, ordinária, ruim.'
            },
            989: {
                title: 'Xereta',
                description: 'Conversador, intrometido, bisbilhoteiro, importuno, novidadeiro, leva-e-traz, bajulador, engrossador.'
            },
            990: {
                title: 'Xi!',
                description: 'Interjeição que expressa surpresa, pasmo, horror.'
            },
            991: {
                title: 'Ximbo',
                description: 'Cavalo cujo dono não se conhece.'
            },
            992: {
                title: 'Xingar',
                description: 'Descompor, injuriar, dizer desaforo.'
            },
            993: {
                title: 'xirú',
                description: 'Expressão usada para se referir a algum indivíduo.\n\n"Xiru" é uma palavra de origem indígena (Guarani) e que significa amigo, podendo também ser sinônimo de forte e corajoso.'
            },
            994: {
                title: 'Xis',
                description: 'Tradicional lanche vendido no RS, normalmente composto de algum tipo de carne, tomate, alface, queijo, milho e ervilha. Aceita variações.'
            },
            995: {
                title: 'Xisme',
                description: 'Confusão., ajuntamento , reunião de pessoas palavra tipica da Fronteira Oeste'
            },
            996: {
                title: 'Xispar',
                description: 'Sair rapidamente. Ex.: "Xispa daqui, índio véio, senão vai fechar a peleia."'
            },
            997: {
                title: 'Xixo',
                description: 'Churrasquinho picado de palito, ou espeto com diferentes tipos de carne, cebola e tomate assados. '
            },
            998: {
                title: 'Xubrega',
                description: 'Diz-se do indivíduo insignificante, de animal ordinário, de coisa sem valor.'
            },
            999: {
                title: 'Xucrice',
                description: 'Qualidade de animal ou de indivíduo xucro. Grosseria. Falta de educação.'
            },
            1000: {
                title: 'Xucrismo',
                description: 'O mesmo que xucrice.'
            },
            1001: {
                title: 'XUCRO',
                description: '(adjetivo) Animal bravo e indomado; pessoa tosca, rude, sem modos. "Te afasta que o bagual é XUCRO"'
            },
            1002: {
                title: 'Zambo',
                description: 'Desnorteado, tonto, aparvalhado, atoleimado.'
            },
            1003: {
                title: 'Zarro',
                description: 'Incômodo, maçante, difícil de fazer, chato.'
            },
            1004: {
                title: 'Zorro',
                description: 'O mesmo que sorro ou guaraxaim. Figuradamente, a pessoa manhosa, atilada, disfarçada, velhaca.'
            },
            1005: {
                title: 'Zunir',
                description: 'Ir-se apressadamente.'
            },
            1006: {
                title: 'Zura',
                description: 'Diz-se da pessoa sovina.'
            },
            1007: {
                title: 'Zurra',
                description: 'Trabalhar intensamente com muito afinco. O mesmo que dizer asneiras, tolices.'
            },
            1008: {
                title: 'Zurrapa',
                description: 'Ruim, ordinário, de má qualidade.'
            }
        };
    }

    fetch() {
      return this.itens;
    }

    find(key) {

        console.log('key', key);

      return this.itens[ key ];
    }

}