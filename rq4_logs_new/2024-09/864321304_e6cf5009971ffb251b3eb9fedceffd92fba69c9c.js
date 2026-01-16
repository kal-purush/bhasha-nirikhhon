new Vue({
  el: '#app',
  data: {
    playerHP: 100,
    opponentHP: 100,
    opponentName: 'Pikachu',
    playerName: 'Blastoise',
    opponentLevel: 100,
    playerLevel: 100,
    opponentImage: 'https://media.giphy.com/media/vc0z5hIqODyhO/giphy.gif',
    playerImage: 'http://bit.ly/blastoisegif',
    message: 'O que Blastoise deve fazer?',
    playerMove: 0,
    opAttacks: ['thunderShock', 'Shock', 'ember', 'tackkle']
  },
  methods: {
    useAttack(attack) {
      if (this.playerMove === 0 && this.playerHP !== 0) {
        let miss = Math.floor(Math.random() * 10) + 1;
        if (miss === 1) {
          this.message = `Blastoise's attack missed!`;
          return;
        }
        
        this.message = `Blastoise used ${attack.replace(/([A-Z])/g, ' $1').toLowerCase()}`;
        let damage = this.calculateDamage(attack);
        this.opponentHP -= damage;
        if (this.opponentHP < 0) this.opponentHP = 0;
        
        if (this.opponentHP === 0) {
          this.message = "Pikachu está fora!";
          this.opponentImage = "https://media.giphy.com/media/3oKIPsoRmzEfUUe8JG/giphy.gif";
        } else {
          // Aguarda um pequeno tempo antes de fazer o ataque do oponente
          setTimeout(() => {
            this.compPokemon(); // Chama a função para o ataque do adversário automaticamente
          }, 1000); // 1000 ms = 1 segundo
        }

        this.playerMove = 1; // Atualiza a jogada do jogador
      }
    },
    calculateDamage(attack) {
      switch (attack) {
        case 'waterCannon':
          return this.criticalHit(30);
        case 'waterPulse':
          return this.criticalHit(20);
        case 'surf':
          return this.criticalHit(10);
        case 'tackle':
          return this.criticalHit(5);
        default:
          return 0;
      }
    },
    criticalHit(baseDamage) {
      let critical = Math.floor(Math.random() * 10) + 1;
      return critical === 4 ? baseDamage * 2 : baseDamage;
    },
    compPokemon() {
      if (this.playerMove === 1 && this.opponentHP !== 0) {
        let move = Math.floor(Math.random() * this.opAttacks.length);
        this[this.opAttacks[move]](); // Chama o método de ataque do oponente
        this.playerMove = 0; // Atualiza a jogada do jogador
      }
    },
    thunderShock() {
      this.opponentAttack(30, "Pikachu used thunder Shock");
    },
    Shock() {
      this.opponentAttack(20, "Pikachu used Shock");
    },
    ember() {
      this.opponentAttack(10, "Pikachu used ember");
    },
    tackkle() {
      this.opponentAttack(5, "Pikachu used tackle");
    },
    opponentAttack(damage, attackMessage) {
      let miss = Math.floor(Math.random() * 10) + 1;
      if (miss === 1) {
        this.message = "Pikachu's attack missed!";
      } else {
        this.message = attackMessage;
        this.playerHP -= this.criticalHit(damage);
        if (this.playerHP < 0) {
          this.playerHP = 0;
          this.playerImage = "https://img.pokemondb.net/sprites/black-white/anim/back-normal/blastoise.gif"; // Imagem do Pokémon caindo
          this.message = "Blastoise está fora!";
        }
      }
    },
    restartGame() {
      // Reinicia o jogo
      this.playerHP = 100;
      this.opponentHP = 100;
      this.opponentImage = 'https://media.giphy.com/media/vc0z5hIqODyhO/giphy.gif';
      this.playerImage = 'http://bit.ly/blastoisegif'; // Retorna a imagem original do jogador
      this.message = 'O que Blastoise deve fazer?';
      this.playerMove = 0;
    }
  }
});