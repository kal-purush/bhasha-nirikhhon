namespace JogoDaVelha.Entities
{
    public static class Tabuleiros
    {
        // função para fazer uma string do tabuleiro para adicioná-lo no registro da partida
        public static string TabuleiroParaRegistro(Tabuleiro tabuleiro)
        {
            string resultado = "";
            foreach (string s in tabuleiro.MatrizTabuleiro)
            {
                if (int.TryParse(s, out int n))
                    resultado += "1,";
                else
                    resultado += $"{s},";
            }
            return resultado;
        }
        // função para transformar um tabuleiro de registro (string[]) em um objeto da classe Tabuleiro
        public static Tabuleiro TransformarTabuleiroDeRegistroEmTabuleiro(string[] tabuleiroDeRegistro, int tamanhoDoTabuleiro)
        {
            string[,] matrizTabuleiro = new string[tamanhoDoTabuleiro, tamanhoDoTabuleiro];
            int cont = 0;
            for (int i = 0; i < tamanhoDoTabuleiro; i++)
            {
                for (int j = 0; j < tamanhoDoTabuleiro; j++)
                {
                    matrizTabuleiro[i, j] = tabuleiroDeRegistro[cont];
                    cont++;
                }
            }
            return new Tabuleiro(matrizTabuleiro, tamanhoDoTabuleiro);
        }
    }
}