package com.macedo.wendel.jollycooperationjava;

public class SoulMemory {

    public int getTier(int SoulM){

        int Tier;
        //Algoritimo de validação do número inserido. Caso o número inserido seja negativo, o sistema retorna que o Tier do jogador é igual a zero, o que não existe no jogo.
        if (SoulM < 0)
        {
            Tier = 0;
            return Tier;
        }

        if (SoulM < 50000)
        {
            float sm = ((float)SoulM + (float)1) / (float)10000;
            Tier = (int) sm;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 150000)
        {
            float sm = ((float)SoulM + (float)1 - (float)50000) / (float)20000;
            Tier = (int) sm;
            Tier = Tier + 5;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 300000)
        {
            float sm = ((float)SoulM + (float)1 - (float)150000) / (float)30000;
            Tier = (int) sm;
            Tier = Tier + 10;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 500000)
        {
            float sm = ((float)SoulM + (float)1 - (float)300000) / (float)50000;
            Tier = (int) sm;
            Tier = Tier + 15;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 1500000)
        {
            float sm = ((float)SoulM + (float)1 - (float)500000) / (float)100000;
            Tier = (int) sm;
            Tier = Tier + 19;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 3000000)
        {
            float sm = ((float)SoulM + (float)1 - (float)1500000) / (float)250000;
            Tier = (int) sm;
            Tier = Tier + 29;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 9000000)
        {
            float sm = ((float)SoulM + (float)1 - (float)3000000) / (float)2000000;
            Tier = (int) sm;
            Tier = Tier + 35;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 15000000)
        {
            float sm = ((float)SoulM + (float)1 - (float)9000000) / (float)3000000;
            Tier = (int) sm;
            Tier = Tier + 38;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 20000000)
        {
            Tier = 41;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 30000000)
        {
            Tier = 42;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else if (SoulM < 45000000)
        {
            Tier = 43;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
        else
        {
            Tier = 44;
            return Tier;
            //Console.WriteLine("Seu Tier é: " + Tier);
        }
    }
}