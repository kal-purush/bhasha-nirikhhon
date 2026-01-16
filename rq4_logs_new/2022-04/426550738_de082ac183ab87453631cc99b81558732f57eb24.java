package com.myProject.ProjectSystem.AES;

public class Decryption implements SharedAES{
    private char state[];
    char[] ExpandedKey;

    @Override
    public char[] KeyExpansionCore(char[] in, int num){
        //rotate left
        char temp = in[0];
        in[0] = in[1];
        in[1] = in[2];
        in[2] = in[3];
        in[3] = temp;
        //sBox lookup
        for(int i = 0;i<4;i++){
            in[i] = sBox[in[i]];
        }
        //RCon look up table
        in[0] = (char) (in[0] ^ rCon[num]);

        return in;
    }
    @Override
    public void KeyExpansion(char[] key){
        ExpandedKey = new char[176];
        //First 16 bytes are the original key in the first add round key
        for (int i = 0;i<16;i++){
            ExpandedKey[i] = key[i];    //copy first 16 bytes of random key for Expansion
        }
        int bytesDone = 16;
        int rConNum = 1;
        char[] temp = new char[4];  //storing for the core
        while(bytesDone<176){
            for(int i = 0;i<4;i++){
                temp[i] = ExpandedKey[i + bytesDone-4];
            }
            //once every key do core.
            if(bytesDone % 16 == 0){
                temp = KeyExpansionCore(temp,rConNum++);
            }
            for(int a = 0;a<4;a++){
                ExpandedKey[bytesDone] = (char) (ExpandedKey[bytesDone - 16] ^ temp[a]);
                bytesDone++;
            }
        }
    }
    Decryption(char[] myKey){
        KeyExpansion(myKey);
    }

    public char[] decrypt(char[] cipherText){
        state = new char[16];
        char[] roundKey = new char[16];

        for(int i = 0;i<16;i++){
            state[i] = cipherText[i];
        }
        for(int a = 0;a<16;a++){
            roundKey[a] = ExpandedKey[(ExpandedKey.length-16) + a];//key 10
        }
        AddRoundKey(state,roundKey);
        ShiftRowsInverse(state);
        SubBytes(state);
        for(int round=0;round < rounds;round++){
            for(int a = 0;a<=15;a++){
                roundKey[a] = ExpandedKey[(ExpandedKey.length-32 -(16*round)) + a];//key 9-1
            }
            AddRoundKey(state,roundKey);
            mixColumns(state);
            ShiftRowsInverse(state);
            SubBytes(state);
        }
        for(int a = 0;a<16;a++){
            roundKey[a] = ExpandedKey[a];//key 9-1
        }
        AddRoundKey(state,roundKey);
        return state;
    }
    public void SubBytes(char[] state){
        for(int i = 0;i<16;i++){
            state[i] = sBoxInv[state[i]];
        }
    }
    public void AddRoundKey(char[] state, char[] key){
        for (int i = 0;i<16;i++){
            state[i] = (char) (state[i] ^ key[i]);
        }
    }
    public void mixColumns(char[] state){
        char[] temp = new char[16];

        temp[0] = (char)(mul14[state[0]] ^ mul11[state[1]] ^ mul13[state[2]] ^ mul9[state[3]]);    //Dot products and matrix multiplication
        temp[1] = (char)(mul9[state[0]] ^ mul14[state[1]] ^ mul11[state[2]] ^ mul13[state[3]]);    //Got stuck on this for quite some time
        temp[2] = (char)(mul13[state[0]] ^ mul9[state[1]] ^ mul14[state[2]] ^ mul11[state[3]]);    //Galois mul tables
        temp[3] = (char)(mul11[state[0]] ^ mul13[state[1]] ^ mul9[state[2]] ^ mul14[state[3]]);

        temp[4] = (char)(mul14[state[4]] ^ mul11[state[5]] ^ mul13[state[6]] ^ mul9[state[7]]);
        temp[5] = (char)(mul9[state[4]] ^ mul14[state[5]] ^ mul11[state[6]] ^ mul13[state[7]]);
        temp[6] = (char)(mul13[state[4]] ^ mul9[state[5]] ^ mul14[state[6]] ^ mul11[state[7]]);
        temp[7] = (char)(mul11[state[4]] ^ mul13[state[5]] ^ mul9[state[6]] ^ mul14[state[7]]);

        temp[8] = (char)(mul14[state[8]] ^ mul11[state[9]] ^ mul13[state[10]] ^ mul9[state[11]]);
        temp[9] = (char)(mul9[state[8]] ^ mul14[state[9]] ^ mul11[state[10]] ^ mul13[state[11]]);
        temp[10] = (char)(mul13[state[8]] ^ mul9[state[9]] ^ mul14[state[10]] ^ mul11[state[11]]);
        temp[11] = (char)(mul11[state[8]] ^ mul13[state[9]] ^ mul9[state[10]] ^ mul14[state[11]]);

        temp[12] = (char)(mul14[state[12]] ^ mul11[state[13]] ^ mul13[state[14]] ^ mul9[state[15]]);
        temp[13] = (char)(mul9[state[12]] ^ mul14[state[13]] ^ mul11[state[14]] ^ mul13[state[15]]);
        temp[14] = (char)(mul13[state[12]] ^ mul9[state[13]] ^ mul14[state[14]] ^ mul11[state[15]]);
        temp[15] = (char)(mul11[state[12]] ^ mul13[state[13]] ^ mul9[state[14]] ^ mul14[state[15]]);

        for(int i = 0;i<16;i++){
            state[i] = temp[i];
        }
    }
    public void ShiftRowsInverse(char[] state){    //inverse
        char[] temp = new char[16];

        temp[0] = state[0];
        temp[1] = state[13];
        temp[2] = state[10];
        temp[3] = state[7];

        temp[4] = state[4];
        temp[5] = state[1];
        temp[6] = state[14];
        temp[7] = state[11];

        temp[8] = state[8];
        temp[9] = state[5];
        temp[10] = state[2];
        temp[11] = state[15];

        temp[12] = state[12];
        temp[13] = state[9];
        temp[14] = state[6];
        temp[15] = state[3];

        for (int i = 0;i<16;i++){
            state[i] = temp[i];
        }
    }
    public void printStatePerRound(char[] state,int round){
        switch(round){
            case -1:
                System.out.println("State bytes");
                break;
            case 0:
                System.out.println("PreRound");
                break;
            default:
                System.out.printf("Round Number %d\n\r",round);
        }
        for(char a: state){
            System.out.printf(String.format("0x%02x ",(int) a));
        }
        System.out.println("\n\r");
    }
}