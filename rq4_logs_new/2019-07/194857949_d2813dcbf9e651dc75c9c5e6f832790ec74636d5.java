public class Q14 {

    public static int countChain(long number, int[] chainNum) {
        int chain = 0;
        if (number < chainNum.length && chainNum[(int) number] > 0) {
            return chainNum[(int) number];
        } else {
            if (number % 2 == 0) {
                chain = countChain(number / 2, chainNum) + 1;
                if (number < chainNum.length)
                    chainNum[(int) number] = chain;
            } else {
                chain = countChain((number * 3 + 1) / 2, chainNum) + 2;
                if (number < chainNum.length)
                    chainNum[(int) number] = chain;
            }
            return chain;
        }
    }

    public static void main(String[] args) {
        final int LIMIT = 1000000;
        int[] chainNum = new int[LIMIT + 1];
        chainNum[1] = 1;

        // 最大値記録用
        int maxChain = 0;
        int maxChainNum = 0;

        // LIMIT以下のすべての数値のチェーンを数え、最大値記録。
        for (int i = LIMIT / 2; i <= LIMIT; i++) {

            // チェーン数える部分
            chainNum[i] = countChain((long) i, chainNum);

            // 記録部分
            if (chainNum[i] > maxChain) {
                maxChain = chainNum[i];
                maxChainNum = i;
            }
        }

        System.out.println(maxChainNum);
    }
}