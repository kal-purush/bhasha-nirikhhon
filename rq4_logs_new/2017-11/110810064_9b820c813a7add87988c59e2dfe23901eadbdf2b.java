package com.makebono.algorithms.greedyalgorithm.huffmancoding;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.makebono.datastructures.binarysearchtree.HTNode;
import com.makebono.datastructures.binarysearchtree.HuffmanBonoTree;

/** 
 * @ClassName: HuffmanCoding 
 * @Description: Implementation of Huffman. Use my BinarySearchBonoTree to encode. And color to store letters.
 * @author makebono
 * @date 2017年11月22日 下午2:47:52 
 *  
 */
public class HuffmanCoding {
    private final HuffmanBonoTree huffmanTree;

    public HuffmanCoding() {

        this.huffmanTree = new HuffmanBonoTree();
    }

    private ArrayList<HTNode> buildTree(final String location) throws FileNotFoundException {
        final Scanner data = new Scanner(new File(location));
        final HashMap<Character, Integer> map = new HashMap<Character, Integer>();
        ArrayList<HTNode> elements = new ArrayList<HTNode>();
        final StringBuilder sb = new StringBuilder();
        int countChar = 0;

        while (data.hasNext()) {
            final String temp = data.next();
            sb.append(temp);
            for (int i = 0; i < temp.length(); i++) {
                final char input = temp.charAt(i);
                int count = 0;

                if (map.get(input) == null) {
                    count = 1;
                } else {
                    count = map.get(input) + 1;
                }
                map.remove(input);
                map.put(input, count);
            }

            if (countChar == 20) {
                sb.append("\n");
                countChar = 0;
            } else {
                sb.append(" ");
                countChar++;
            }
        }
        System.out.println("The original text is: ");
        System.out.println(sb.toString() + "\n\n");

        data.close();
        // System.out.println(map.entrySet().size());

        int index = 0;
        for (final Map.Entry<Character, Integer> cursor : map.entrySet()) {
            elements.add(new HTNode(index, cursor.getValue(), cursor.getKey()));
            index++;
        }

        /*
        for (final HTNode cursor : elements) {
            System.out.println(cursor.getColor() + ": " + cursor.getData());
        }
        */

        elements = this.huffmanTree.buildTree(elements);

        return elements;
    }

    public ArrayList<HTNode> encode(final String location) throws FileNotFoundException {
        final ArrayList<HTNode> result = this.buildTree(location);
        System.out.println("Huffman coding goes like below: ");
        int i = 1;
        for (final HTNode cursor : result) {
            System.out.print(cursor.getColor() + "(" + cursor.getCode() + ")   ");
            if (i == 10) {
                i = 0;
                System.out.print("\n");
            } else {
                i++;
            }
        }

        return result;
    }
}