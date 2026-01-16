package com.roundsToThree.Structures;

import com.roundsToThree.DataProcessing.ByteUtils;
import com.roundsToThree.FileIO.DICOMLoader;

import java.util.ArrayList;
import java.util.Arrays;

public class ItemElement {

    private ItemElement() {
    }

    ;

    // Class specific variables
    public byte[] value;

    // Converts the raw binary value in a data element into readable content
    public static ArrayList<ItemElement> itemElementsFromDataElement(ValueRepresentation vr, byte[] data) {
        // Create an arraylist to store the items in
        ArrayList<ItemElement> items = new ArrayList<>();

        // The value is a sequence of elements
        if (vr.valueRepresentation == ValueRepresentation.VR_SQ) {
            // Where the current cursor is in the data
            int index = 0;
            while (index < data.length) {
                // Create a new item to populate
                ItemElement item = new ItemElement();

                // Ensure the element formally begins (0xFFFEE000)
                if (!Arrays.equals(Arrays.copyOfRange(data, index, index + 4), ITEM_ELEMENT_HEADER))
                    break;

                index += 4;
                // Get the length of the item's data
                long dataLength = ByteUtils.longFrom32Bit(Arrays.copyOfRange(data, index, index + 4));
                index += 4;
                // Check if the length is 'undefined' (in which case we search for a delimiter to end the item element)
                if (dataLength == 4294967295L) {
                    // Search for a delimiter to end the element
                    int end_index = ByteUtils.indexOfByteSequence(data, DICOMLoader.ITEM_DELIMINATOR, index);
                    item.value = Arrays.copyOfRange(data, index, end_index);
                } else {
                    System.out.println(dataLength);
                    // Fixed length element
                    item.value = Arrays.copyOfRange(data, index, (int) (index + dataLength));
                    index += dataLength;
                }

                items.add(item);
            }
        } else {
            // Else just a single item
            ItemElement item = new ItemElement();
            item.value = data;
            items.add(item);
        }

        return items;

    }


    // The byte header of a new item
    private static final byte[] ITEM_ELEMENT_HEADER = {(byte) 0xFE, (byte) 0xFF, (byte) 0x00, (byte) 0xE0};
}