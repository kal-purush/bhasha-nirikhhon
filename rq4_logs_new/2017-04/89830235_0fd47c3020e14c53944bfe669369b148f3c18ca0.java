/*
* Copyright 2017 by Robert M. Brako,
* All rights reserved.
*
* Permission is not granted to any person or entity to obtain a copy of this software and associated files
* (the "Software"), to deal in the Software, which includes without limitation any rights to use, copy, modify, merge,
* publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
* furnished to do so.  Permission may only be obtained by a signed and dated license agreement between licensor Robert
* Brako and prospective licensee.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
* WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
* COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
* OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package com.rmbcorp.organicchemistry.elements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** API Important Notice and Dev Instructions:
 * Use ElementType for general operations;
 * use Element for specific operations.
 */
public class Element {

    private static final char $ = ' ';

    private final ElementType primitive;
    private Orbital orbitals;
    private List<Element> elementsBonded = new ArrayList<>(4);

    Element(ElementType elementType) {
        primitive = elementType;
        orbitals = new Orbital(primitive.getOrbitalReference());
    }

    private char[] abbv() {
        return primitive.getAbbv();
    }

    boolean canBond() {
        return orbitals.canBond();
    }

    boolean bond(Element element) {
        return orbitals.bond(element.orbitals) && element.elementsBonded.add(this) && elementsBonded.add(element);
    }

    int bondCountWith(Element otherElement) {
        int count = 0;
        for (Element element : elementsBonded) {
            if (element.equals(otherElement)) {
                count++;
            }
        }
        return count;
    }

    int totalBondCount() {
        return elementsBonded.size();
    }

    static boolean bond(Element first, Element second) {
        return first.bondCountWith(second) < 3 && first.bond(second);
    }

    static char[][] graph(Element rootNode) {
        HashMap<int[], Element> graph = new HashMap<>();
        int[] coords = {0, 0};
        graph.put(coords, rootNode);
        int[] extrema = {0, 0, 0, 0};
        graph(rootNode, rootNode, graph, coords, extrema);
        return convert(graph, extrema);
    }

    static final int[][] disps = { { -1, 0}, { 0, 2 }, { 1, 0 }, { 0, -2 } };

    static void graph(Element prevNode, Element rootNode, Map<int[], Element> graph, int[] rootNodeCoords, int[] extrema) {
        for (int i = 0; i < rootNode.elementsBonded.size(); i++) {
            Element currNode = rootNode.elementsBonded.get(i);
            if (!currNode.equals(prevNode)) {
                int[] currCoords = {rootNodeCoords[0] + disps[i][0], rootNodeCoords[1] + disps[i][1]};
                graph(rootNode, currNode, graph, currCoords, extrema);
                putGraph(graph, currNode, currCoords, i);
                if (currCoords[0] < extrema[0])
                    extrema[0] = currCoords[0];
                if (currCoords[1] < extrema[1])
                    extrema[1] = currCoords[1];
                if (currCoords[0] > extrema[2])
                    extrema[2] = currCoords[0];
                if (currCoords[1] > extrema[3])
                    extrema[3] = currCoords[1];
            }
        }
    }

    private static void putGraph(Map<int[], Element> graph, Element currNode, int[] currCoords, int dir) {
        int[] currGraphItem;
        for (Map.Entry<int[], Element> entry : graph.entrySet()) {
            currGraphItem = entry.getKey();
            if (currGraphItem[0] == currCoords[0] && currGraphItem[1] == currCoords[1]) {
                int[] newDisp = disps[(dir + 2) % 4];
                currCoords[0] += 2 * newDisp[0];
                currCoords[1] += 2 * newDisp[1];
            }
        }
        graph.put(currCoords, currNode);
    }

    private static char[][] convert(Map<int[], Element> graph, int[] extrema) {
        int height = extrema[2] - extrema[0];
        int width = (extrema[3] - extrema[1]);
        int[] coords;
        char[][] result = new char[height+1][width+2];
        for (Map.Entry<int[], Element> entry : graph.entrySet()) {
            coords = entry.getKey();
            result[coords[0] - extrema[0]][coords[1] - extrema[1]] = entry.getValue().abbv()[0];
            result[coords[0] - extrema[0]][coords[1] - extrema[1] +1] = entry.getValue().abbv()[1];
        }
        return result;
    }
}