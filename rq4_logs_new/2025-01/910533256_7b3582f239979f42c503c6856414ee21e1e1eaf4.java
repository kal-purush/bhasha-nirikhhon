/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tictactoe.domain.usecases;

import java.util.ArrayList;
import java.util.Random;

/**
 *
 * @author medos
 */
public class GetRandomPositionUseCase {
    
    public static int getRandomPosition(ArrayList<Integer> positions) {
         int randomIndex = new Random().nextInt(positions.size());
        return positions.get(randomIndex);
    }
}