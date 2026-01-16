/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DB;

import java.util.List;

/**
 *
 * @author Adminn
 */
public interface ITweetDataBase {
    public void addTweet(String tweet);
    public List<String> getTrendingTweet(int n, int time);
}