package com.example.projetfinal;

import androidx.room.ColumnInfo;
import androidx.room.Entity;
import androidx.room.PrimaryKey;
/**
 * The class used as the database's object
 */
@Entity
public class User {
    /**
     * The identifier that differentiate each user, cannot be decided or changed
     */
    @PrimaryKey(autoGenerate = true)
    public int uid;

    /**
     * the score of the user
     */
    @ColumnInfo(name = "score")
    public int score;

    /**
     * The name of the user
     */
    @ColumnInfo(name = "pieces")
    public int pieces;

    /**
     * The name of the user
     */
    @ColumnInfo(name = "skins")
    public int skins[];

    /**
     * The sound setting of the user
     */
    @ColumnInfo(name = "sound")
    public int sound;

    /**
     * The basic constructor of the User containing the essentials
     * @param pieces
     * @param score
     */
    public User(int score, int pieces) {
        this.score = score;
        this.pieces = pieces;
        this.sound = 100;
        this.skins = new int[]{2, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    }

    /**
     *
     * @return uid
     */
    public int getUid() {
        return uid;
    }

    /**
     *
     * @return score
     */
    public int getScore() {
        return score;
    }

    /**
     *
     * @param score
     */
    public void setScore(int score) {
        this.score = score;
    }

    /**
     *
     * @return sound
     */
    public int getSound() {
        return sound;
    }

    /**
     *
     * @param sound
     */
    public void setSound(int sound) {
        this.sound = sound;
    }

    /**
     *
     * @return pieces
     */
    public int getPieces() {
        return pieces;
    }

    /**
     *
     * @param pieces
     */
    public void setPieces(int pieces) {
        this.pieces = pieces;
    }

    /**
     *
     * @return skins
     */
    public int[] getSkins() {
        return skins;
    }

    /**
     *
     * @param skins
     */
    public void setSkins(int[] skins) {
        this.skins = skins;
    }
}