package com.nikart.data.dto;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * Created by Artem on 07.04.2017.
 * Статистика профиля
 */

public class Stats {

    @SerializedName("watchedHours")
    @Expose
    private Double watchedHours;
    @SerializedName("remainingHours")
    @Expose
    private Double remainingHours;
    @SerializedName("watchedEpisodes")
    @Expose
    private Integer watchedEpisodes;
    @SerializedName("remainingEpisodes")
    @Expose
    private Integer remainingEpisodes;
    @SerializedName("totalEpisodes")
    @Expose
    private Integer totalEpisodes;
    @SerializedName("totalDays")
    @Expose
    private Double totalDays;
    @SerializedName("totalHours")
    @Expose
    private Double totalHours;
    @SerializedName("remainingDays")
    @Expose
    private Double remainingDays;
    @SerializedName("watchedDays")
    @Expose
    private Double watchedDays;

    public Double getWatchedHours() {
        return watchedHours;
    }

    public void setWatchedHours(Double watchedHours) {
        this.watchedHours = watchedHours;
    }

    public Double getRemainingHours() {
        return remainingHours;
    }

    public void setRemainingHours(Double remainingHours) {
        this.remainingHours = remainingHours;
    }

    public Integer getWatchedEpisodes() {
        return watchedEpisodes;
    }

    public void setWatchedEpisodes(Integer watchedEpisodes) {
        this.watchedEpisodes = watchedEpisodes;
    }

    public Integer getRemainingEpisodes() {
        return remainingEpisodes;
    }

    public void setRemainingEpisodes(Integer remainingEpisodes) {
        this.remainingEpisodes = remainingEpisodes;
    }

    public Integer getTotalEpisodes() {
        return totalEpisodes;
    }

    public void setTotalEpisodes(Integer totalEpisodes) {
        this.totalEpisodes = totalEpisodes;
    }

    public Double getTotalDays() {
        return totalDays;
    }

    public void setTotalDays(Double totalDays) {
        this.totalDays = totalDays;
    }

    public Double getTotalHours() {
        return totalHours;
    }

    public void setTotalHours(Double totalHours) {
        this.totalHours = totalHours;
    }

    public Double getRemainingDays() {
        return remainingDays;
    }

    public void setRemainingDays(Double remainingDays) {
        this.remainingDays = remainingDays;
    }

    public Double getWatchedDays() {
        return watchedDays;
    }

    public void setWatchedDays(Double watchedDays) {
        this.watchedDays = watchedDays;
    }

}