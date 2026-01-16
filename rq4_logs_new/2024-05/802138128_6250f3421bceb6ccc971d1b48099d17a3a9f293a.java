package com.example.study;

import android.os.Parcel;
import android.os.Parcelable;


//passing data between activities or processes in Android applications-->Parcelable
public class DailyTask implements Parcelable {
    private String name;
    private String notes;
    private boolean completed;

    public DailyTask(String name) {
        this.name = name;
        this.notes = "";
        this.completed = false;
    }

    protected DailyTask(Parcel in) {
        name = in.readString();
        notes = in.readString();
        completed = in.readByte() != 0;
    }

    public static final Creator<DailyTask> CREATOR = new Creator<DailyTask>() {
        @Override
        public DailyTask createFromParcel(Parcel in) {
            return new DailyTask(in);
        }

        @Override
        public DailyTask[] newArray(int size) {
            return new DailyTask[size];
        }
    };

    public String getName() {
        return name;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public String getNotes() {
        return notes;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel parcel, int i) {
        parcel.writeString(name);
        parcel.writeString(notes);
        parcel.writeByte((byte) (completed ? 1 : 0));
    }
}