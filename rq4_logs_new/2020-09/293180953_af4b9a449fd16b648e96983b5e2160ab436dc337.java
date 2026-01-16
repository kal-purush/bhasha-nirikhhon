
package com.example.android.tour_guide_app;

/**
 * {@link TourItem} represents a tour.
 * It contains a title, description, event date, address, phone, office hours, and an image for that tour.
 */
public class TourItem {

    /** Default title for the TourItem */
    private String mTitle;

    /** Description for the TourItem */
    private String mDescription;

    /** Event date for the TourItem */
    private String mEventDate;

    /** Address for the TourItem */
    private String mAddress;

    /** Phone number for the TourItem */
    private String mPhone;

    /** Office hours for the TourItem */
    private String mOfficeHours;

    /** Image resource ID for the TourItem */
    private int mImageResourceId = NO_IMAGE_PROVIDED;

    /** Constant value that represents no image was provided for this TourItem */
    private static final int NO_IMAGE_PROVIDED = -1;

    /**
     * Create a new TourItem object.
     *
     * @param title is the title for the TourItem
     * @param description is a description of the TourItem
     * @param eventDate is the date for the TourItem
     * @param address is the address for the TourItem
     * @param phone is the phone number for the TourItem
     * @param officeHours are the office hours for the TourItem
     * @param imageResourceId is the drawable resource ID associated with the TourItem
     *
     */
    public TourItem(String title, String description, String eventDate, String address, String phone, String officeHours, int imageResourceId) {
        mTitle = title;
        mDescription = description;
        mEventDate = eventDate;
        mAddress = address;
        mPhone = phone;
        mOfficeHours = officeHours;
        mImageResourceId = imageResourceId;
    }

    /**
     * Get the title of the TourItem.
     */
    public String getTitle() {
        return mTitle;
    }

    /**
     * Get the description of the TourItem.
     */
    public String getDescription() {
        return mDescription;
    }

    /**
     * Get the event date of the TourItem.
     */
    public String getmEventDate() {
        return mEventDate;
    }

    /**
     * Get the address of the TourItem.
     */
    public String getmAddress() {
        return mAddress;
    }

    /**
     * Get the phone number of the TourItem.
     */
    public String getmPhone() {
        return mPhone;
    }

    /**
     * Get the office hours of the TourItem.
     */
    public String getmOfficeHours() {
        return mOfficeHours;
    }

    /**
     * Get the image resource ID of the TourItem.
     */
    public int getImageResourceId() {
        return mImageResourceId;
    }

    /**
     * Returns whether or not there is an image for this TourItem.
     */
    public boolean hasImage() {
        return mImageResourceId != NO_IMAGE_PROVIDED;
    }
}