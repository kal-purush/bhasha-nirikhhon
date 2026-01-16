package fi.tuni.koodimankelit.antibiootit.database.data;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Database representation of targetedInfo
 */
@Schema(description = "Representation of a targeted info")
public class TargetedInfo {
    
    private final String checkbox;
    private final String text;

    /**
     * Default constructor
     * @param checkbox the checkbox selected 
     * @param text text for that selection
     */
    public TargetedInfo(String checkbox, String text) {
        this.checkbox = checkbox;
        this.text = text;
    }

    /**
     * Returns checkbox
     * @return String checkbox
     */
    public String getCheckbox() {
        return this.checkbox;
    }

    /**
     * Returns text for checkbox
     * @return String text
     */
    public String getText() {
        return this.text;
    }

}