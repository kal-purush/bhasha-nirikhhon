package seedu.address.model.tag;

/**
 * Enum class for type of tags.
 */
public enum TagType {
    FRIEND("Friend"),
    CLOSE_FRIEND("Close Friend"),
    EMERGENCY("Emergency");

    private final String displayName;

    TagType(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Method to return the string of tag display name.
     *
     * @return String of display name.
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Method to transform a string to a Tagtype.
     *
     * @param name name of the String.
     * @return Tagtype of the name passed in.
     */
    public static TagType fromString(String name) {
        for (TagType tagType : TagType.values()) {
            if (tagType.displayName.equalsIgnoreCase(name)
                    || (tagType == CLOSE_FRIEND && name.equalsIgnoreCase("cf"))) {
                return tagType;
            }
        }
        throw new IllegalArgumentException("Invalid tag name: " + name);
    }
}