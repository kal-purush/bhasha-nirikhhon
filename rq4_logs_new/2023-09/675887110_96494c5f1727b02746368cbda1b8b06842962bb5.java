package library;

public class MultimediaResources {
    // Enums ----------------------------------------------------------------------------
    private enum MultimediaType {
        AUDIO, VIDEO, SOFTWARE
    }

    // **********************************************************************************
    public static class AudioResource {
        // Private Attributes ***************************************
        private final MultimediaType type;
        private final String title;
        private final String description;
        private final String size;

        // Constructor **********************************************
        public AudioResource(String title, String description, String size) {
            this.type = MultimediaType.AUDIO;
            this.title = title;
            this.description = description;
            this.size = size;
        }

        // Getters **************************************************
        public MultimediaType getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public String getSize() {
            return size;
        }
    }

    // **********************************************************************************
    public static class VideoResource {
        // Private Attributes ***************************************
        private final MultimediaType type;
        private final String title;
        private final String description;
        private final String author;
        private final String duration;

        // Constructor **********************************************
        public VideoResource(String title, String description, String author, String duration) {
            this.type = MultimediaType.VIDEO;
            this.title = title;
            this.description = description;
            this.author = author;
            this.duration = duration;
        }

        // Getters **************************************************
        public MultimediaType getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public String getAuthor() {
            return author;
        }

        public String getDuration() {
            return duration;
        }
    }

    public static class SoftwareResource {
        // Private Attributes ***************************************
        private final MultimediaType type;
        private final String title;
        private final String version;
        private final String description;
        private final String size;

        // Constructor **********************************************
        public SoftwareResource(String title, String version, String description, String size) {
            this.type = MultimediaType.AUDIO;
            this.title = title;
            this.version = version;
            this.description = description;
            this.size = size;
        }

        // Getters **************************************************
        public MultimediaType getType() {
            return type;
        }

        public String getTitle() {
            return title;
        }

        public String getVersion() {
            return version;
        }

        public String getDescription() {
            return description;
        }

        public String getSize() {
            return size;
        }
    }
}