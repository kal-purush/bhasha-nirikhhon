package library.equipment;

/**
 * Class that represents an equipment piece belonging to a library.
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

public abstract class Equipment {
    // **********************************************************************************
    public static class Informatics {
        // Private Attributes ***************************************
        private final String id;
        private final InformaticsType type;
        private boolean available;

        // Constructor **********************************************
        public Informatics(String id, InformaticsType type) {
            this.id = id;
            this.type = type;
            this.available = true;
        }

        // Getters **************************************************
        public String getId() {
            return id;
        }

        public InformaticsType getType() {
            return type;
        }

        public boolean getAvailable() {
            return available;
        }

        // Setters **************************************************
        public void setAvailable(boolean newStatus) {
            this.available = newStatus;
        }
    }

    // **********************************************************************************
    public static class AudiovisualEquipment {
        // Private Attributes ***********************************
        private final String id;
        private final AudiovisualType type;
        private boolean available;

        // Constructor ******************************************
        public AudiovisualEquipment(String id, AudiovisualType type) {
            this.id = id;
            this.type = type;
            this.available = true;
        }

        // Getters **********************************************
        public String getId() {
            return id;
        }

        public AudiovisualType getType() {
            return type;
        }

        public boolean getAvailable() {
            return available;
        }

        // Setters **********************************************
        public void setAvailable(boolean newStatus) {
            this.available = newStatus;
        }
    }

    // **********************************************************************************
    public static class PrintingEquipment {
        // Private Attributes ***********************************
        private final String id;
        private final PrintingType type;
        private boolean available;

        // Constructor ******************************************
        public PrintingEquipment(String id, PrintingType type) {
            this.id = id;
            this.type = type;
            this.available = true;
        }

        // Getters **********************************************
        public String getId() {
            return id;
        }

        public PrintingType getType() {
            return type;
        }

        public boolean getAvailable() {
            return available;
        }

        // Setters **********************************************
        public void setAvailable(boolean newStatus) {
            this.available = newStatus;
        }
    }
}