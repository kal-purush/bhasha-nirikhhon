package library.management.equipment;

import library.Library;

/**
 * Class that represents the various items and equipment belonging to a library.
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

public class Equipment {
    // Private Attributes ---------------------------------------------------------------
    private Library library;
    private final int id;
    private final EquipmentCategory category;
    private final String brand;
    private EquipmentStatus status;

    // Constructor ----------------------------------------------------------------------
    public Equipment(Library library, int id, EquipmentCategory category, String brand) {
        this.library = library;
        this.id = id;
        this.category = category;
        this.brand = brand;
    }

    // Inner Classes --------------------------------------------------------------------
    // Informatics Inner Class **********************************************************
    public class Informatics {
        // Private Attributes ***************************************
        private final InformaticsType type;

        // Constructor **********************************************
        public Informatics(InformaticsType type) {
            this.type = type;
        }

        // Getters **************************************************
        public InformaticsType getType() {
            return type;
        }
    }

    // Audiovisual Inner Class **********************************************************
    public class Audiovisual {
        // Private Attributes ***************************************
        private final AudiovisualType type;

        // Constructor **********************************************
        public Audiovisual(AudiovisualType type) {
            this.type = type;
        }

        // Getters **************************************************
        public AudiovisualType getType() {
            return type;
        }
    }

    // Printing Inner Class *************************************************************
    public class Printing {
        // Private Attributes ***************************************
        private final PrintingType type;
        private boolean hasPaint;        // Always true for scanners.

        // Constructor **********************************************
        public Printing(PrintingType type) {
            this.type = type;
            this.hasPaint = true;
        }

        public Printing(PrintingType type, boolean hasPaint) {
            this.type = type;
            this.hasPaint = hasPaint;
        }

        // Getters **************************************************
        public PrintingType getType() {
            return type;
        }

        public boolean getHasPaint() {
            return hasPaint;
        }

        // Setters **************************************************
        public void setHasPaint(boolean newPaintStatus) {
            this.hasPaint = newPaintStatus;
        }
    }

    // Getters --------------------------------------------------------------------------
    public Library getLibrary() {
        return library;
    }

    public int getId() {
        return id;
    }

    public EquipmentCategory getCategory() {
        return category;
    }

    public String getBrand() {
        return brand;
    }

    public EquipmentStatus getStatus() {
        return status;
    }

    // Setters --------------------------------------------------------------------------
    private void setLibrary(Library newLibrary) {
        this.library = newLibrary;
    }

    public void setStatus(EquipmentStatus newStatus) {
        this.status = newStatus;
    }
}