package a_vegetable_garden.modele.legumes;

public enum FamillesLegume {

    //FAMILLE (humidite_min, humidite_max, ensoleillement_min, ensoleillement_max, temperature_min, temperature_max)
    LEGUME_FEUILLES(50, 100, 0, 50, 10, 25), //salade, poireau, brocoli, choux-fleur, épinard
    LEGUME_RACINES(0, 50, 0, 50, 5, 25), //carotte, navet, radis,
    LEGUME_A_FRUITS(50, 100, 50, 100, 15, 35), //tomate, aubergine, poivron
    LEGUME_GRIMPANT(50, 100, 50, 100, 15, 35), //maïs
    LEGUME_A_BULBES(0, 50, 0, 50, 10, 25); //oignon, ail

    private final float humidite_min;
    private final float humidite_max;
    private final float ensoleillement_min;
    private final float ensoleillement_max;
    private final float temperature_min;
    private final float temperature_max;
    FamillesLegume(float humidite_min, float humidite_max, float ensoleillement_min, float ensoleillement_max, float temperature_min, float temperature_max) {
        this.humidite_min = humidite_min;
        this.humidite_max = humidite_max;
        this.ensoleillement_min = ensoleillement_min;
        this.ensoleillement_max = ensoleillement_max;
        this.temperature_min = temperature_min;
        this.temperature_max = temperature_max;
    }

    float getHumiditeMin() {
        return humidite_min;
    }

    float getHumiditeMax() {
        return humidite_max;
    }

    float getEnsoleillementMin() {
        return ensoleillement_min;
    }

    float getEnsoleillementMax() {
        return ensoleillement_max;
    }

    float getTemperatureMin() {
        return temperature_min;
    }

    float getTemperatureMax() {
        return temperature_max;
    }
}