package pt.ulusofona.lp2.deisiJungle;

import pt.ulusofona.lp2.deisiJungle.especieFilho.*;

public abstract class Specie {

    protected String id;
    protected String name;
    protected String image;
    protected String typeFeedSpecies;
    protected int currentEnergy;
    protected int energyConsumption;
    protected int gainEnergyRest;
    protected int minimumSpeed;
    protected int maximumSpeed;

    public Specie() {
    }

    /**
     * Identify species
     */
    protected static Specie identifySpecie(String id) {
        switch (id) {
            case "E" -> {
                return new Elephant();
            }
            case "L" -> {
                return new Lion();
            }
            case "T" -> {
                return new Turtle();
            }
            case "P" -> {
                return new Bird();
            }
            case "Z" -> {
                return new Tarzan();
            }
            case "U" -> {
                return new Unicorn();
            }
            case "G" -> {
                return new Giraffe();
            }
            default -> {
                return null;
            }
        }
    }

    public abstract String getId();

    public abstract void setId(String id);

    public abstract String getName();

    public abstract void setName(String name);

    public abstract String getImage();

    public abstract void setImage(String image);

    public abstract int getCurrentEnergy();

    public abstract void definirEnergiaAtual(int energiaAtual);

    public abstract int getEnergyConsumption();

    public abstract void definirConsumoEnergia(int consumoEnergia);

    public abstract int getGainEnergyRest();

    public abstract void definirGanhoEnergiaDescanso(int ganhoEnergiaDescanso);

    public abstract int getMaximumSpeed();

    public abstract void definirVelocidadeMaxima(int velocidadeMaxima);

    public abstract int getMinimumSpeed();

    public abstract void definirVelocidadeMinima(int velocidadeMinima);

    public abstract String getTypeFeedSpecies();

    public abstract String defineSpeciesFeedType(String tipoAlimentacaoDaEspecie);

    public abstract String toString();
}