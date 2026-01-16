package lavage;

public class Assiette {
	// On compte le nombre total d'assiettes
	private static int nombreAssiettes = 0;
    // Chaque assiette a un numéro
	private final int numeroAssiette;

	public Assiette() {
        // Pour que la classe soit "thread safe" :
		synchronized(this) {
			numeroAssiette = ++nombreAssiettes;
		}
	}

	@Override
	public String toString() {
		return "assiette n°" + numeroAssiette;
	}

}