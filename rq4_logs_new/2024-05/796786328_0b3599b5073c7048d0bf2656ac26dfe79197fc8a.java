package hotel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class Hotel {
    private final int numQuartos = 10;
    private final int numHospedes = 50;
    private final int numCamareiras = 10;
    private final int numRecepcionistas = 5;

    private final BlockingQueue<Quarto> quartosDisponiveis;
    private final List<Thread> threadsCamareira;
    private final List<Thread> threadsHospede;
    private final List<Thread> threadsRecepcionista;
    private final List<Hospede> hospedesEsperando;

    public Hotel() {
        quartosDisponiveis = new ArrayBlockingQueue<>(numQuartos);
        threadsCamareira = new ArrayList<>();
        threadsHospede = new ArrayList<>();
        threadsRecepcionista = new ArrayList<>();
        hospedesEsperando = new ArrayList<>();

        for (int i = 0; i < numQuartos; i++) {
            quartosDisponiveis.add(new Quarto(i + 1));
        }

        for (int i = 0; i < numCamareiras; i++) {
            Thread threadCamareira = new Thread(new Camareira("Camareira-" + (i + 1), this));
            threadsCamareira.add(threadCamareira);
            threadCamareira.start();
        }

        for (int i = 0; i < numRecepcionistas; i++) {
            Thread threadRecepcionista = new Thread(new Recepcionista("Recepcionista-" + (i + 1), this));
            threadsRecepcionista.add(threadRecepcionista);
            threadRecepcionista.start();
        }

        for (int i = 0; i < numHospedes; i++) {
            Thread threadHospede = new Thread(new Hospede("Hospede-" + (i + 1), this));
            threadsHospede.add(threadHospede);
            threadHospede.start();
        }
    }

    public Quarto getQuartoDisponivel() throws InterruptedException {
        return quartosDisponiveis.take();
    }

    public void retornarQuarto(Quarto quarto) throws InterruptedException {
        quartosDisponiveis.put(quarto);
    }

    public synchronized void adicionarHospedeEsperando(Hospede hospede) {
        hospedesEsperando.add(hospede);
    }

    public synchronized void removerHospedeEsperando(Hospede hospede) {
        hospedesEsperando.remove(hospede);
    }

    public synchronized List<Hospede> getHospedesEsperando() {
        return new ArrayList<>(hospedesEsperando);
    }
}

class Quarto {
    private final int numeroQuarto;
    private final int capacidade = 4;
    private boolean ocupado;
    private boolean limpo;
    private int numHospedes;

    public Quarto(int numeroQuarto) {
        this.numeroQuarto = numeroQuarto;
        this.ocupado = false;
        this.limpo = true;
        this.numHospedes = 0;
    }

    public int getNumeroQuarto() {
        return numeroQuarto;
    }

    public synchronized boolean estaOcupado() {
        return ocupado;
    }

    public synchronized boolean estaLimpo() {
        return limpo;
    }

    public synchronized void setOcupado(boolean ocupado) {
        this.ocupado = ocupado;
    }

    public synchronized void setLimpo(boolean limpo) {
        this.limpo = limpo;
    }

    public synchronized void adicionarHospede(int numHospedes) {
        this.numHospedes += numHospedes;
        if (this.numHospedes >= capacidade) {
            ocupado = true;
        }
    }

    public synchronized void removerHospede(int numHospedes) {
        this.numHospedes -= numHospedes;
        if (this.numHospedes <= 0) {
            ocupado = false;
        }
    }
}

class Camareira implements Runnable {
    private final String nome;
    private final Hotel hotel;

    public Camareira(String nome, Hotel hotel) {
        this.nome = nome;
        this.hotel = hotel;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Quarto quarto = hotel.getQuartoDisponivel();
                limparQuarto(quarto);
                hotel.retornarQuarto(quarto);
                Thread.sleep(1000); // Simula tempo de limpeza
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void limparQuarto(Quarto quarto) {
        synchronized (quarto) {
            quarto.setLimpo(true);
        }
        System.out.println(nome + " limpou o quarto " + quarto.getNumeroQuarto());
    }
}

class Recepcionista implements Runnable {
    private final String nome;
    private final Hotel hotel;

    public Recepcionista(String nome, Hotel hotel) {
        this.nome = nome;
        this.hotel = hotel;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<Hospede> hospedesEsperando = hotel.getHospedesEsperando();
                for (Hospede hospede : hospedesEsperando) {
                    Quarto quarto = hotel.getQuartoDisponivel();
                    if (quarto != null) {
                        checkIn(quarto, hospede);
                    } else {
                        System.out.println(nome + " não encontrou um quarto para " + hospede.getNome() + ", eles terão que esperar.");
                    }
                }
                Thread.sleep(2000); // Simula tempo de trabalho do recepcionista
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void checkIn(Quarto quarto, Hospede hospede) {
        synchronized (quarto) {
            quarto.setOcupado(true);
            quarto.adicionarHospede(1);
        }
        System.out.println(nome + " fez check-in de " + hospede.getNome() + " no quarto " + quarto.getNumeroQuarto());
        hotel.removerHospedeEsperando(hospede);
    }
}

class Hospede implements Runnable {
    private final String nome;
    private final Hotel hotel;

    public Hospede(String nome, Hotel hotel) {
        this.nome = nome;
        this.hotel = hotel;
    }

    public String getNome() {
        return nome;
    }

    @Override
    public void run() {
        try {
            Thread.sleep((int) (Math.random() * 5000)); // Simula chegada do hóspede em momentos aleatórios
            Quarto quarto = hotel.getQuartoDisponivel();
            if (quarto != null) {
                fazerCheckIn(quarto);
                Thread.sleep((int) (Math.random() * 5000)); // Simula tempo de permanência no quarto
                fazerCheckOut(quarto);
            } else {
                System.out.println(nome + " não conseguiu um quarto e foi embora.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void fazerCheckIn(Quarto quarto) {
        synchronized (quarto) {
            quarto.setOcupado(true);
            quarto.adicionarHospede(1);
        }
        System.out.println(nome + " fez check-in no quarto " + quarto.getNumeroQuarto());
    }

    private void fazerCheckOut(Quarto quarto) {
        synchronized (quarto) {
            quarto.setOcupado(false);
            quarto.removerHospede(1);
        }
        System.out.println(nome + " fez check-out do quarto " + quarto.getNumeroQuarto());
        try {
            Thread.sleep(1000); // Simula hóspede saindo do quarto antes da camareira limpar
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        hotel.adicionarHospedeEsperando(this);
    }
}

public class Principal {
    public static void main(String[] args) {
        Hotel hotel = new Hotel();
        Thread threadRecepcionista = new Thread(new Recepcionista("Recepcionista", hotel));
        threadRecepcionista.start();
    }
}