package Producer_Consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import Semaphore.Mysemaphore;

public class buffer {
	public int size = 0;
	private int store[];
	private int inptr = 0;
	private int outptr = 0;
	private int counter = 0;
	Mysemaphore empty;
	Mysemaphore full;
	Mysemaphore mutex;

///////////////////////*Constructors*///////////////////////
	public buffer()// Default Constructor
	{
	}

	public buffer(buffer buff)// Copy Constructor
	{
		size = buff.size;
		empty = new Mysemaphore(size);
		full = new Mysemaphore(0);
		mutex = new Mysemaphore(1);
		store = new int[size];
	}

	public buffer(int b) // Parameterized constructor // the buffer bound
	{
		size = b;
		empty = new Mysemaphore(size);
		full = new Mysemaphore(0);
		mutex = new Mysemaphore(1);
		store = new int[size];
	}

///////////////////////*Wait and Signal*///////////////////////
	public void Signal(Mysemaphore S) {
		S.value++;
	}

	public void Wait(Mysemaphore S) {
		while (S.value <= 0)
			;
		S.value--;
	}

///////////////////////*Write into a file function*///////////////
	public void writeInFile(int value) {
		try (FileWriter fw = new FileWriter("write.txt", Charset.forName("UTF-8"), true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)) {
			char c = '"';
			out.write(c);
			out.print(value);
			out.write(c);
			out.write(",");

		} catch (IOException e) {
		}
	}

///////////////////////*Producer function*///////////////////////
	public void produce(int value) {
		do {
			Wait(empty);
			Wait(mutex);
			/* add next produced to the buffer */
			store[inptr] = value;
			inptr = (inptr + 1) % size;
			counter++;
			Signal(mutex);
			Signal(full);

		} while (counter < size);
	}

///////////////////////*Consumer function*///////////////////////
	public void consume() {
		int value;
		do {
			Wait(full);
			Wait(mutex);
			/* remove an item from buffer to next consumed */
			value = store[outptr];
			writeInFile(value);
			outptr = (outptr + 1) % size;
			counter--;
			// counter--;
			Signal(mutex);
			Signal(empty);
		} while (true);
	}
}