package thebombzen.thecolourofmoney;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataToModel {
	public static Map<Integer, boolean[]> positionMap = new HashMap<Integer, boolean[]>();
	public static Map<Integer, Integer> positionSumMap = new HashMap<Integer, Integer>();

	public static void main(String[] args) throws Exception {
		System.out.println("Reading...");
		// populatePositionMap();
		readPositionMap();
		System.out.println("Calculating...");
		int pts = 59;
		float best = 0;
		int bestx = 0;
		int bestxd = 0;
		int besty = 0;
		int bestyd = 0;
		int bestz = 0;
		int bestzd = 0;
		int bestc = 0;
		int bestcd = 0;
		byte[][] results = new byte[1 << 20][pts + 1];
		for (int constant = -1; constant <= 1; constant += 2) {
			for (int constantd = 1; constantd <= 10; constantd += 1) {
				for (int xterm = -1; xterm <= 1; xterm += 2) {
					for (int xtermd = 1; xtermd <= 10; xtermd += 1) {
						for (int yterm = -1; yterm <= 1; yterm += 2) {
							for (int ytermd = 1; ytermd <= Math.abs(yterm)
									* (pts / 2) + 1; ytermd += 1) {
								for (int zterm = -1; zterm <= 1; zterm += 2) {
									for (int ztermd = 1; ztermd <= 10; ztermd += 1) {
										PolynomialModel modeltest = new PolynomialModel(
												constant, constantd, xterm,
												xtermd, yterm, ytermd, zterm,
												ztermd);
										results = generateData(modeltest, pts,
												results);
										float prob = chanceOfWinning(results,
												pts);
										if (prob > best) {
											best = prob;
											bestx = xterm;
											besty = yterm;
											bestz = zterm;
											bestc = constant;
											bestxd = xtermd;
											bestyd = ytermd;
											bestzd = ztermd;
											bestcd = constantd;
											System.out
													.format("Best prob so far was %f%% at c=%d, cd=%d, x=%d, xd=%d, y=%d, yd=%d, z=%d, zd=%d%n",
															best * 100, bestc,
															bestcd, bestx,
															bestxd, besty,
															bestyd, bestz,
															bestzd);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		System.out
				.format("Best prob was %f%% at c=%d, cd=%d, x=%d, xd=%d, y=%d, yd=%d, z=%d, zd=%d%n",
						best * 100, bestc, bestcd, bestx, bestxd, besty,
						bestyd, bestz, bestzd);
		// System.out.println("Serializing...");
		// ObjectOutputStream out = new ObjectOutputStream(new
		// BufferedOutputStream(new FileOutputStream("move_guess_data.dat")));
		// out.writeObject(results);
		// out.close();
	}

	public static byte[][] generateData(PolynomialModel model, int pts,
			byte[][] alloc) {
		byte[][] results;
		if (alloc != null && alloc.length >= (1 << 20)
				&& alloc[0].length >= pts + 1) {
			results = alloc;
		} else {
			results = new byte[1 << 20][pts + 1];
		}
		for (int position = 0; position < (1 << 20); position++) {
			int numCards = hammingWeight(position);
			if (numCards < 11) {
				continue;
			}
			boolean[] cards = positionMap.get(position);
			int sum = positionSumMap.get(position);
			int avgCard = sum / numCards;
			for (int pointsLeft = 0; pointsLeft <= pts; pointsLeft++) {
				int base = model.getLinearValue(numCards, pointsLeft, avgCard) - 1;
				// int base = 10;
				if (base < 0) {
					base = 0;
				}
				if (base > 19) {
					base = 19;
				}
				int reach = 1;
				while (!cards[base]) {
					int lower = base - reach;
					int higher = base + reach;
					if (lower < 0) {
						lower = 0;
					}
					if (higher > 19) {
						higher = 19;
					}
					if (cards[higher]) {
						base = higher;
						break;
					}
					if (cards[lower]) {
						base = lower;
						break;
					}
					reach++;
				}
				results[position][pointsLeft] = (byte) (base + 1);
			}
		}
		return results;
	}

	public static void populatePositionMap() throws Exception {
		for (int i = 0; i < (1 << 20); i++) {
			boolean[] positionArray = new boolean[20];
			int sum = 0;
			for (int k = 0; k < 20; k++) {
				positionArray[k] = ((i >> k) & 1) == 1;
				if (positionArray[k]) {
					sum += k + 1;
				}
			}
			positionMap.put(i, positionArray);
			positionSumMap.put(i, sum);
		}
		ObjectOutputStream oos = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream(
						"position_array_data.dat")));
		oos.writeObject(positionMap);
		oos.writeObject(positionSumMap);
		oos.close();
	}

	@SuppressWarnings("unchecked")
	public static void readPositionMap() throws Exception {
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(
				new FileInputStream("position_array_data.dat")));
		positionMap = (Map<Integer, boolean[]>) ois.readObject();
		positionSumMap = (Map<Integer, Integer>) ois.readObject();
		ois.close();
	}

	public static int hammingWeight(int x) {
		x -= ((x >>> 1) & 0x55555555);
		x = (((x >>> 2) & 0x33333333) + (x & 0x33333333));
		x = (((x >>> 4) + x) & 0x0f0f0f0f);
		x += (x >>> 8);
		x += (x >>> 16);
		return (x & 0x0000003f);
	}

	public static float chanceOfWinning(byte[][] results, int pts)
			throws Exception {
		float wins = 0;
		final float plays = 100;
		Random random = new Random();
		boolean[] cards = new boolean[20];
		for (int play = 0; play < plays; play++) {
			int ptsLeft = pts;
			int guess = 0;
			int flippedCard = 0;
			int position = (1 << 20) - 1;
			Arrays.fill(cards, true);
			for (int cardsLeft = 20; cardsLeft > 10 && ptsLeft > 0; cardsLeft--) {
				flippedCard = getRandomTrueSlot(random, cards);
				position -= 1 << (flippedCard - 1);
				cards[flippedCard - 1] = false;
				guess = results[position][ptsLeft];
				if (guess <= flippedCard) {
					ptsLeft -= guess;
				}
				if (ptsLeft <= 0) {
					wins++;
				}
			}
		}
		return wins / plays;
	}

	public static int[] openSlots = new int[20];

	public static int getRandomTrueSlot(Random random, boolean[] array) {
		int curr = 0;
		for (int i = 0; i < 20; i++) {
			if (array[i]) {
				openSlots[curr] = i;
				curr++;
			}
		}
		return openSlots[random.nextInt(curr)] + 1;
	}
}