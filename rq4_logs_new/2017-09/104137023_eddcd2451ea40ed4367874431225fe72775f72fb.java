package net.coolblossom;

import net.coolblossom.lycee.machinelearning.classification.online.PAType;
import net.coolblossom.lycee.machinelearning.classification.online.PossiveAggressiveAlgorithm;

public class SamplePA extends SampleProgram {


	/**
	 * Passive-Aggressive Algorithmsによるオンライン学習
	 * @param C ソフトマージン
	 * @param type PAアルゴリズムの種類（PA/PA-I/PA-II）
	 */
	private void PassiveAgressiveAlgorithm(double C, PAType type) {
		System.out.println("* * * * Passive-Aggressive Algorithm (type="+type+"/C="+C+") * * * *");
		classify_online(new PossiveAggressiveAlgorithm(3, type, C));
	}

	@Override
	void execute() {
		// [オンライン学習] PAアルゴリズム
		for(PAType type : new PAType []{
				//PAType.PA,
				PAType.PA1,
				//PAType.PA2,
				}
		) {
			PassiveAgressiveAlgorithm(0.1, type);
			PassiveAgressiveAlgorithm(0.2, type);
			PassiveAgressiveAlgorithm(0.4, type);
			PassiveAgressiveAlgorithm(0.6, type);
			PassiveAgressiveAlgorithm(0.8, type);
			PassiveAgressiveAlgorithm(1.0, type);
		}
	}

}