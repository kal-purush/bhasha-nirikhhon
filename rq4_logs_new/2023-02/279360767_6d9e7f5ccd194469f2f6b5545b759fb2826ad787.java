package net.zeeraa.novacore.commons.utils.misc;

import java.util.List;
import java.util.function.Consumer;

public class LowEffortShittyBatchedExecutor<T> {
	private int division;

	private List<T> source;
	private int index;

	public LowEffortShittyBatchedExecutor(int division, List<T> source) {
		this.division = division;
		this.source = source;

		index = 0;
	}

	public void consumeBatch(Consumer<T> consumer) {
		double toGetD =  (double)source.size() / (double)division;
		if(toGetD > 1) {
			toGetD = Math.round(toGetD);
		}
		int toGet = (int) toGetD;
		for(int i = 0; i < toGet; i++) {
			consumer.accept(source.get(index));
			index++;
			if(index >= source.size()) {
				index = 0;
				return;
			}	
		}
	}
}