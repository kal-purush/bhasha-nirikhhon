package stack;

import java.util.HashMap;
import java.util.Map;

public class CountUniqueElementsStackDecorator<Item> extends
		StackDecorator<Item> {

	private Map<Item, Integer> countUniqueElementsMap = new HashMap<Item, Integer>();

	public CountUniqueElementsStackDecorator(Stack<Item> decoratedStack) {
		super(decoratedStack);
	}

	@Override
	public void push(Item item) {
		Integer counter = 1;
		if (countUniqueElementsMap.containsKey(item)) {
			++counter;
		}
		countUniqueElementsMap.put(item, counter);

		decoratedStack.push(item);
	}

	@Override
	public Item pop() {
		Item item = decoratedStack.pop();
		Integer counter = countUniqueElementsMap.get(item);
		if (counter == 1) {
			countUniqueElementsMap.remove(item);
		} else {
			countUniqueElementsMap.put(item, --counter);
		}

		return item;
	}

	public int getCountOfUniqueElementsInStack() {
		return countUniqueElementsMap.size();
	}
}