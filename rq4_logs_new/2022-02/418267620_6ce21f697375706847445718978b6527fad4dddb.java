package dev.stocky37.xiv.actions.json;

import com.google.common.collect.Lists;
import com.ibm.asyncutil.util.Either;
import dev.stocky37.xiv.actions.data.Action;
import dev.stocky37.xiv.actions.data.Effect;
import dev.stocky37.xiv.actions.data.Item;
import dev.stocky37.xiv.actions.data.Rotation;
import dev.stocky37.xiv.actions.data.RotationAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RotationBuilder {
	private final List<Either<Action, Item>> actions;
	private final List<Effect> effects = new ArrayList<>();
	private Duration nextGcd = Duration.ZERO;
	private Duration nextOGcd = Duration.ZERO;
	private int gcdCount = 0;
	public static final Duration OGCD_DELAY = Duration.ofMillis(700);

	public RotationBuilder(List<Either<Action, Item>> actions) {
		this.actions = actions;
	}

	public Rotation build() {
		final List<RotationAction> timeline = actions.stream().map(this::buildAction).toList();
		return new Rotation(timeline);
	}

	private RotationAction buildAction(Either<Action, Item> actionOrItem) {
		if(onGCD(actionOrItem)) {
			return handleGcd(actionOrItem);
		} else {
			return handleOGcd(actionOrItem);
		}
	}

	private RotationAction handleGcd(Either<Action, Item> actionOrItem) {
		final var action = new RotationAction(
			actionOrItem.left(),
			actionOrItem.right(),
			nextGcd,
			Optional.of(gcdCount++),
			Lists.newArrayList(effects)
		);

		nextOGcd = nextGcd.plus(OGCD_DELAY);
		nextGcd = nextGcd.plus(recast(actionOrItem));

		return action;
	}

	private RotationAction handleOGcd(Either<Action, Item> actionOrItem) {
//		if(actionOrItem.isRight()) {
//			//noinspection OptionalGetWithoutIsPresent
//			effects.add(new Effect(
//				actionOrItem.right().get().id(),
//				nextOGcd,
//				actionOrItem.right().get().bonusDuration()
//			));
//		}

		final var action = new RotationAction(
			actionOrItem.left(),
			actionOrItem.right(),
			nextOGcd,
			Optional.empty(),
			Lists.newArrayList(effects)
		);

		nextOGcd = nextOGcd.plus(OGCD_DELAY);
		if(nextOGcd.compareTo(nextGcd) > 0) {
			nextGcd = nextOGcd;
		}
		return action;
	}

	private boolean onGCD(Either<Action, Item> actionOrItem) {
		return actionOrItem.fold(Action::onGCD, Item::onGCD);
	}

	private Duration recast(Either<Action, Item> actionOrItem) {
		return actionOrItem.fold(Action::recast, Item::recast);
	}

//	private void checkEffects(Duration nextAction) {
//		final List<Effect> removedEffects = effects.stream().filter(effect -> effect.started().plus
//		(effect.))
//	}
}