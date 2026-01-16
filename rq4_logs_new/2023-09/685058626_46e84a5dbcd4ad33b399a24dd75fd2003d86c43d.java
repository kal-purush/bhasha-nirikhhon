package main.NpeAndOptional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import main.Collection.Knight;
import main.Collection.MagicKnight;
import main.Collection.Unit;
import main.classStudy.Generic1.Side;

// ### `NullPointerException`
//
// 	- `null` 인 것으로부터 필드나 메소드 등을 호출하려 할 때 발생
// 	- *폐업한 중국집에 배민 주문*
// 	- 컴파일러 선에서 방지되지 않음
// 	- `RuntimeException`
public class Main {
	public static void main(String[] args) {
		String nulStr = null;
		System.out.println(nulStr.length()); // ⚠️ NPE

		System.out.println(
			catOrNull().length()  // 반복실행해 볼 것
		);

		//  try-catch 문으로 NPE에 대비하기
		//  반복실행해 볼 것
		try {
			System.out.println(
				catOrNull().length()
			);
		} catch (NullPointerException ne) {
			ne.printStackTrace();
			System.out.println(0);
		}

		//  💡 Optional 만들기
		//  of : 담으려는 것이 확실히 있을 때 null이 아닌 실제값을 넣을 때 널이 아닐때 확실할 때 of 사용
		Optional<String> catOpt = Optional.of("Cat");

		//  ⚠️ of로 null을 담으면 NPE
		catOpt = Optional.of(null);

		//  ofNullable : 담으려는 것이 null일 수도 있을 때
		Optional<String> dogOpt = Optional.ofNullable("Dog");
		Optional<String> cowOpt = Optional.ofNullable(null);

		//  명시적으로 null을 담으려면 empty 메소드 사용
		Optional<String> henOpt = Optional.empty();

		List<Optional<Unit>> randomUnitOpts = new ArrayList<>();
		IntStream.range(0, 20)
			.forEach(i -> randomUnitOpts.add(randomUnitOpt()));
		// 옵셔널의 리스트를 반환한다
		//  ⭐️ Optional의 값 사용하기
		randomUnitOpts.stream()
			.forEach(opt -> System.out.println(
				opt.isPresent() // 있다면 true
				//opt.isEmpty() // 없다면 true

				//opt.get() // 있다면 반환, 없다면 NPE

				// 💡 없을 시 다른 것 반환 (기본값으로 사용)
				//opt.orElse(new Swordman(Side.RED))
			));

		System.out.println("\n- - - - -\n");

		randomUnitOpts.stream()
			.forEach(opt -> {
				//  있다면 때 실행할 Consumer
				opt.ifPresent(unit -> System.out.println(unit));

				//  있다면 실행할 Consumer, 없다면 실행할 Runner(없으므로)
				//opt.ifPresentOrElse( // 두가지를 넣어준다 input이 없을 경우
				//        unit -> System.out.println(unit),
				//        () -> System.out.println("(유닛 없음)")
				//);
			});

		System.out.println("\n- - - - -\n");

		List<Optional<Integer>> optInts = new ArrayList<>();
		IntStream.range(0, 20)
			.forEach(i -> {
				optInts.add(Optional.ofNullable(
					new Random().nextBoolean() ? i : null
				));
			});

		//  💡 Optional의 filter와 map 메소드
		optInts.stream()
			.forEach(opt -> {
				System.out.println(
					opt
						//  ⭐️ 걸러진 것은 null로 인식됨
						//  - 스트림의 filter처럼 건너뛰는 것이 아님!
						.filter(i -> i % 2 == 1)// 이 filter는 optinal에 속하는 메서드이 optional의 필터는 걸러질것을 null로 처리한다
						.map(i -> "%d 출력".formatted(i)) // 맵에서 정수의 optional을 String optional로 바꿔준것임 값이 존재해야지 실행된다
						.orElse("(SKIP)") // Optional이 비어있을 경우
				);
			});

		var numFromOpt = IntStream.range(0, 100)
			//.parallel() // 병렬 실행 (이후 배움), 주석해제 해 볼 것

			.filter(i -> i % 2 == 1)
			//.filter(i -> i > 100) // 주석해제 후 다시 실행해 볼 것

			//  💡 첫 번째 요소를 반환
			.findFirst() // 항상 순서상 첫번 째 것을 반환 optional로 반환
			//.findAny() // ⭐️ 병렬작업 시 먼저 나오는 것 반환
			// 병렬작업 시 findAny가 보다 효율적
			// (순서가 중요하지 않다면)

			//.max()
			//.min()

			//  평균값을 ⭐️ Double로 반환
			//.average()

			//.reduce((prev, curr) -> prev + curr)

			.orElse(-1); // Optional이 반환되므로
		//  혹은 기타 Optional의 인스턴스 메소드 사용
	}

	public static String catOrNull () {
		//  슈뢰딩거의 고양이
		return new Random().nextBoolean() ? "Cat" : null;
	}

	public static Optional<String> getCatOpt () {
		return Optional.ofNullable(
			new Random().nextBoolean() ? "Cat" : null
		);
	}

	public static Optional<Unit> randomUnitOpt () {
		switch (new Random().nextInt(0, 3)) {

			//  💡 각 return 문을 가지므로 break 필요 없음
			case 0: return Optional.of(new Knight(Side.BLUE));
			case 1: return Optional.of(new MagicKnight(Side.BLUE));

			default: return Optional.empty();
		}
		//  ⭐️ Optional을 반환하는 메서드는 null을 반환하도록 하지 말 것! 빈 값이라도 옵셔널에 담아서 반환해줘야한다
		//  - 대신 빈 Optional을 반환 (Optional.empty)
		//  - NPE를 방지하기 위한 메소드이므로

	}


}