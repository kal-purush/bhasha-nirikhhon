package jp.co.opst.java9.exercise.lib.exception;

/**
 * try-with-resourcesのリソースです。
 * 
 * @param <A> オリジナルのリソース。このリソースがtry-with-resourcesのリソースになります
 * @param <P> リソースを加工した、実際のアクセスに用いるオブジェクト
 */
public final class Resource<A extends AutoCloseable, P> {

	/**
	 * リソースが結果を返している間、リソースへのアクセスを繰り返します。
	 * 
	 * @param <R> リソースに対して繰り返しアクセスする関数の結果
	 */
	public final class WhilePresent<R> {

		/** リソースに対して繰り返しアクセスする関数。 */
		private final Processor<P, R, Exception> processor;

		/**
		 * コンストラクター。
		 * 
		 * @param processor リソースに対して繰り返しアクセスする関数
		 */
		private WhilePresent(Processor<P, R, Exception> processor) {
			this.processor = processor;
		}

		/**
		 * リソースが結果を返している間、リソースへのアクセスを繰り返した後、リソースを閉じます。
		 * 
		 * <p>
		 * リソースに対して繰り返しアクセスする関数がnullを返した時点で、繰り返しを終了します。
		 * なお、結果を受け取る関数には、nullを渡しません。
		 * </p>
		 * 
		 * @param acceptor 結果を受け取る関数
		 * @throws Exception リソースへのアクセスに失敗した場合
		 */
		public void accept(Acceptor<R, Exception> acceptor) throws Exception {
			try (A resource = resourceGenerator.generate()) {
				P processed =  resourceProcessor.process(resource);
				processor.normalize(processed).whilePresent(acceptor::uncheck);
			}
		}

		/**
		 * リソースが結果を返している間、リソースへのアクセスを繰り返した後、リソースを閉じます。
		 * 
		 * <p>
		 * リソースに対して繰り返しアクセスする関数がnullを返した時点で、繰り返しを終了します。
		 * なお、結果は使用せずに破棄します。
		 * </p>
		 * 
		 * @throws Exception リソースへのアクセスに失敗した場合
		 */
		public void cast() throws Exception {
			accept(Acceptor.nop());
		}
	}

	/**
	 * リソースを生成します。
	 * 
	 * @param <A> オリジナルのリソース
	 * @param generator リソースを取得する関数。このリソースがtry-with-resourcesのリソースになります
	 * @return リソース
	 */
	public static <A extends AutoCloseable> Resource<A, A> of(Generator<A, Exception> generator) {
		return new Resource<A, A>(generator, Processor.pipe());
	}

	/** リソースを取得する関数。 */
	private final Generator<A, Exception> resourceGenerator;

	/** リソースを加工する関数。 */
	private final Processor<A, P, Exception> resourceProcessor;

	/**
	 * コンストラクター。
	 * 
	 * @param resourceGenerator リソースを取得する関数
	 * @param resourceProcessor リソースを加工する関数
	 */
	private Resource(Generator<A, Exception> resourceGenerator, Processor<A, P, Exception> resourceProcessor) {
		this.resourceGenerator = resourceGenerator;
		this.resourceProcessor = resourceProcessor;
	}

	/**
	 * リソースを加工します。
	 * 
	 * <p>
	 * try-with-resourcesのリソースは、あくまで{@link #of(Generator)}の引数になります。
	 * このリソースの加工は、主に{@link #whilePresent(Processor)}を実行するにあたっての前準備です。
	 * </p>
	 * 
	 * @param <PP> 加工後の新しいリソース
	 * @param mapper リソースを加工する関数
	 * @return 新しいリソース
	 */
	public <PP> Resource<A, PP> map(Processor<P, PP, Exception> mapper) {
		return new Resource<>(resourceGenerator, resourceProcessor.andThen(mapper));
	}

	/**
	 * リソースにアクセスした後、リソースを閉じます。
	 * 
	 * @param acceptor 結果を受け取る関数
	 * @throws Exception リソースへのアクセスに失敗した場合
	 */
	public void accept(Acceptor<P, Exception> acceptor) throws Exception {
		try (A resource = resourceGenerator.generate()) {
			acceptor.accept(resourceProcessor.process(resource));
		}
	}

	/**
	 * リソースにアクセスした後、リソースを閉じます。
	 * 
	 * @param <R> リソースから取得した値を処理した結果
	 * @param processor リソースから取得した値を処理する関数
	 * @return 結果
	 * @throws Exception リソースへのアクセスに失敗した場合
	 */
	public <R> R process(Processor<P, R, Exception> processor) throws Exception {
		try (A resource = resourceGenerator.generate()) {
			return processor.process(resourceProcessor.process(resource));
		}
	}

	/**
	 * リソースへの繰り返し操作を開始します。
	 * 
	 * @param <R> リソースに対して繰り返しアクセスする関数の結果
	 * @param processor リソースに対して繰り返しアクセスする関数
	 * @return リソースが結果を返している間、リソースへのアクセスを繰り返すオブジェクト
	 */
	public <R> WhilePresent<R> whilePresent(Processor<P, R, Exception> processor) {
		return new WhilePresent<>(processor);
	}
}