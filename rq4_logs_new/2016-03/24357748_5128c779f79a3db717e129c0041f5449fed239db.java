package jp.co.dk.datastoremanager.exception;

import java.util.List;

import jp.co.dk.logger.Logger;
import jp.co.dk.logger.LoggerFactory;
import jp.co.dk.message.MessageInterface;
import jp.co.dk.message.exception.AbstractMessageException;

/**
 * DataStoreManagerExceptionは、データストアへの操作にて例外が発生したことを通知する例外クラスです。
 * 
 * @version 1.0
 * @author D.Kanno
 */
public class DataStoreExporterException extends AbstractMessageException{
	
	/**
	 * シリアルバージョンID
	 */
	private static final long serialVersionUID = -3584469327245160649L;
	
	/** ロガーインスタンス */
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージで例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg){
		super(msg);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージで例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param str メッセージ埋め込み文字列
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, String str){
		super(msg, str);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、埋め込み文字列一覧で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param list メッセージ埋め込み文字列一覧
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, List<String> list){
		super(msg, list);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、埋め込み文字列一覧で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param str メッセージ埋め込み文字列一覧
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, String[] str){
		super(msg, str);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、例外で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param throwable 例外インスタンス
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, Throwable throwable){
		super(msg, throwable);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、埋め込み文字列、例外で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param str メッセージ埋め込み文字列
	 * @param throwable 例外インスタンス
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, String str, Throwable throwable){
		super(msg, str, throwable);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、埋め込み文字列一覧、例外で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param list メッセージ埋め込み文字列一覧
	 * @param throwable 例外インスタンス
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, List<String> list,Throwable throwable){
		super(msg, list,throwable);
		this.logger.error(this.getMessage());
	}
	
	/**
	 * コンストラクタ<p>
	 * 
	 * 指定のメッセージ、埋め込み文字列一覧、例外で例外を生成します。
	 * 
	 * @param msg メッセージ定数インスタンス
	 * @param str メッセージ埋め込み文字列一覧
	 * @param throwable 例外インスタンス
	 * @since 1.0
	 */
	public DataStoreExporterException(MessageInterface msg, String[] str, Throwable throwable){
		super(msg, str, throwable);
		this.logger.error(this.getMessage());
	}
}