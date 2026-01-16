/**
 * 
 */
package summ.btc;

/**
 * @author wfeng007
 * @date 2016-2-16 下午11:35:07
 */
public class CircularRotatingMap<K, V> extends RotatingMap<K, V>{

//	/**
//	 * @param callback
//	 */
//	public CircularRotatingMap(
//			summ.btc.RotatingMap.ExpiredCallback<K, V> callback) {
//		super(callback);
//	}
//
//	/**
//	 * @param numBuckets
//	 * @param callback
//	 */
//	public CircularRotatingMap(int numBuckets,
//			summ.btc.RotatingMap.ExpiredCallback<K, V> callback) {
//		super(numBuckets, callback);
//	}
//	/**
//	 * @param numBuckets
//	 */
//	public CircularRotatingMap(int numBuckets) {
//		super(numBuckets);
//	}
	
	/**
	 * @param numBuckets
	 */
	public CircularRotatingMap() {
		super(3);
	}

	/* (non-Javadoc)
	 * @see summ.btc.RotatingMap#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(K key, V value) {
		if(super.size()%10==0){
			super.rotate();
		}
		super.put(key, value);
	}
	
	

}