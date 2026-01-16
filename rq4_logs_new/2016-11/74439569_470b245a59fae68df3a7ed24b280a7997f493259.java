package com.imut.bll;

import com.imut.model.CollectLink;
import com.imut.bll.CollectLinkbll;
import com.imut.dal.CollectLinkdal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * CollectionService瀹炵幇
 */
public class CollectLinkbll {



	/**
	 * 娣诲姞瀹炰綋
	 * 
	 * @param Collection锛岃
	 *            娣诲姞鐨凢unction瀵硅薄
	 * @throws Exception
	 */
	public static boolean save(CollectLink collectLink) throws Exception {
		// TODO Auto-generated method stub

		return CollectLinkdal.save(collectLink);

	}

	
}