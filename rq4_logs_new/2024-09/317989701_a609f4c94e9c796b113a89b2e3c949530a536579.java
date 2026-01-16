/*
 * Copyright © 2023-2025 Cloud Software Group, Inc.
 * */
/**
 * Locking implementation class which uses MongoDB store as the lock provider
 * 
 * */
package com.tibco.be.mongoDB;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoWriteException;
import com.mongodb.client.result.DeleteResult;
import com.tibco.cep.kernel.service.logging.Level;
import com.tibco.cep.store.locking.AbstractLockProvider;
import com.tibco.cep.store.locking.LockEntry;

public class MongoDBLockProvider extends AbstractLockProvider {

	public final static MongoDBLockProvider INSTANCE = new MongoDBLockProvider();
	private MongoDBStoreProvider storeProvider;
	
	private MongoDBLockProvider() {
		this.storeProvider = (MongoDBStoreProvider) MongoDBStoreProvider.getStoreProviderInstance();
	}

	@Override
	public boolean unlock(Object key) {
		logger.log(Level.DEBUG, "MongoDB store lock :: Attempting to unlock");
		boolean isUnlocked = false;
		Document document = new Document();
		document.append(MongoDBConstants.PROPERTY_KEY_MONGODB_ID, key.toString());
		document.append(MongoDBConstants.DOCUMENT_LOCKS_MEMBERID_FIELD, memberId);
		DeleteResult result;
		try {
			result = storeProvider.deleteWithFilter(document, MongoDBConstants.LOCKS_COLLECTION_NAME);
			isUnlocked = result.wasAcknowledged();
		} catch (Exception e) {
			logger.log(Level.ERROR, "MongoDB store lock :: Failed while trying to unlock", e);
			isUnlocked = false;
		}
		return isUnlocked;
	}

	/**
	 * This method will be used to release all the locks held by a member when it gets killed
	 * */
	@Override
	public void unlockOnMemberId(String memberId) {
		logger.log(Level.DEBUG, "MongoDB store lock :: Engine shutting down. Releasing all the locks held by " + memberId);
		Document document = new Document();
		document.append(MongoDBConstants.DOCUMENT_LOCKS_MEMBERID_FIELD, memberId);
		try {
			storeProvider.deleteManyWithFilter(document, MongoDBConstants.LOCKS_COLLECTION_NAME);
		} catch (Exception e) {
			logger.log(Level.ERROR, "MongoDB store lock :: Failed while trying to unlockOnMemberId", e);
		}
	}

	@Override
	protected LockEntry getLockEntry(Object key) throws Exception {
		// Fetch entry from locks table based on the key
		Document document = new Document();
		document.append(MongoDBConstants.DOCUMENT_LOCKS_KEY_FIELD, key.toString());
		Document result = storeProvider.readWithFilter(document, MongoDBConstants.LOCKS_COLLECTION_NAME);
		if (null != result) {
			LockEntry lockEntry = new LockEntry(result.get(MongoDBConstants.DOCUMENT_LOCKS_KEY_FIELD).toString(),
											result.get(MongoDBConstants.DOCUMENT_LOCKS_MEMBERID_FIELD).toString(),
											result.get(MongoDBConstants.DOCUMENT_LOCKS_SOCKET_FIELD).toString());
			return lockEntry;
		} else {
			return null;
		}
		
	}

	@Override
	protected boolean addLockEntry(LockEntry entry) throws Exception {
		return insertOrUpdateLockEntry(entry, null);
	}

	@Override
	protected boolean updateLockEntry(LockEntry entry, String oldMemberId) throws Exception {
		return insertOrUpdateLockEntry(entry, oldMemberId);
	}
	
	private boolean insertOrUpdateLockEntry(LockEntry entry, String oldMemberId) throws Exception {
		boolean isSuccess = false;
		
		try {
			storeProvider.startTransaction();
			if (oldMemberId == null || (oldMemberId != null && 
							oldMemberId.equalsIgnoreCase(entry.getMemberId())) ) {
				// Create the document
				Document document = new Document();
				document.append(MongoDBConstants.PROPERTY_KEY_MONGODB_ID, entry.getKey());
				document.append(MongoDBConstants.DOCUMENT_LOCKS_MEMBERID_FIELD, entry.getMemberId());
				document.append(MongoDBConstants.DOCUMENT_LOCKS_SOCKET_FIELD, entry.getSocket());
				
				if (oldMemberId == null) {
					// insert new entry
					
					isSuccess = storeProvider.addOrUpdateEntryWithFilter(document, null, MongoDBConstants.LOCKS_COLLECTION_NAME, true);
				} else {
					// update old entry only if the filter matches, set isInsert = false
					// don't add a new entry if the filter does not match (upsert = false)
					
					// Create a filter for the update operation
					List<Bson> filters = new ArrayList<Bson>();
					filters.add(eq(MongoDBConstants.DOCUMENT_LOCKS_KEY_FIELD, entry.getKey()));
					filters.add(eq(MongoDBConstants.DOCUMENT_LOCKS_MEMBERID_FIELD, oldMemberId));
					filters.add(eq(MongoDBConstants.DOCUMENT_LOCKS_SOCKET_FIELD, entry.getSocket()));
					
					isSuccess = storeProvider.addOrUpdateEntryWithFilter(document, filters, MongoDBConstants.LOCKS_COLLECTION_NAME, false);
				}
				
				if (isSuccess)
					storeProvider.commit();
				else
					storeProvider.rollback();
			} else {
				isSuccess = false;
			}
		} catch (MongoWriteException mwe) {
			logger.log(Level.DEBUG, "MongoDB store lock :: Cannot insert duplicate entry in Mongo DB store", mwe);
			storeProvider.rollback();
			isSuccess = false;
		} catch (Exception e) {
			logger.log(Level.DEBUG, "MongoDB store lock :: Failed while trying to insert/update entry in Mongo DB store", e);
			storeProvider.rollback();
			isSuccess = false;
		} finally {
			storeProvider.endTransaction();
		}
		return isSuccess;
	}
}