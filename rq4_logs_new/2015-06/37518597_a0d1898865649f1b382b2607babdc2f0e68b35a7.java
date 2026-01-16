package org.everit.blobstore.mem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Map that manages its transactional state by enlisting itself if there is an ongoing transaction.
 * The implementation is based on {@link org.apache.commons.transaction.memory.BasicTxMap}.
 *
 * @param <K>
 *          the type of keys maintained by this map
 * @param <V>
 *          the type of mapped values
 */
public class ManagedMap<K, V> implements Map<K, V> {

  // mostly copied from org.apache.commons.collections.map.AbstractHashedMap
  protected static class HashEntry<K, V> implements Entry<K, V> {

    protected K key;

    protected V value;

    protected HashEntry(final K key, final V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Map.Entry)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      Map.Entry<K, V> other = (Map.Entry<K, V>) obj;
      return (getKey() == null ? other.getKey() == null : getKey().equals(other.getKey()))
          && (getValue() == null ? other.getValue() == null : getValue().equals(
              other.getValue()));
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return (getKey() == null ? 0 : getKey().hashCode())
          ^ (getValue() == null ? 0 : getValue().hashCode());
    }

    @Override
    public V setValue(final V value) {
      V old = this.value;
      this.value = value;
      return old;
    }

    @Override
    public String toString() {
      return new StringBuffer().append(getKey()).append('=').append(getValue()).toString();
    }
  }

  protected class MapTxContext implements Map<K, V> {
    protected Map<K, V> adds;

    protected Map<K, V> changes;

    protected boolean cleared;

    protected Set<K> deletes;

    protected boolean readOnly = true;

    public MapTxContext() {
      deletes = new HashSet<K>();
      changes = new HashMap<K, V>();
      adds = new HashMap<K, V>();
      cleared = false;
    }

    @Override
    public void clear() {
      readOnly = false;
      cleared = true;
      deletes.clear();
      changes.clear();
      adds.clear();
    }

    public void commit() {
      if (!isReadOnly()) {

        if (cleared) {
          wrapped.clear();
        }

        wrapped.putAll(changes);
        wrapped.putAll(adds);

        for (Object key : deletes) {
          wrapped.remove(key);
        }
      }
    }

    @Override
    public boolean containsKey(final Object key) {
      return keySet().contains(key);
    }

    @Override
    public boolean containsValue(final Object value) {
      return values().contains(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      Set<Entry<K, V>> entrySet = new HashSet<>();
      // XXX expensive :(
      for (K key : keySet()) {
        V value = get(key);
        // XXX we have no isolation, so get entry might have been
        // deleted in the meantime
        if (value != null) {
          entrySet.add(new HashEntry<K, V>(key, value));
        }
      }
      return entrySet;
    }

    @Override
    public V get(final Object key) {

      if (deletes.contains(key)) {
        // reflects that entry has been deleted in this tx
        return null;
      }

      if (changes.containsKey(key)) {
        return changes.get(key);
      }

      if (adds.containsKey(key)) {
        return adds.get(key);
      }

      if (cleared) {
        return null;
      } else {
        // not modified in this tx
        return wrapped.get(key);
      }
    }

    @Override
    public boolean isEmpty() {
      return (size() == 0);
    }

    public boolean isReadOnly() {
      return readOnly;
    }

    @Override
    public Set<K> keySet() {
      Set<K> keySet = new HashSet<K>();
      if (!cleared) {
        keySet.addAll(wrapped.keySet());
        keySet.removeAll(deletes);
      }
      keySet.addAll(adds.keySet());
      return keySet;
    }

    @Override
    public V put(final K key, final V value) {
      readOnly = false;

      V oldValue = get(key);

      deletes.remove(key);
      if (wrapped.containsKey(key)) {
        changes.put(key, value);
      } else {
        adds.put(key, value);
      }

      return oldValue;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
      for (Object name : map.entrySet()) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map.Entry<K, V> entry = (Map.Entry) name;
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public V remove(final Object key) {
      V oldValue = get(key);

      readOnly = false;
      changes.remove(key);
      adds.remove(key);
      if (wrapped.containsKey(key) && !cleared) {
        @SuppressWarnings("unchecked")
        K typedKey = (K) key;
        deletes.add(typedKey);
      }

      return oldValue;
    }

    @Override
    public int size() {
      int size = (cleared ? 0 : wrapped.size());

      size -= deletes.size();
      size += adds.size();

      return size;
    }

    @Override
    public Collection<V> values() {
      // XXX expensive :(
      Collection<V> values = new ArrayList<V>();
      Set<K> keys = keySet();
      for (K key : keys) {
        V value = get(key);
        // XXX we have no isolation, so entry might have been
        // deleted in the meantime
        if (value != null) {
          values.add(value);
        }
      }
      return values;
    }

  }

  /**
   * XAResource to manage the map.
   */
  private class MapXAResource implements XAResource {

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      Transaction transaction = handleTransactionState();
      getActiveTx().commit();
      setActiveTx(null);
      transactionOfWrappedMapTL.set(null);
      enlistedTransactions.remove(transaction);
    }

    @Override
    public void end(final Xid xid, final int flags) throws XAException {
      // Do nothing as only commit and fail are handled.
    }

    @Override
    public void forget(final Xid xid) {
    }

    protected ManagedMap<K, V> getManagedMap() {
      return ManagedMap.this;
    }

    @Override
    public int getTransactionTimeout() {
      // TODO Auto-generated method stub
      return 600;
    }

    @Override
    public boolean isSameRM(final XAResource xares) {
      if (xares instanceof ManagedMap.MapXAResource) {
        @SuppressWarnings("rawtypes")
        ManagedMap.MapXAResource mapXares = (ManagedMap.MapXAResource) xares;
        return getManagedMap().equals(mapXares.getManagedMap());
      }
      return false;
    }

    @Override
    public int prepare(final Xid xid) throws XAException {
      return XAResource.XA_OK;
    }

    @Override
    public Xid[] recover(final int flag) throws XAException {
      return new Xid[0];
    }

    @Override
    public void rollback(final Xid xid) throws XAException {
      Transaction transaction = handleTransactionState();
      setActiveTx(null);
      transactionOfWrappedMapTL.set(null);
      enlistedTransactions.remove(transaction);
    }

    @Override
    public boolean setTransactionTimeout(final int seconds) {
      return false;
    }

    @Override
    public void start(final Xid xid, final int flags) throws XAException {
    }

  }

  protected ThreadLocal<MapTxContext> activeTx = new ThreadLocal<>();

  protected final Map<Transaction, Boolean> enlistedTransactions = new WeakHashMap<>();

  protected final XAResource mapXAResource = new MapXAResource();

  protected final Map<Object, MapTxContext> suspendedTXContexts = new ConcurrentHashMap<>();

  protected final TransactionManager transactionManager;

  protected final ThreadLocal<Transaction> transactionOfWrappedMapTL = new ThreadLocal<>();

  protected final ConcurrentMap<K, V> wrapped;

  public ManagedMap(final TransactionManager transactionManager) {
    this(transactionManager, null);
  }

  public ManagedMap(final TransactionManager transactionManager,
      final ConcurrentMap<K, V> wrapped) {
    this.transactionManager = transactionManager;
    if (wrapped != null) {
      this.wrapped = wrapped;
    } else {
      this.wrapped = new ConcurrentHashMap<>();
    }
  }

  @Override
  public void clear() {
    handleTransactionState();
    coalesceActiveTxOrWrapped().clear();
  }

  protected Map<K, V> coalesceActiveTxOrWrapped() {
    ManagedMap<K, V>.MapTxContext txContext = getActiveTx();
    return (txContext != null) ? txContext : wrapped;
  }

  @Override
  public boolean containsKey(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().entrySet();
  }

  @Override
  public V get(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().get(key);
  }

  protected MapTxContext getActiveTx() {
    return activeTx.get();
  }

  protected Transaction handleTransactionState() {
    try {
      int status = transactionManager.getStatus();
      if (status == Status.STATUS_NO_TRANSACTION) {
        Transaction transactionOfWrappedMap = transactionOfWrappedMapTL.get();
        if (transactionOfWrappedMap != null) {
          suspendTransaction(transactionOfWrappedMap);
        }
      } else {
        Transaction transactionOfWrappedMap = transactionOfWrappedMapTL.get();
        Transaction currentTransaction = transactionManager.getTransaction();
        if (transactionOfWrappedMap != null
            && !transactionOfWrappedMap.equals(currentTransaction)) {
          suspendTransaction(transactionOfWrappedMap);
        }

        if (transactionOfWrappedMap == null
            || !transactionOfWrappedMap.equals(currentTransaction)) {

          Boolean enlisted = enlistedTransactions.get(currentTransaction);
          if (enlisted != null) {
            resumeTransaction(currentTransaction);
            transactionOfWrappedMapTL.set(currentTransaction);
          } else {
            try {
              currentTransaction.enlistResource(mapXAResource);
            } catch (RollbackException e) {
              throw new UncheckedRollbackException(e);
            }
            transactionOfWrappedMapTL.set(currentTransaction);
            startTransaction();
            enlistedTransactions.put(currentTransaction, true);
          }
        }
        return currentTransaction;
      }
      return null;
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

  }

  @Override
  public boolean isEmpty() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().isEmpty();
  }

  @Override
  public Set<K> keySet() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().keySet();
  }

  @Override
  public V put(final K key, final V value) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    handleTransactionState();
    coalesceActiveTxOrWrapped().putAll(m);
  }

  @Override
  public V remove(final Object key) {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().remove(key);
  }

  protected void resumeTransaction(final Transaction currentTransaction) {
    MapTxContext txContext = suspendedTXContexts.remove(currentTransaction);
    Objects.requireNonNull(txContext);
    setActiveTx(txContext);
  }

  protected void setActiveTx(final MapTxContext mapContext) {
    activeTx.set(mapContext);
  }

  @Override
  public int size() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().size();
  }

  protected void startTransaction() {
    setActiveTx(new MapTxContext());
  }

  protected void suspendTransaction(final Transaction transactionOfWrappedMap) {
    MapTxContext activeTx = getActiveTx();
    Objects.requireNonNull(activeTx, "No active map context for transaction.");
    suspendedTXContexts.put(transactionOfWrappedMap, activeTx);
    setActiveTx(null);
  }

  @Override
  public Collection<V> values() {
    handleTransactionState();
    return coalesceActiveTxOrWrapped().values();
  }

}