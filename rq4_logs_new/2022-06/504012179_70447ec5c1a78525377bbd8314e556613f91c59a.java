package mutu.core;

import java.util.Date;
import java.util.List;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import mutu.core.entities.MutuOrders;
import mutu.core.entities.MutuStocks;

@Stateless
public class MutuOrdersBean implements MutuOrdersBeanRemote {

    @PersistenceContext(unitName = "MutuCoreEJBPU")
    private EntityManager em;
    
    @EJB
    private MutuStocksBean _stockDB;

    @Override
    public MutuOrders[] getAllQueuingOrdersByStockId(long _stockId) {
        MutuStocks _stock = _stockDB.getStockByStockId(_stockId);
        
        if(_stock == null){
            return null;
        }
        
        Query _q = em.createQuery("select o from MutuOrders o where o.issettle = false and o.sid = :stock");
        _q.setParameter("stock", _stock);
        List<MutuOrders> rtn = _q.getResultList();
        
        return rtn.toArray(new MutuOrders[rtn.size()]);
    }

    @Override
    public MutuOrders placeOrder(long _stockId, int _qty, double price, boolean _isBuy) {
        //Check for stockId existance
        MutuStocks _stock = _stockDB.getStockByStockId(_stockId);
        
        if(_stock == null){
            return null;
        }
        
        MutuOrders _mo = new MutuOrders(_isBuy, _qty, price, false, new Date(), _stock);
        em.persist(_mo);
        return _mo;
    }
    
    @Override
    public boolean confirmOrder(long _oId) {
        MutuOrders _orders = em.find(MutuOrders.class, _oId);
        if(_orders == null){
            return false;
        }
                
        MutuOrders[] _pairOrders = this.getMatchQueueOrder(_orders);
        if(_pairOrders.length <= 0){
            return false;
        }
        _stockDB.updStockCurrentPrice(_orders.getSid().getSid(), _orders.getPrice());
        _pairOrders[0].setIssettle(true);
        _orders.setIssettle(true);
        return true;
    }
    
    @Override
    public boolean isAlreadySettled(long _oId) {
        MutuOrders _orders = em.find(MutuOrders.class, _oId);
        if(_orders == null){
            return false;
        }     
        return _orders.getIssettle();
    }
    
    @Override
    public MutuOrders getQueuingOrdersByOid(long _oId) {     
        return em.find(MutuOrders.class, _oId);
    }

    private List<MutuOrders> getAllQueuingOrdersListByStocksId(long _stockId, boolean _isBuy){
        MutuStocks _stock = _stockDB.getStockByStockId(_stockId);
        
        if(_stock == null){
            return null;
        }
        
        Query _q = em.createQuery("select o from MutuOrders o where o.issettle = false and o.sid = :stock and o.isbuy = :isBuy");
        _q.setParameter("stock", _stock);
        _q.setParameter("isBuy", _isBuy);
        
        return _q.getResultList();
    }    
        
    @Override
    public MutuOrders[] getMatchQueueOrder(MutuOrders _o){
        MutuStocks _stock = _stockDB.getStockByStockId(_o.getSid().getSid());
        
        if(_stock == null){
            return null;
        }
        
        Query _q = em.createQuery("select o from MutuOrders o where o.issettle = false and o.sid = :stock and o.isbuy = :isBuy and o.price = :price and o.qty = :qty");
        _q.setParameter("stock", _stock);
        _q.setParameter("isBuy", !_o.getIsbuy());
        _q.setParameter("price", _o.getPrice());
        _q.setParameter("qty", _o.getQty());
        
        List<MutuOrders> rtn = _q.getResultList();
        
        return rtn.toArray(new MutuOrders[rtn.size()]);
    }

    @Override
    public MutuOrders[] getBuySideQueueByStockId(long _stockId) {
        List<MutuOrders> rtn = getAllQueuingOrdersListByStocksId(_stockId, true);
        if(rtn == null){
            return null;
        }
        return rtn.toArray(new MutuOrders[rtn.size()]);
    }

    @Override
    public MutuOrders[] getSellSideQueueByStockId(long _stockId) {
        List<MutuOrders> rtn = getAllQueuingOrdersListByStocksId(_stockId, false);
        if(rtn == null){
            return null;
        }
        return rtn.toArray(new MutuOrders[rtn.size()]);
    }
}