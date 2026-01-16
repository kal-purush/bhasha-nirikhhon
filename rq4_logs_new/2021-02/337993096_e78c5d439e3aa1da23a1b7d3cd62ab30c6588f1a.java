package back;

import java.util.List;
import java.util.Optional;

import dao.Dao;
import entity.Hop;

public class HopsBackMain {

	private static Dao<Hop> hopDao;

	
//	public HopsBackMain() {
//		super();
//		// TODO Auto-generated constructor stub
//	}

	public static void main(String[] args) {
//        Hop hop1 = getHop(getAllHops().size() - 1);
//        System.out.println(hop1);
//        updateHop(hop1, new String[]{"Galaxy", "14.5", "AUS"});
//        saveHop(new Hop("TestHop", "99.9", "PL"));
//        //deleteHop(getHop(2));
//        getAllHops().forEach(hop -> System.out.println(hop.getName()));
    }
    
    public static Hop getHop(long id) {
        Optional<Hop> hop = hopDao.get(id);
        
        return hop.orElseGet(
          () -> new Hop(-1, "non-existing hop", "no-acid-concentrate", "no-country"));
    }
    
    public static List<Hop> getAllHops() {
        return hopDao.getAll();
    }
    
    public static void updateHop(Hop hop, String[] params) {
        hopDao.update(hop, params);
    }
    
    public static void saveHop(Hop hop) {
        hopDao.save(hop);
    }
    
    public static void deleteHop(Hop hop) {
        hopDao.delete(hop);
    }
}