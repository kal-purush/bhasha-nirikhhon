package mx.com.lania.oamtemplate.Database.Dao.EjercicioPerpetuo;

import android.arch.persistence.room.Dao;
import android.arch.persistence.room.Insert;
import android.arch.persistence.room.OnConflictStrategy;
import android.arch.persistence.room.Query;

import mx.com.lania.oamtemplate.Database.Entities.EjercicioPerpetuo.Acreedores;

@Dao
public interface AcreedoresDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    public void insertAcreedores(Acreedores acreedores);

    @Query("SELECT * FROM Acreedores")
    Acreedores getAcreedor();
}