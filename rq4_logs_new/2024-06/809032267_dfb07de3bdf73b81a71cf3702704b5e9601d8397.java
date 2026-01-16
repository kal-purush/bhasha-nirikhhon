/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template


 */
package TanggalHelperTest;
import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.mycompany.tp_mod_10_1302213089_muhammadvikhanmuharram.TanggalHelper;

/**
 *
 * @author hp
 */
public class TanggalHelperTest {
    @Test
    public void testTambahMenit() {
        LocalDateTime waktu = LocalDateTime.of(2023, 6, 1, 12, 0, 0);
        String expected = "2023-06-01 12:10:00";
        String actual = TanggalHelper.tambahMenit(waktu, 10);
        assertEquals(expected, actual);
    }

    @Test
    public void testTambahJam() {
        LocalDateTime waktu = LocalDateTime.of(2023, 6, 1, 12, 0, 0);
        String expected = "2023-06-01 15:00:00";
        String actual = TanggalHelper.tambahJam(waktu, 3);
        assertEquals(expected, actual);
    }
}