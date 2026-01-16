package siit.finalProject.VehicleManagement.service;

import org.apache.commons.codec.digest.DigestUtils;

public class EncodePassword {
    /**
     * @param pass
     * @return
     * @throws RuntimeException
     */
    public String encodePass(String pass) throws RuntimeException {
        try {
            return DigestUtils.md5Hex(pass);
        } catch (Exception ex) {
            throw new RuntimeException("Encoding problem");
        }
    }

    // TODO !!! delete this !!!
    public static void main(String[] args) {
              EncodePassword encode = new EncodePassword();
        String encoded = encode.encodePass("andu");
        System.out.println(encoded);
    }
}