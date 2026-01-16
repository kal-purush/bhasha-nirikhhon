/**
 * WebServiceAnnonceSoapBindingImpl.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package WebService;

public class WebServiceAnnonceSoapBindingImpl implements WebService.WebServiceAnnonce{
    public java.lang.String createAnnonce(java.lang.String nom, int category_id, int telephone, int numero_adresse, java.lang.String rue_adresse, int codePostal_adresse, java.lang.String ville_adresse) throws java.rmi.RemoteException {
        return null;
    }

    public boolean deleteAnnonce(int id) throws java.rmi.RemoteException {
        return false;
    }

    public java.lang.String getAnnoncesCat(int categorie_id) throws java.rmi.RemoteException {
        return null;
    }

    public java.lang.String getAnnonceByID(int id) throws java.rmi.RemoteException {
        return null;
    }

    public java.lang.String getAnnonces() throws java.rmi.RemoteException {
        return null;
    }

    public boolean modifyAnnonce(int annonce_id, java.lang.String nom, int category_id, int telephone, int numero_adresse, java.lang.String rue_adresse, int codePostal_adresse, java.lang.String ville_adresse) throws java.rmi.RemoteException {
        return false;
    }

    public java.lang.String searchAnnonces(int categorie_id, java.lang.String ville, java.lang.String annonce_nom, int departement, boolean sont_recentes) throws java.rmi.RemoteException {
        return null;
    }

}