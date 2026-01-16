package co.com.homologacionesu.jpacontroller;

import java.io.Serializable;
import javax.persistence.Query;
import javax.persistence.EntityNotFoundException;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import co.com.homologacionesu.entidades.TblEstado;
import co.com.homologacionesu.entidades.TblProgramas;
import co.com.homologacionesu.entidades.TblHomologacion;
import co.com.homologacionesu.entidades.TblPlanPrograma;
import co.com.homologacionesu.jpacontroller.exceptions.IllegalOrphanException;
import co.com.homologacionesu.jpacontroller.exceptions.NonexistentEntityException;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

/**
 * Objetivo: Realizar las diferentes operaciones para la entidad
 * @author daserna
 */
public class TblPlanProgramaJpaController implements Serializable {

    public TblPlanProgramaJpaController(EntityManagerFactory emf) {
        this.emf = emf;
    }
    private EntityManagerFactory emf = null;

    public EntityManager getEntityManager() {
        return emf.createEntityManager();
    }

    public void create(TblPlanPrograma tblPlanPrograma) {
        if (tblPlanPrograma.getTblHomologacionList() == null) {
            tblPlanPrograma.setTblHomologacionList(new ArrayList<TblHomologacion>());
        }
        if (tblPlanPrograma.getTblHomologacionList1() == null) {
            tblPlanPrograma.setTblHomologacionList1(new ArrayList<TblHomologacion>());
        }
        EntityManager em = null;
        try {
            em = getEntityManager();
            em.getTransaction().begin();
            TblEstado idEstado = tblPlanPrograma.getIdEstado();
            if (idEstado != null) {
                idEstado = em.getReference(idEstado.getClass(), idEstado.getIdEstado());
                tblPlanPrograma.setIdEstado(idEstado);
            }
            TblProgramas idPrograma = tblPlanPrograma.getIdPrograma();
            if (idPrograma != null) {
                idPrograma = em.getReference(idPrograma.getClass(), idPrograma.getIdPrograma());
                tblPlanPrograma.setIdPrograma(idPrograma);
            }
            List<TblHomologacion> attachedTblHomologacionList = new ArrayList<>();
            for (TblHomologacion tblHomologacionListTblHomologacionToAttach : tblPlanPrograma.getTblHomologacionList()) {
                tblHomologacionListTblHomologacionToAttach = em.getReference(tblHomologacionListTblHomologacionToAttach.getClass(), tblHomologacionListTblHomologacionToAttach.getIdHomologa());
                attachedTblHomologacionList.add(tblHomologacionListTblHomologacionToAttach);
            }
            tblPlanPrograma.setTblHomologacionList(attachedTblHomologacionList);
            List<TblHomologacion> attachedTblHomologacionList1 = new ArrayList<>();
            for (TblHomologacion tblHomologacionList1TblHomologacionToAttach : tblPlanPrograma.getTblHomologacionList1()) {
                tblHomologacionList1TblHomologacionToAttach = em.getReference(tblHomologacionList1TblHomologacionToAttach.getClass(), tblHomologacionList1TblHomologacionToAttach.getIdHomologa());
                attachedTblHomologacionList1.add(tblHomologacionList1TblHomologacionToAttach);
            }
            tblPlanPrograma.setTblHomologacionList1(attachedTblHomologacionList1);
            em.persist(tblPlanPrograma);
            if (idEstado != null) {
                idEstado.getTblPlanProgramaList().add(tblPlanPrograma);
                idEstado = em.merge(idEstado);
            }
            if (idPrograma != null) {
                idPrograma.getTblPlanProgramaList().add(tblPlanPrograma);
                idPrograma = em.merge(idPrograma);
            }
            for (TblHomologacion tblHomologacionListTblHomologacion : tblPlanPrograma.getTblHomologacionList()) {
                TblPlanPrograma oldPlanOrigenOfTblHomologacionListTblHomologacion = tblHomologacionListTblHomologacion.getPlanOrigen();
                tblHomologacionListTblHomologacion.setPlanOrigen(tblPlanPrograma);
                tblHomologacionListTblHomologacion = em.merge(tblHomologacionListTblHomologacion);
                if (oldPlanOrigenOfTblHomologacionListTblHomologacion != null) {
                    oldPlanOrigenOfTblHomologacionListTblHomologacion.getTblHomologacionList().remove(tblHomologacionListTblHomologacion);
                    oldPlanOrigenOfTblHomologacionListTblHomologacion = em.merge(oldPlanOrigenOfTblHomologacionListTblHomologacion);
                }
            }
            for (TblHomologacion tblHomologacionList1TblHomologacion : tblPlanPrograma.getTblHomologacionList1()) {
                TblPlanPrograma oldPlanDestinoOfTblHomologacionList1TblHomologacion = tblHomologacionList1TblHomologacion.getPlanDestino();
                tblHomologacionList1TblHomologacion.setPlanDestino(tblPlanPrograma);
                tblHomologacionList1TblHomologacion = em.merge(tblHomologacionList1TblHomologacion);
                if (oldPlanDestinoOfTblHomologacionList1TblHomologacion != null) {
                    oldPlanDestinoOfTblHomologacionList1TblHomologacion.getTblHomologacionList1().remove(tblHomologacionList1TblHomologacion);
                    oldPlanDestinoOfTblHomologacionList1TblHomologacion = em.merge(oldPlanDestinoOfTblHomologacionList1TblHomologacion);
                }
            }
            em.getTransaction().commit();
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }

    public void edit(TblPlanPrograma tblPlanPrograma) throws IllegalOrphanException, NonexistentEntityException, Exception {
        EntityManager em = null;
        try {
            em = getEntityManager();
            em.getTransaction().begin();
            TblPlanPrograma persistentTblPlanPrograma = em.find(TblPlanPrograma.class, tblPlanPrograma.getIdPlanPrograma());
            TblEstado idEstadoOld = persistentTblPlanPrograma.getIdEstado();
            TblEstado idEstadoNew = tblPlanPrograma.getIdEstado();
            TblProgramas idProgramaOld = persistentTblPlanPrograma.getIdPrograma();
            TblProgramas idProgramaNew = tblPlanPrograma.getIdPrograma();
            List<TblHomologacion> tblHomologacionListOld = persistentTblPlanPrograma.getTblHomologacionList();
            List<TblHomologacion> tblHomologacionListNew = tblPlanPrograma.getTblHomologacionList();
            List<TblHomologacion> tblHomologacionList1Old = persistentTblPlanPrograma.getTblHomologacionList1();
            List<TblHomologacion> tblHomologacionList1New = tblPlanPrograma.getTblHomologacionList1();
            List<String> illegalOrphanMessages = null;
            for (TblHomologacion tblHomologacionListOldTblHomologacion : tblHomologacionListOld) {
                if (!tblHomologacionListNew.contains(tblHomologacionListOldTblHomologacion)) {
                    if (illegalOrphanMessages == null) {
                        illegalOrphanMessages = new ArrayList<>();
                    }
                    illegalOrphanMessages.add("You must retain TblHomologacion " + tblHomologacionListOldTblHomologacion + " since its planOrigen field is not nullable.");
                }
            }
            for (TblHomologacion tblHomologacionList1OldTblHomologacion : tblHomologacionList1Old) {
                if (!tblHomologacionList1New.contains(tblHomologacionList1OldTblHomologacion)) {
                    if (illegalOrphanMessages == null) {
                        illegalOrphanMessages = new ArrayList<>();
                    }
                    illegalOrphanMessages.add("You must retain TblHomologacion " + tblHomologacionList1OldTblHomologacion + " since its planDestino field is not nullable.");
                }
            }
            if (illegalOrphanMessages != null) {
                throw new IllegalOrphanException(illegalOrphanMessages);
            }
            if (idEstadoNew != null) {
                idEstadoNew = em.getReference(idEstadoNew.getClass(), idEstadoNew.getIdEstado());
                tblPlanPrograma.setIdEstado(idEstadoNew);
            }
            if (idProgramaNew != null) {
                idProgramaNew = em.getReference(idProgramaNew.getClass(), idProgramaNew.getIdPrograma());
                tblPlanPrograma.setIdPrograma(idProgramaNew);
            }
            List<TblHomologacion> attachedTblHomologacionListNew = new ArrayList<>();
            for (TblHomologacion tblHomologacionListNewTblHomologacionToAttach : tblHomologacionListNew) {
                tblHomologacionListNewTblHomologacionToAttach = em.getReference(tblHomologacionListNewTblHomologacionToAttach.getClass(), tblHomologacionListNewTblHomologacionToAttach.getIdHomologa());
                attachedTblHomologacionListNew.add(tblHomologacionListNewTblHomologacionToAttach);
            }
            tblHomologacionListNew = attachedTblHomologacionListNew;
            tblPlanPrograma.setTblHomologacionList(tblHomologacionListNew);
            List<TblHomologacion> attachedTblHomologacionList1New = new ArrayList<>();
            for (TblHomologacion tblHomologacionList1NewTblHomologacionToAttach : tblHomologacionList1New) {
                tblHomologacionList1NewTblHomologacionToAttach = em.getReference(tblHomologacionList1NewTblHomologacionToAttach.getClass(), tblHomologacionList1NewTblHomologacionToAttach.getIdHomologa());
                attachedTblHomologacionList1New.add(tblHomologacionList1NewTblHomologacionToAttach);
            }
            tblHomologacionList1New = attachedTblHomologacionList1New;
            tblPlanPrograma.setTblHomologacionList1(tblHomologacionList1New);
            tblPlanPrograma = em.merge(tblPlanPrograma);
            if (idEstadoOld != null && !idEstadoOld.equals(idEstadoNew)) {
                idEstadoOld.getTblPlanProgramaList().remove(tblPlanPrograma);
                idEstadoOld = em.merge(idEstadoOld);
            }
            if (idEstadoNew != null && !idEstadoNew.equals(idEstadoOld)) {
                idEstadoNew.getTblPlanProgramaList().add(tblPlanPrograma);
                idEstadoNew = em.merge(idEstadoNew);
            }
            if (idProgramaOld != null && !idProgramaOld.equals(idProgramaNew)) {
                idProgramaOld.getTblPlanProgramaList().remove(tblPlanPrograma);
                idProgramaOld = em.merge(idProgramaOld);
            }
            if (idProgramaNew != null && !idProgramaNew.equals(idProgramaOld)) {
                idProgramaNew.getTblPlanProgramaList().add(tblPlanPrograma);
                idProgramaNew = em.merge(idProgramaNew);
            }
            for (TblHomologacion tblHomologacionListNewTblHomologacion : tblHomologacionListNew) {
                if (!tblHomologacionListOld.contains(tblHomologacionListNewTblHomologacion)) {
                    TblPlanPrograma oldPlanOrigenOfTblHomologacionListNewTblHomologacion = tblHomologacionListNewTblHomologacion.getPlanOrigen();
                    tblHomologacionListNewTblHomologacion.setPlanOrigen(tblPlanPrograma);
                    tblHomologacionListNewTblHomologacion = em.merge(tblHomologacionListNewTblHomologacion);
                    if (oldPlanOrigenOfTblHomologacionListNewTblHomologacion != null && !oldPlanOrigenOfTblHomologacionListNewTblHomologacion.equals(tblPlanPrograma)) {
                        oldPlanOrigenOfTblHomologacionListNewTblHomologacion.getTblHomologacionList().remove(tblHomologacionListNewTblHomologacion);
                        oldPlanOrigenOfTblHomologacionListNewTblHomologacion = em.merge(oldPlanOrigenOfTblHomologacionListNewTblHomologacion);
                    }
                }
            }
            for (TblHomologacion tblHomologacionList1NewTblHomologacion : tblHomologacionList1New) {
                if (!tblHomologacionList1Old.contains(tblHomologacionList1NewTblHomologacion)) {
                    TblPlanPrograma oldPlanDestinoOfTblHomologacionList1NewTblHomologacion = tblHomologacionList1NewTblHomologacion.getPlanDestino();
                    tblHomologacionList1NewTblHomologacion.setPlanDestino(tblPlanPrograma);
                    tblHomologacionList1NewTblHomologacion = em.merge(tblHomologacionList1NewTblHomologacion);
                    if (oldPlanDestinoOfTblHomologacionList1NewTblHomologacion != null && !oldPlanDestinoOfTblHomologacionList1NewTblHomologacion.equals(tblPlanPrograma)) {
                        oldPlanDestinoOfTblHomologacionList1NewTblHomologacion.getTblHomologacionList1().remove(tblHomologacionList1NewTblHomologacion);
                        oldPlanDestinoOfTblHomologacionList1NewTblHomologacion = em.merge(oldPlanDestinoOfTblHomologacionList1NewTblHomologacion);
                    }
                }
            }
            em.getTransaction().commit();
        } catch (IllegalOrphanException ex) {
            String msg = ex.getLocalizedMessage();
            if (msg == null || msg.length() == 0) {
                Integer id = tblPlanPrograma.getIdPlanPrograma();
                if (findTblPlanPrograma(id) == null) {
                    throw new NonexistentEntityException("The tblPlanPrograma with id " + id + " no longer exists.");
                }
            }
            throw ex;
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }

    public void destroy(Integer id) throws IllegalOrphanException, NonexistentEntityException {
        EntityManager em = null;
        try {
            em = getEntityManager();
            em.getTransaction().begin();
            TblPlanPrograma tblPlanPrograma;
            try {
                tblPlanPrograma = em.getReference(TblPlanPrograma.class, id);
                tblPlanPrograma.getIdPlanPrograma();
            } catch (EntityNotFoundException enfe) {
                throw new NonexistentEntityException("The tblPlanPrograma with id " + id + " no longer exists.", enfe);
            }
            List<String> illegalOrphanMessages = null;
            List<TblHomologacion> tblHomologacionListOrphanCheck = tblPlanPrograma.getTblHomologacionList();
            for (TblHomologacion tblHomologacionListOrphanCheckTblHomologacion : tblHomologacionListOrphanCheck) {
                if (illegalOrphanMessages == null) {
                    illegalOrphanMessages = new ArrayList<>();
                }
                illegalOrphanMessages.add("This TblPlanPrograma (" + tblPlanPrograma + ") cannot be destroyed since the TblHomologacion " + tblHomologacionListOrphanCheckTblHomologacion + " in its tblHomologacionList field has a non-nullable planOrigen field.");
            }
            List<TblHomologacion> tblHomologacionList1OrphanCheck = tblPlanPrograma.getTblHomologacionList1();
            for (TblHomologacion tblHomologacionList1OrphanCheckTblHomologacion : tblHomologacionList1OrphanCheck) {
                if (illegalOrphanMessages == null) {
                    illegalOrphanMessages = new ArrayList<>();
                }
                illegalOrphanMessages.add("This TblPlanPrograma (" + tblPlanPrograma + ") cannot be destroyed since the TblHomologacion " + tblHomologacionList1OrphanCheckTblHomologacion + " in its tblHomologacionList1 field has a non-nullable planDestino field.");
            }
            if (illegalOrphanMessages != null) {
                throw new IllegalOrphanException(illegalOrphanMessages);
            }
            TblEstado idEstado = tblPlanPrograma.getIdEstado();
            if (idEstado != null) {
                idEstado.getTblPlanProgramaList().remove(tblPlanPrograma);
                idEstado = em.merge(idEstado);
            }
            TblProgramas idPrograma = tblPlanPrograma.getIdPrograma();
            if (idPrograma != null) {
                idPrograma.getTblPlanProgramaList().remove(tblPlanPrograma);
                idPrograma = em.merge(idPrograma);
            }
            em.remove(tblPlanPrograma);
            em.getTransaction().commit();
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }

    public List<TblPlanPrograma> findTblPlanProgramaEntities() {
        return findTblPlanProgramaEntities(true, -1, -1);
    }

    public List<TblPlanPrograma> findTblPlanProgramaEntities(int maxResults, int firstResult) {
        return findTblPlanProgramaEntities(false, maxResults, firstResult);
    }

    private List<TblPlanPrograma> findTblPlanProgramaEntities(boolean all, int maxResults, int firstResult) {
        EntityManager em = getEntityManager();
        try {
            CriteriaQuery cq = em.getCriteriaBuilder().createQuery();
            cq.select(cq.from(TblPlanPrograma.class));
            Query q = em.createQuery(cq);
            if (!all) {
                q.setMaxResults(maxResults);
                q.setFirstResult(firstResult);
            }
            return q.getResultList();
        } finally {
            em.close();
        }
    }

    public TblPlanPrograma findTblPlanPrograma(Integer id) {
        EntityManager em = getEntityManager();
        try {
            return em.find(TblPlanPrograma.class, id);
        } finally {
            em.close();
        }
    }

    public int getTblPlanProgramaCount() {
        EntityManager em = getEntityManager();
        try {
            CriteriaQuery cq = em.getCriteriaBuilder().createQuery();
            Root<TblPlanPrograma> rt = cq.from(TblPlanPrograma.class);
            cq.select(em.getCriteriaBuilder().count(rt));
            Query q = em.createQuery(cq);
            return ((Long) q.getSingleResult()).intValue();
        } finally {
            em.close();
        }
    }
    
}