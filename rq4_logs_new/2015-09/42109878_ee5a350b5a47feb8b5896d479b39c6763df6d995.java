/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.job.persistence.service.impl;

import com.job.persistence.dao.IEntrepriseDao;
import com.job.persistence.dao.IJobSeekerDao;
import com.job.persistence.dao.IPlacementDao;
import com.job.persistence.model.Entreprise;
import com.job.persistence.model.JobSeeker;
import com.job.persistence.model.Placement;
import com.job.persistence.service.IPlacementService;
import com.job.persistence.service.common.AbstractService;
import java.util.Date;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Service;

/**
 *
 * @author Brice GUEMKAM <briceguemkam@gmail.com>
 */
@Service("placementService")
public class PlacementService extends AbstractService<Placement> implements IPlacementService
{

    @Autowired
    IPlacementDao placementDao;

    @Autowired
    IEntrepriseDao entrepriseDao;
    @Autowired
    IJobSeekerDao jobSeekerDao;

    @Override
    protected PagingAndSortingRepository<Placement, Long> getDao()
    {
        return placementDao;
    }

    @Override
    public Placement create(Placement placement)
    {
        final Entreprise entreprise = entrepriseDao.findOne(placement.getEntreprise().getId());
        final JobSeeker jobSeeker = jobSeekerDao.findOne(placement.getJobSeeker().getId());
        if (placement.getDateFin().after(new Date()))
        {
            jobSeeker.setStatut("Indisponible");
            jobSeekerDao.save(jobSeeker);
        }
        placement.setJobSeeker(jobSeeker);
        placement.setEntreprise(entreprise);
        return placementDao.save(placement);
    }

    @Override
    public Placement update(Placement placement)
    {
        final Entreprise entreprise = entrepriseDao.findOne(placement.getEntreprise().getId());
        final JobSeeker jobSeeker = jobSeekerDao.findOne(placement.getJobSeeker().getId());
        final Placement toUpdate = placementDao.findOne(placement.getId());
        toUpdate.setEntreprise(entreprise);
        toUpdate.setJobSeeker(jobSeeker);
        toUpdate.setDateDebut(placement.getDateDebut());
        toUpdate.setDateFin(placement.getDateFin());
        toUpdate.setObservation(placement.getObservation());
        toUpdate.setStatut(placement.getStatut());
        toUpdate.setTauxHoraire(placement.getTauxHoraire());
        toUpdate.setNombreDheureParJour(placement.getNombreDheureParJour());
        if (placement.getDateFin().after(new Date()))
        {
            jobSeeker.setStatut("Indisponible");
            jobSeekerDao.save(jobSeeker);
        }
        else
        {
            jobSeeker.setStatut("Disponible");
            jobSeekerDao.save(jobSeeker);
        }
        placement.setJobSeeker(jobSeeker);
        placement.setEntreprise(entreprise);
        return placementDao.save(placement);
    }

    @Override
    public Page<Placement> search(long idEntreprise, String nomChercheur, String prenomChercheur, String statut, Date dateDebut, Date dateFin, int page, Integer size)
    {
        String param = "idEntre=" + idEntreprise + " nom=" + nomChercheur + " statut=" + statut + " dateDebut=" + dateDebut + " dateFin=" + dateFin;
        System.out.println("dans la couche service voici les param \n" + param);
        if (idEntreprise == -1)
        {
            return placementDao.search("%" + nomChercheur + "%", "%" + statut + "%", dateDebut, dateFin, new PageRequest(page, size));
        }
        else
        {
            return placementDao.search(idEntreprise, "%" + nomChercheur + "%", "%" + statut + "%", dateDebut, dateFin, new PageRequest(page, size));
        }

    }

}