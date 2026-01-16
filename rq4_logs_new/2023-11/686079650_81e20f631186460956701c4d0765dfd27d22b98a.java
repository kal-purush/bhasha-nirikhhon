package by.bsuir.poit.services.impl;

import by.bsuir.poit.bean.*;
import by.bsuir.poit.bean.mappers.AuctionMapper;
import by.bsuir.poit.context.Service;
import by.bsuir.poit.dao.AuctionBetDao;
import by.bsuir.poit.dao.AuctionDao;
import by.bsuir.poit.dao.AuctionMemberDao;
import by.bsuir.poit.dao.AuctionTypeDao;
import by.bsuir.poit.dao.exception.DataAccessException;
import by.bsuir.poit.dao.exception.DataModifyingException;
import by.bsuir.poit.services.AuctionService;
import by.bsuir.poit.services.exception.resources.ResourceBusyException;
import by.bsuir.poit.services.exception.resources.ResourceModifyingException;
import by.bsuir.poit.services.exception.resources.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

/**
 * @author Paval Shlyk
 * @since 20/11/2023
 */
@Service
@RequiredArgsConstructor
public class AuctionServiceImpl implements AuctionService {
private static final Logger LOGGER = LogManager.getLogger(AuctionServiceImpl.class);
private final AuctionDao auctionDao;
private final AuctionMemberDao auctionMemberDao;
private final AuctionBetDao auctionBetDao;
private final AuctionTypeDao auctionTypeDao;

@Override
public List<Auction> findAfterEventDate(Date date) {
      List<Auction> auctions;
      try {
	    auctions = auctionDao.findAllAfterEventDate(date);
      } catch (DataAccessException e) {
	    final String msg = String.format("Failed to find auctions after event date=%s", date.toString());
	    LOGGER.error(msg);
	    throw new ResourceBusyException(msg);
      }
      return auctions;
}

@Override
public List<Auction> findByClientId(long clientId) {
      List<Auction> auctions;
      try {
	    List<AuctionMember> members = auctionMemberDao.findAllByClientId(clientId);
	    auctions = new ArrayList<>();
	    for (AuctionMember member : members) {
		  auctionDao
		      .findById(member.getAuctionId())
		      .ifPresent(auctions::add);
	    }
      } catch (DataAccessException e) {
	    final String msg = String.format("Failed to find auctions by given clientId=%d", clientId);
	    LOGGER.error(msg);
	    throw new ResourceBusyException(msg);
      }
      return auctions;
}

@Override
public Auction findById(long auctionId) throws ResourceNotFoundException {
      Auction auction;
      try {
	    auction = auctionDao.findById(auctionId).orElseThrow(() -> newAuctionNotFoundException(auctionId));
      } catch (DataAccessException e) {
	    LOGGER.error("Failed find auction by id={}", auctionId);
	    throw new ResourceBusyException(e);
      }
      return auction;
}

@Override
public List<AuctionBet> findAllBets(long auctionId) throws ResourceNotFoundException {
      List<AuctionBet> bets;
      try {
	    bets = auctionBetDao.findAllByAuctionId(auctionId);
      } catch (DataAccessException e) {
	    LOGGER.error("Failed find bets by given auctionId={}", auctionId);
	    throw new ResourceBusyException(e);
      }
      return bets;
}

@Override
public List<AuctionBet> findAllBetsByClientId(long auctionId, long clientId) throws ResourceNotFoundException {
      List<AuctionBet> bets;
      try {
	    bets = auctionBetDao.findAllByAuctionIdAndClientId(auctionId, clientId);
      } catch (DataAccessException e) {
	    LOGGER.error("Failed to find bets by clientId={} and auctionId={}", clientId, auctionId);
	    throw new ResourceBusyException(e);
      }
      return bets;
}

@Override
public AuctionType findTypeByAuctionId(long auctionId) throws ResourceNotFoundException {
      AuctionType type;
      try {
	    Auction auction = auctionDao.findById(auctionId).orElseThrow(() -> newAuctionNotFoundException(auctionId));
	    type = auctionTypeDao.findById(auction.getAuctionTypeId()).orElseThrow(() -> newAuctionTypeNotFoundException(auction.getAuctionTypeId()));
      } catch (DataAccessException e) {
	    LOGGER.error("Failed to find auction type by given actionId={}", auctionId);
	    throw new ResourceBusyException(e);
      }
      return type;
}

@Override
public List<AuctionType> findAllTypes() {
      try {
	    return auctionTypeDao.findAll();
      } catch (DataAccessException e) {
	    LOGGER.error("Failed to find all auction type");
	    throw new ResourceBusyException(e);
      }
}

@Override
public void saveAuction(Auction auction) throws ResourceModifyingException {
      try {
	    auctionDao.save(auction);
      } catch (DataAccessException e) {
	    LOGGER.error("Failed to save auction {}", auction);
	    throw new ResourceModifyingException(e);
      }
}

@Override
public void saveBet(AuctionBet bet) throws ResourceNotFoundException {
      try {
	    auctionBetDao.save(bet);
      } catch (DataAccessException e) {
	    LOGGER.error("Failed to save auction bet {}", bet);
	    throw new ResourceModifyingException(e);
      }
}

private ResourceNotFoundException newAuctionTypeNotFoundException(long auctionTypeId) {
      final String msg = String.format("Failed to find auction type by given id=%d", auctionTypeId);
      LOGGER.info(msg);
      throw new ResourceNotFoundException(msg);

}

private ResourceNotFoundException newAuctionNotFoundException(long auctionId) {
      final String msg = String.format("Failed to find auction by given id=%d", auctionId);
      LOGGER.info(msg);
      return new ResourceNotFoundException(msg);
}
}