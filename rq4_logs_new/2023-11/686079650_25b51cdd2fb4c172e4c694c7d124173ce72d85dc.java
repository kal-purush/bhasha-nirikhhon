package by.bsuir.poit.services.impl;

import by.bsuir.poit.bean.Auction;
import by.bsuir.poit.bean.AuctionBet;
import by.bsuir.poit.bean.AuctionMember;
import by.bsuir.poit.bean.AuctionType;
import by.bsuir.poit.dao.AuctionBetDao;
import by.bsuir.poit.dao.AuctionDao;
import by.bsuir.poit.dao.AuctionMemberDao;
import by.bsuir.poit.dao.AuctionTypeDao;
import by.bsuir.poit.dao.exception.DataAccessException;
import by.bsuir.poit.services.exception.resources.ResourceBusyException;
import by.bsuir.poit.services.exception.resources.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author Paval Shlyk
 * @since 20/11/2023
 */
class AuctionServiceImplTest {
private AuctionDao auctionDao = mock(AuctionDao.class);
private AuctionMemberDao auctionMemberDao = mock(AuctionMemberDao.class);
private AuctionBetDao auctionBetDao = mock(AuctionBetDao.class);
private AuctionTypeDao auctionTypeDao = mock(AuctionTypeDao.class);
private AuctionServiceImpl auctionService;

@BeforeEach
public void initMockDao() {
      auctionDao = mock(AuctionDao.class);
      auctionMemberDao = mock(AuctionMemberDao.class);
      auctionBetDao = mock(AuctionBetDao.class);
      auctionTypeDao = mock(AuctionTypeDao.class);
      auctionService = new AuctionServiceImpl(
	  auctionDao, auctionMemberDao,
	  auctionBetDao, auctionTypeDao);
}

@Test
void findAfterEventDate() {
      List<Auction> list = new ArrayList<>();
      Date date = new Date();
      doReturn(list).when(auctionDao).findAllAfterEventDate(date);
      assertEquals(list, auctionService.findAfterEventDate(date));
      doThrow(DataAccessException.class).when(auctionDao).findAllAfterEventDate(date);
      assertThrows(ResourceBusyException.class, () -> auctionService.findAfterEventDate(date));
      doThrow(IllegalStateException.class).when(auctionDao).findAllAfterEventDate(date);
      assertThrows(IllegalStateException.class, () -> auctionService.findAfterEventDate(date));
}

@Test
void findByClientId() {
      List<AuctionMember> members = List.of(
	  new AuctionMember(1L, 1L, (short) 2),
	  new AuctionMember(2L, 2L, (short) 2)
      );
      Auction auction = new Auction();
      when(auctionMemberDao.findAllByClientId(anyLong())).thenReturn(members);
      when(auctionDao.findById(anyLong())).thenReturn(Optional.empty());
      assertThrows(ResourceNotFoundException.class, () -> auctionService.findByClientId(anyLong()));
      when(auctionDao.findById(anyLong())).thenReturn(Optional.of(auction));
      assertTrue(
	  auctionService.findByClientId(anyLong()).stream().allMatch(auction::equals)
      );
      when(auctionDao.findById(anyLong())).thenThrow(DataAccessException.class);
      assertThrows(ResourceBusyException.class, () -> auctionService.findByClientId(anyLong()));
}

@Test
void findById() {
      doThrow(DataAccessException.class).when(auctionDao).findById(anyLong());
      assertThrows(ResourceBusyException.class, () -> auctionService.findById(anyLong()));
      Auction auction = new Auction();
      doReturn(Optional.of(auction)).when(auctionDao).findById(anyLong());
      assertEquals(auction, auctionService.findById(anyLong()));
}

@Test
void findAllBetsByClientId() {
      List<AuctionBet> firstUserBets = List.of();
      List<AuctionBet> secondUserBets = List.of(
	  AuctionBet.builder().bet(0.3).build(),
	  AuctionBet.builder().bet(0.7).build());
      long auctionId = 42;
      doReturn(firstUserBets).when(auctionBetDao).findAllByAuctionIdAndClientId(auctionId, 1);
      doReturn(secondUserBets).when(auctionBetDao).findAllByAuctionIdAndClientId(auctionId, 2);
      assertEquals(firstUserBets, auctionService.findAllBetsByClientId(auctionId, 1));
      assertEquals(secondUserBets, auctionService.findAllBetsByClientId(auctionId, 2));
}

@Test
void findTypeByAuctionId() {
      AuctionType type = AuctionType.builder()
			     .id(1).name("First type").description("No description")
			     .build();
      Auction auction = Auction.builder()
			    .auctionTypeId(1L)
			    .build();
      when(auctionDao.findById(anyLong())).thenReturn(Optional.of(auction));
      when(auctionTypeDao.findById(1)).thenReturn(Optional.of(type));
      assertEquals(type, auctionService.findTypeByAuctionId(anyLong()));
      when(auctionTypeDao.findById(anyLong())).thenReturn(Optional.empty());
      assertThrows(ResourceNotFoundException.class, () -> auctionService.findTypeByAuctionId(anyLong()));
}
}