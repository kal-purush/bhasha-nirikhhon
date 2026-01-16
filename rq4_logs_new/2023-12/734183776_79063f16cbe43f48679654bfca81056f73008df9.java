package com.avj.stratop.bearcallspread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import com.avj.stratop.common.BuyCall;
import com.avj.stratop.common.Constants;
import com.avj.stratop.common.OptionsStrategy;
import com.avj.stratop.common.SellCall;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BearCallSpread extends OptionsStrategy {

	private Double scallPrice;
	private Double scallStrike;

	private Double bcallPrice;
	private Double bcallStrike;

	private Double strikediff;
	private Double lotsize;
	private Double netinvested;
	private Double netCredit;
	private Double spotat;
	private Double combinedBEP;
	private String disp;

	private Double bcallIbep;
	private Double bcallCbep;
	private Double scallIbep;
	private Double scallCbep;

	public BearCallSpread(String str) {
		this.disp = str;
	}

	private Double findNearestLowerStrikeOfSpot(Double spot, List<Double> spots) {
		Double start = scallStrike;
		while (true) {
			start -= strikediff;
			if (start < spot) {
				break;
			}
			spots.add(start);
		}
		return start;
	}

	private void generate() {
		List<Double> spots = new ArrayList<>();
		spots.add(spotat);
		Double nearestLowerStrike = findNearestLowerStrikeOfSpot(spotat, spots);
		spots.add(nearestLowerStrike);
		for (int i = 0; i < 5; i++) {
			nearestLowerStrike -= strikediff;
			spots.add(nearestLowerStrike);
		}
		spots.add(scallStrike);
		Double start = scallStrike;
		while (start <= bcallStrike) {
			start += strikediff;
			spots.add(start);
		}
		for (int i = 0; i < 5; i++) {
			start += strikediff;
			spots.add(start);
		}
		spots.add(scallIbep);
		spots.add(bcallIbep);
		spots.add(combinedBEP);
		Collections.sort(spots);
		List<BearCallSpreadRow> spotlist = new ArrayList<>();
		for (Double sp : spots) {
			SellCall sellcallrow = SellCall.builder().aya(scallPrice).diya(0.0).strike(scallStrike)
					.build();
			BuyCall buycallrow = BuyCall.builder().aya(0.0).diya(bcallPrice).strike(bcallStrike).build();
			BearCallSpreadRow spot = BearCallSpreadRow.builder().spotprice(sp).sellcall(sellcallrow).buycall(buycallrow).build();
			spotlist.add(spot);
		}

		for (BearCallSpreadRow spot : spotlist) {
			Double spotprice = spot.getSpotprice();

			SellCall scall = spot.getSellcall();
			Double sellCallStrike = scall.getStrike();
			if (spotprice < sellCallStrike) {
				scall.setWhereami(Constants.OTM);
				scall.setDiya(0.0);
			} else if (spotprice.equals(sellCallStrike)) {
				scall.setWhereami(Constants.ATM);
				scall.setDiya(0.0);
			} else if (spotprice > sellCallStrike) {
				scall.setWhereami(Constants.ITM);
				scall.setDiya(roundoff(spotprice - sellCallStrike));
			}
			scall.classify();
			scall.setPandl(roundoff(scall.getAya() - scall.getDiya()));

			System.out.print(spot.getSpotprice() + "\t" + scall.getDes() + "\t" + scall.getAya() + "\t"
					+ scall.getDiya() + "\t" + scall.getPandl() + "\t");

			BuyCall bcall = spot.getBuycall();
			Double buyCallStrike = bcall.getStrike();
			if (spotprice < buyCallStrike) {
				bcall.setWhereami(Constants.OTM);
				bcall.setAya(0.0);
			} else if (spotprice.equals(buyCallStrike)) {
				bcall.setWhereami(Constants.ATM);
				bcall.setAya(0.0);
			} else if (spotprice > buyCallStrike) {
				bcall.setWhereami(Constants.ITM);
				bcall.setAya(roundoff(spotprice - buyCallStrike));
			}
			bcall.classify();
			bcall.setPandl(roundoff(bcall.getAya() - bcall.getDiya()));
			System.out.print(spot.getSpotprice() + "\t" + bcall.getDes() + "\t" + bcall.getAya() + "\t"
					+ bcall.getDiya() + "\t" + bcall.getPandl());

			spot.setPandl(roundoff(scall.getPandl() + bcall.getPandl()));
			spot.setPlPerLot(roundoff(spot.getPandl() * lotsize));
			Double roi = roundoff((spot.getPlPerLot() / netinvested) * 100);
			spot.setRoi(roi);
			System.out.println("\t" + spot.getPandl() + "\t" + spot.getPlPerLot() + "\t" + spot.getRoi());
		}

	}

	@Override
	public void evaluate() {

		Scanner myObj = new Scanner(System.in);

		System.out.println(disp);
		System.out.println("Spot: ");
		String spotatstr = myObj.nextLine();
		spotat = Double.valueOf(spotatstr);

		System.out.println("Sell CE Strike: ");
		String soldcallstrike = myObj.nextLine();
		scallStrike = Double.valueOf(soldcallstrike);

		System.out.println("Sell CE @: ");
		String soldcallprice = myObj.nextLine();
		scallPrice = Double.valueOf(soldcallprice);

		System.out.println("Buy CE Strike: ");
		String botcallstrike = myObj.nextLine();
		bcallStrike = Double.valueOf(botcallstrike);

		System.out.println("Buy CE @: ");
		String botcallprice = myObj.nextLine();
		bcallPrice = Double.valueOf(botcallprice);

		System.out.println("Lot size: ");
		String lotsizestr = myObj.nextLine();
		lotsize = Double.valueOf(lotsizestr);

		System.out.println("Strike Difference: ");
		String sdiffstr = myObj.nextLine();
		strikediff = Double.valueOf(sdiffstr);

		System.out.println("Total investment: ");
		String margin = myObj.nextLine();
		netinvested = Double.valueOf(margin);

		System.out.println("*****************************************************************************************");
		scallIbep = scallStrike + scallPrice;
		bcallIbep = bcallStrike + bcallPrice;
		netCredit = scallPrice - bcallPrice;
		combinedBEP = scallStrike + netCredit;

		generate();

	}

}