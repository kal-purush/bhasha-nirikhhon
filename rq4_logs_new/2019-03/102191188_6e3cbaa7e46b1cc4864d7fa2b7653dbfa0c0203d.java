package com.vivala.data.transformers;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;

import com.gf.collections.GfCollection;
import com.gf.collections.GfCollections;
import com.gf.collections.functions.ToDouble;
import com.gf.util.string.JSON;
import com.gf.util.string.Splitter;

public final class ClosedBetsUniqueCount {
	private static final String csvFileAll = "/Users/dmitry/athena/athena-platform/closed_bets_all.csv";
	private static final String csvFileTeam = "/Users/dmitry/athena/athena-platform/closed_bets_team.csv";
	private static final String csvFileTeamPlayer = "/Users/dmitry/athena/athena-platform/closed_bets_team_player.csv";
	private static final String csvFileTeamPlayerLocation = "/Users/dmitry/athena/athena-platform/closed_bets_team_player_location.csv";

	public static final void main(final String[] args) throws IOException {
		new ClosedBetsUniqueCount();
	}

	
	public ClosedBetsUniqueCount() throws UnsupportedEncodingException, IOException {
		System.out.println("All bets:");
		this.start(csvFileAll);
		System.out.println("==================");
		System.out.println("Team bets:");
		this.start(csvFileTeam);
		System.out.println("==================");
		System.out.println("Team+player bets:");
		this.start(csvFileTeamPlayer);
		System.out.println("==================");
		System.out.println("Team+player+location bets:");
		this.start(csvFileTeamPlayerLocation);
		System.out.println("==================");
	}

	public final void start(final String path) throws UnsupportedEncodingException, IOException {
		GfCollections.asArrayCollection(Splitter.split(new String(Files.readAllBytes(new File(path).toPath()), "UTF-8"), '\n'))
		.flatMap(r->{
			if (r.trim().isEmpty())
				return GfCollections.asLinkedCollection();

			if (r.toLowerCase().contains("project id"))
				return GfCollections.asLinkedCollection();

			return GfCollections.asLinkedCollection(parse(r));
		})
		.groupBy(r->r.user_id)
		.collectValues()
		.action(collection->{
			System.out.println("Uniques: " + collection.size());
			System.out.println("Avarage per user: " + collection.avarage(new ToDouble<GfCollection<ClosedBetDescriptor>>() {
				@Override
				public final double toDouble(final GfCollection<ClosedBetDescriptor> c) {
					return c.size();
				}
			}));
		});
	}

	private static final ClosedBetDescriptor parse(final String row) {
		try {
			final String[] arr = Splitter.splitIncludeEmpty(row, ',');
			final ClosedBetDescriptor res = new ClosedBetDescriptor();
			int index = -1;
			res.project_id = arr[++index];
			res.country = arr[++index];
			res.city = arr[++index];
			res.platform = arr[++index];
			res.user_id = arr[++index];
			res.type = arr[++index];
			res.action_time = arr[++index];
			res.betid = arr[++index];
			res.gameid = arr[++index];
			res.teamid = arr[++index];
			res.playerid = arr[++index];
			res.location = arr[++index];
			res.bonus = arr[++index];
			res.profitnetto = Integer.parseInt(arr[++index]);
			res.risked = Integer.parseInt(arr[++index]);
			res.noBonusProfit = Integer.parseInt(arr[++index]);
			return res;
		}catch(final RuntimeException t) {
			t.printStackTrace();
			System.out.println("");
			System.out.println(row);
			System.out.println(JSON.toJson(Splitter.splitIncludeEmpty(row, ',')));
			throw t;
		}
	}
}