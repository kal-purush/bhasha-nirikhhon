import { Client, InteractionReplyOptions, SlashCommandBuilder, SlashCommandIntegerOption, SlashCommandStringOption } from "discord.js";
import PocketBase, { Record as PBRecord } from "pocketbase";
import MulticraftAPI from "./utils/multicraft.js";
export const data = [
	new SlashCommandBuilder()
		.setName("register")
		.setDescription("Register for this weeks whitelist raffle")
		.addStringOption(new SlashCommandStringOption().setName("username").setDescription("Minecraft username").setRequired(true)),
	new SlashCommandBuilder().setName("unregister").setDescription("Unrgister from this weeks whitelist raffle"),
	new SlashCommandBuilder().setName("dvz_open").setDescription("Open whitelist registrations"),
	new SlashCommandBuilder().setName("dvz_close").setDescription("Close whitelist registrations"),
	new SlashCommandBuilder()
		.setName("whitelist")
		.setDescription("Whitelist additional players")
		.addIntegerOption(
			new SlashCommandIntegerOption().setName("player_count").setDescription("Number of players (default: 50)").setRequired(false)
		)
		.setDefaultMemberPermissions(0),
];

const DVZ_SERVER_ID = 14;
const REPLIES = {
	error: (message: string, raw: Partial<InteractionReplyOptions> = {}) => ({
		content: "",
		embeds: [
			{
				title: "Error",
				description: message,
				color: 0xa01a04,
			},
		],
		...raw,
	}),
	info: (message: string, raw: Partial<InteractionReplyOptions> = {}) => ({
		content: "",
		embeds: [
			{
				title: "Info",
				description: message,
				color: 0x247db9,
			},
		],
		...raw,
	}),
	registered: (username: string, uuid: string) => ({
		content: "",
		embeds: [
			{
				title: "Success",
				description: `Whitelist registration of \`${username}\` has been received.`,
				color: 0x4db924,
				thumbnail: {
					url: `https://crafatar.com/renders/body/${uuid}?overlay`,
				},
			},
		],
	}),
} satisfies Record<string, (...args: any[]) => InteractionReplyOptions>;

const ROLE_TICKETS = {
	"932370313541460038": -1, // Birthday Mouse, guaranteed
	"413104808334196757": 5, // Patreon 100
	"444327585103478794": 3, // Patreon 25
	"586309003441733648": 5, // VIPs
	"413104736636502026": 2, // Patreon 10
	"444770697106030602": 1, // Patreon 1
	"1011423880562348174": 2, // YouTube 7
	"1011423880562348173": 1, // YouTube 3.5
	"689250596532125741": 3, // Twitch 18
	"689250596532125738": 2, // Twitch 7
	"689250596532125737": 1, // Twitch 3.5
};

export default function MinecraftWhitelist(client: Client, db: PocketBase, multicraft: MulticraftAPI) {
	client.on("interactionCreate", async (interaction) => {
		if (interaction.isChatInputCommand() && interaction.commandName === "dvz_open") {
			const registrationOpen = await db
				.collection("settings")
				.getFirstListItem<PBRecord & { value: boolean }>('key="dvz_registrations_open"');
			if (typeof registrationOpen.value !== "boolean") throw new Error("setting 'dvz_registrations_open' is not a boolean");
			if (registrationOpen.value === true) {
				await interaction.reply(REPLIES.error("Registrations are already open", { ephemeral: true }));
				return;
			}
			await db.collection("settings").update(registrationOpen.id, { value: true });
			await interaction.reply(
				REPLIES.info("**Registrations are now open.**\nUse `/register <minecraft_username>` to register for today's shows.")
			);
			return;
		} else if (interaction.isChatInputCommand() && interaction.commandName === "dvz_close") {
			const registrationOpen = await db
				.collection("settings")
				.getFirstListItem<PBRecord & { value: boolean }>('key="dvz_registrations_open"');
			if (typeof registrationOpen.value !== "boolean") throw new Error("setting 'dvz_registrations_open' is not a boolean");
			if (registrationOpen.value === false) {
				await interaction.reply(REPLIES.error("Registrations are already closed", { ephemeral: true }));
				return;
			}
			await db.collection("settings").update(registrationOpen.id, { value: false });
			await interaction.reply(REPLIES.info("Registrations are now closed."));
			return;
		} else if (interaction.isChatInputCommand() && interaction.commandName === "register") {
			if (!interaction.member) {
				await interaction.reply(REPLIES.error("You must be in a server to register."));
				return;
			}

			const username = interaction.options.getString("username", true);
			const registrationOpen = await db
				.collection("settings")
				.getFirstListItem<PBRecord & { value: boolean }>('key="dvz_registrations_open"');
			if (typeof registrationOpen.value !== "boolean") throw new Error("setting 'dvz_registrations_open' is not a boolean");

			// Sign up user
			// Check if registrations are open
			if (registrationOpen.value === false) {
				await interaction.reply(
					REPLIES.error("Registrations not currently open. Check here for info on the next game:\nhttps://whenisdvz.rawb.tv")
				);
				return;
			}

			// Get uuid from username
			const profile = await usernameToProfile(username);
			if (!profile) {
				await interaction.reply(REPLIES.error(`Minecraft account \`${username}\` does not exist.`));
				return;
			}

			// Check if user or minecraft account is already registered
			let user: (Partial<PBRecord> & DvzUserRecord) | null = await db
				.collection("dvz_users")
				.getFirstListItem<PBRecord & DvzUserRecord>(`discordId="${interaction.user.id}" || uuid="${profile.id}"`)
				.catch(() => null);
			if (user && user.uuid !== "") {
				let error = "You are already registered. Good luck in the next whitelist raffle!";
				if (user.discordId !== interaction.user.id) {
					error = `Minecraft account \`${profile.name}\` is already registered to another Discord account.`;
				} else if (user.uuid !== profile.id) {
					const oldUsername = await uuidToProfile(user.uuid);
					error = `You are already registered with the Minecraft account \`${oldUsername}\`.\nIf you want to change your Minecraft account, use /unregister.`;
				}
				await interaction.reply(REPLIES.error(error));
				return;
			}

			// Calculate tickets
			let tickets = 1;
			const userRoles = Array.isArray(interaction.member.roles)
				? interaction.member.roles
				: Array.from(interaction.member.roles.cache.keys());
			function rolePresent(role: string): role is keyof typeof ROLE_TICKETS {
				return role in ROLE_TICKETS;
			}
			for (const role of userRoles) {
				if (rolePresent(role)) {
					if (ROLE_TICKETS[role] === -1) {
						tickets = -1;
						break;
					}
					tickets += ROLE_TICKETS[role];
				}
			}

			// Update user db
			const date = new Date().toISOString().split("T")[0];
			if (user) {
				user.uuid = profile.id;
				user.username = profile.name;
				user.tickets = tickets;
				if (!user.history) user.history = { interaction: [interaction.id, interaction.token], registered: [], participated: [] };
				if (!user.history.registered.includes(date)) user.history.registered.push(date);
				user.history.interaction = [interaction.id, interaction.token];
			} else
				user = {
					discordId: interaction.user.id,
					uuid: profile.id,
					username: profile.name,
					tickets: tickets,
					history: {
						interaction: [interaction.id, interaction.token],
						registered: [date],
						participated: [],
					},
				};

			if (user.id) await db.collection("dvz_users").update(user.id, user);
			else await db.collection("dvz_users").create(user);

			// Send reply
			const a = await interaction.reply(REPLIES.registered(profile.name, profile.id));

			return;
		} else if (interaction.isChatInputCommand() && interaction.commandName === "unregister") {
			let user = await db
				.collection("dvz_users")
				.getFirstListItem<PBRecord & DvzUserRecord>(`discordId="${interaction.user.id}"`)
				.catch(() => null);
			if (!user) {
				await interaction.reply(REPLIES.error("You are not yet registered for today's game."));
				return;
			}

			user.uuid = "";
			user.tickets = 0;
			if (!user.history) user.history = { interaction: null, registered: [], participated: [] };
			user.history.interaction = null;

			await db.collection("dvz_users").update(user.id, user);

			await interaction.reply(REPLIES.info("You have been unregistered from today's game."));
			return;
		} else if (interaction.isChatInputCommand() && interaction.commandName === "whitelist") {
			const amount = interaction.options.getInteger("player_count", false) ?? 50;

			const users = await db.collection("dvz_users").getFullList<PBRecord & DvzUserRecord>(undefined, {
				filter: "tickets != 0",
			});
			console.log(`Registering ${amount}/${users.length} users:`);
			interaction.deferReply();

			const backupUsers = new Set<PBRecord & DvzUserRecord>();
			const selectedUsers = new Set<PBRecord & DvzUserRecord>();
			const ticketPool: (PBRecord & DvzUserRecord)[] = [];
			for (const user of users) {
				if (user.history?.participated.includes(new Date().toISOString().split("T")[0])) {
					backupUsers.add(user);
					continue;
				}
				if (user.tickets === -1) {
					selectedUsers.add(user);
					continue;
				}
				ticketPool.push(...new Array(user.tickets).fill(user));
			}

			while (selectedUsers.size < amount && ticketPool.length > 0) {
				console.log(selectedUsers.size, ticketPool.length);
				const index = Math.floor(Math.random() * ticketPool.length);
				selectedUsers.add(ticketPool[index]);
				ticketPool.splice(index, 1);
			}

			console.log("Registered users", users.length);
			console.log(
				"Selected users",
				selectedUsers.size,
				[...selectedUsers].map((u) => u.tickets)
			);

			if (selectedUsers.size < amount) {
				console.log(`Adding ${amount - selectedUsers.size} backup users`);
				for (const user of [...backupUsers].sort((a, b) => b.tickets - a.tickets)) {
					selectedUsers.add(user);
					if (selectedUsers.size >= amount) break;
				}
			} else {
				console.log("Backup not needed");
			}

			for (const user of selectedUsers) {
				const commandResponse = await multicraft.call("sendConsoleCommand", {
					server_id: DVZ_SERVER_ID,
					command: `whitelist add ${user.username}`,
				});
				if (!commandResponse.success) {
					await interaction.reply(REPLIES.error(`An error occurred while ${user.username} the account to the whitelist.`));
					console.error("Multicraft error", JSON.stringify(commandResponse.errors, null, 2));
					return;
				}
				user.history?.participated.push(new Date().toISOString().split("T")[0]);

				await interaction.guild?.members.fetch(user.discordId).then((member) => member.roles.add("1074038162885714032"));

				console.log("Whitelisted", user.username);
			}
			await interaction.followUp({
				content: `Whitelisted **${selectedUsers.size}**/${amount} users:\n${[...selectedUsers]
					.map((u) => `<@${u.discordId}>`)
					.join("\n")}\n\nPlease check <#1074038036234502224> for the server address to connect`,
			});
		}
	});
}

async function usernameToProfile(username: string) {
	const res = await fetch("https://api.mojang.com/users/profiles/minecraft/" + username);
	if (res.status === 204) return null;
	if (res.status !== 200) {
		console.error("Mojang API error", res.status, res.statusText, await res.text());
		throw new Error("Mojang API error");
	}
	return (await res.json()) as { name: string; id: string };
}

async function uuidToProfile(uuid: string) {
	const res = await fetch(`https://sessionserver.mojang.com/session/minecraft/profile/${uuid}`);
	if (res.status === 400) throw new Error("Invalid UUID");
	if (res.status === 204) return null;
	if (res.status !== 200) {
		console.error("Mojang API error", res.status, res.statusText, await res.text());
		throw new Error("Mojang API error");
	}
	const { name } = (await res.json()) as { name: string };
	return name;
}

type DvzUserRecord = {
	discordId: string;
	uuid: string;
	username: string;
	tickets: number;
	history: null | { interaction: null | [string, string]; registered: string[]; participated: string[] };
};