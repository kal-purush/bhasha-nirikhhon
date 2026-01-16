interface Permissions {
	[key: string]: number;
}

const permissions: Permissions = {
	'Create instant invite': 0x1,
	'Kick members': 0x2,
	'Ban members': 0x4,
	Administrator: 0x8,
	'Manage channels': 0x10,
	'Manage guild': 0x20,
	'Add reactions': 0x40,
	'View audit log': 0x80,
	'Priority speaker': 0x100,
	Stream: 0x200,
	'View channel': 0x400,
	'Send messages': 0x800,
	'Send TTS messages': 0x1000,
	'Manage messages': 0x2000,
	'Embed links': 0x4000,
	'Attach files': 0x8000,
	'Read message history': 0x10000,
	'Mention everyone': 0x20000,
	'Use external emojis': 0x40000,
	'View guild insights': 0x80000,
	Connect: 0x100000,
	Speak: 0x200000,
	'Mute members': 0x400000,
	'Deafen members': 0x800000,
	'Move members': 0x1000000,
	'Use VAD': 0x2000000,
	'Change nickname': 0x4000000,
	'Manage nicknames': 0x8000000,
	'Manage roles': 0x10000000,
	'Manage webhooks': 0x20000000,
	'Manage emojis': 0x40000000,
};

export default permissions;