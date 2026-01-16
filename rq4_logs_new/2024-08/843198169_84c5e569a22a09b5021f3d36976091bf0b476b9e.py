from interactions import Extension, Embed, Button, ButtonStyle
from interactions.ext.prefixed_commands import prefixed_command, PrefixedContext


class HiddenCommands(Extension):
    def __init__(self, bot):
        self.bot = bot

    @prefixed_command(name="send_server_rules")
    async def send_server_rules_function(self, ctx: PrefixedContext):
        intro = Embed(
            title="Server Rules",
            description="This is a complete list of all the rules that we expect users of the United Marxist Pact to abide by when a member of the Server. Some rules have attached threads which includes clarifications on specific rules.",
            color="#36393F"
            )
        intro.set_thumbnail(ctx.guild.icon.url)
        await ctx.send(embeds=[intro])

        rule_1 = Embed(
            title="1 | Discord Guidelines",
            description="Users should follow the Discord [Terms of Service](https://discord.com/terms) and [Community Guidelines](https://discord.com/guidelines). Please read them if you haven’t already. As a politically left-wing server, we’re already on thin ice with Discord, so it remains detrimentally important that we follow the guidelines to prevent Discord staff from attacking us.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_1])

        rule_2 = Embed(
            title="2 | Personal Information",
            description="Don’t share personal information, including names, locations, or photos, of yourself or others. This includes jokingly posting fake information. Respect each other’s privacy to maintain a safe online space.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_2])

        rule_3 = Embed(
            title="3 | Bullying and Harassment",
            description="No bullying, harassment, or toxicity including off server. We will ban people for actions and words they take off server or in Direct Messages, especially in Partnered Servers. The actions and words of members in this server reflect upon it, and we don’t want people who act poorly in our name.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_3])

        rule_4 = Embed(
            title="4 | NSFW Content",
            description="No NSFW or NSFL content is allowed on the server, including inappropriate conversations, profile pictures, names, memes, images, videos, and other miscellaneous forms of content. No posts or images may contain genitals, the female breast, gore of any sort, pornography, or NSFW art. You can discuss these topics in a mature manner or reference them, but, for example, intending to sexualize things by saying something sexually suggestive is deemed inappropriate.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_4])

        rule_5 = Embed(
            title="5 | Impersonation",
            description="Please refrain from impersonating any user or trying to access unauthorized content. This involves changing your username, profile picture, bio, and status intentionally to match those of another user in an impersonation attempt, claiming roles you didn’t earn, and intentionally plagiarizing to get ahead in the server.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_5])

        rule_6 = Embed(
            title="6 | Principled Criticism",
            description="The server only allows principled, critical support for movements or leaders on this server (e.g., no Personality Cults, active engagement in Criticism/Self-Criticism of any kind). Apply rational object differential analysis—you can support one aspect while denouncing another within a principled critical analysis. On the other end, we also want principled and credible criticism from reliable sources of movements and leaders",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_6])

        rule_vetting = Embed(
            title="Vetting Rule",
            description="If you are completing your vetting, the correct answer to the question on if you have read and understood the Rules is 'Workers of the World, Unite!'",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_vetting])

        rule_8 = Embed(
            title="8 | Bearing False Witness",
            description="Lying or bearing false witness to moderators or omitting relevant information necessary for moderators or administrators to perform their job isn't allowed. If you feel as if you had good reasons (such as worrying about doxxing someone) then feel free to contact moderation.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_8])

        rule_9 = Embed(
            title="9 | Reactionary Tendencies",
            description="Justifying genocide, imperialism, bigotry, or extreme right-wing ideologies/movements/states is prohibited. This includes engaging in “ironic jokes” about hate speech, slurs, or expressing support for them. We don’t want to create an environment of toxic “irony” and a smokescreen for reactionaries to normalize, and taking these topics lightly instead of treating them as the threat to the working class they are. Levity may be one thing—Schrödinger’s Douchebag is another.",
            color="#36393F"
            )

        easter_egg_button = Button(
            style=ButtonStyle.BLURPLE,
            label="Clarifications On Rule 9",
            custom_id="rule_9"
        )

        await ctx.send(embeds=[rule_9], components=[easter_egg_button])

        rule_10 = Embed(
            title="10 | Channel Usage",
            description="Utilise the correct channels for discussion. Mediators may ask you to move when appropriate. Consistent refusal to do so can result in Mediator action.",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_10])

        rule_1984 = Embed(
            title="1984 | Moderator Action",
            description="To make it clear: the rules are not hardcoded, nor are they equally applied, nor do we want nor expect staff to be neutral arbiters of the letter of the law. The spirit, the PURPOSE of the rules is what we are interested in, which include: \n\n1) Forming the basis for maintaining our server's existence (Discord ToS) - these are the only hard coded ones\n2) Maintaining a high quality membership of good faith people who engage in proper ⁠discussion-ethics zn3) Allow for a comfortable place to discuss highly stressful topics, and to protect our membership",
            color="#36393F"
            )
        await ctx.send(embeds=[rule_1984])

    @prefixed_command(name="send_discussion_ethics")
    async def send_discussion_ethics_function(self, ctx: PrefixedContext):
        await ctx.reply("Hello world!")

    @prefixed_command(name="collect_last_50")
    async def collect_last_50_function(self, ctx: PrefixedContext):
        channel = ctx.channel
        messages = await channel.fetch_messages(50)
        
        # Assuming you want to save the messages in reverse order (newest first)
        messages.reverse()

        # Generate HTML content
        html_content = generate_html(messages)

        # Save HTML content to a file
        await save_html_to_file(html_content)

        # Send a confirmation message
        await ctx.send("Last 50 messages saved to last_50_messages.html.", ephemeral=True)

def generate_html(messages):
    html_content = "<html><body>"
    for msg in messages:
        html_content += f"<p>alt='{msg.author.username}'> <strong>{msg.author.username}</strong>: {msg.content}</p>"
    html_content += "</body></html>"
    return html_content

async def save_html_to_file(html_content, filename="last_50_messages.html"):
    with open(filename, "w", encoding='utf-8') as file:
        file.write(html_content)