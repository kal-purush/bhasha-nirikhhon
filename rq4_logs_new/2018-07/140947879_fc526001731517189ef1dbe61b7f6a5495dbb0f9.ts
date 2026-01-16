// tslint:disable:max-line-length
import { Injectable } from '@angular/core';
import { of, Observable } from '@node_modules/rxjs';

@Injectable({
  providedIn: 'root'
})
export class FeedRetrieverService {

  constructor() { }

  getFeeds(): Observable<any> {
    return of(
      [
        {
          'title': 'Shop Contest: Create More Fortnite Mysteries, Winners!',
          'link': 'https://kotaku.com/shop-contest-create-more-fortnite-mysteries-winners-1827599798',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--1AtOtH2m--/c_fit,fl_progressive,q_80,w_636/oggzx5vdxacine8nk77e.png\' /><p><em>Fornite <\/em>keeps upping the ante with its strange world developments from season to season, and<a href=\'https://kotaku.com/shop-contest-create-more-fortnite-mysteries-1827429272#_ga=2.131293638.836107552.1530927321-248556907.1526835855\' rel=\'nofollow\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://kotaku.com/shop-contest-create-more-fortnite-mysteries-1827429272#_ga=2.131293638.836107552.1530927321-248556907.1526835855&#39;, {metric25:1})\'>last week’s ‘Shop Contest<\/a> was all about adding even more mysteries to the pile.<br><\/p><p><a href=\'https://kotaku.com/shop-contest-create-more-fortnite-mysteries-winners-1827599798\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kotaku shop contest'
            },
            {
              '@domain': '',
              '#text': 'shop contest'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Sat, 14 Jul 2018 16:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827599798'
          },
          'creator': 'Cameron Kunzelman'
        },
        {
          'title': 'Snake, do you remember the sinking of that tanker two years ago?',
          'link': 'https://kotaku.com/snake-do-you-remember-the-sinking-of-that-tanker-two-y-1827600432',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--V6KVXUsE--/c_fit,fl_progressive,q_80,w_636/mjxdjt12ihadlnkutd7r.png\' /><p><strong>Snake, do you remember the sinking of that tanker two years ago? We’re sneaking our way through (hopefully) all of Metal Gear Solid 2 <a href=\'https://www.twitch.tv/kotaku\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://www.twitch.tv/kotaku&#39;, {metric25:1})\'>live now on Twitch<\/a>!<br><\/strong><\/p><p><a href=\'https://kotaku.com/snake-do-you-remember-the-sinking-of-that-tanker-two-y-1827600432\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'metal gear solid 2'
            },
            {
              '@domain': '',
              '#text': 'twitch'
            }
          ],
          'pubDate': 'Sat, 14 Jul 2018 15:55:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827600432'
          },
          'creator': 'Heather Alexandra'
        },
        {
          'title': 'Today’s selection of articles from Kotaku’s reader-run community: Guilt, Depression And Doki Doki Li',
          'link': 'https://tay.kinja.com/today-s-selection-of-articles-from-kotaku-s-reader-run-1827598518',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--w_9QTgkU--/c_fit,fl_progressive,q_80,w_636/v0eyujbyj71ojbio0rnz.png\' /><p><strong>Today’s selection of articles<\/strong> from<em>Kotaku<\/em>’s reader-run community:<a href=\'https://tay.kinja.com/guilt-depression-and-ddlc-1826546331\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://tay.kinja.com/guilt-depression-and-ddlc-1826546331&#39;, {metric25:1})\'>Guilt, Depression And <em>Doki Doki Literature Club<\/em><\/a><strong>• <\/strong><a href=\'https://tay.kinja.com/a-bite-of-my-backlog-alliance-alive-session-2-1826819398#_ga=2.162200636.1273086674.1531456381-438771944.1502456496\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://tay.kinja.com/a-bite-of-my-backlog-alliance-alive-session-2-1826819398#_ga=2.162200636.1273086674.1531456381-438771944.1502456496&#39;, {metric25:1})\'>A Bite Of My Backlog: <em>Alliance Alive<\/em>, Session 2<\/a><strong>•<\/strong><a href=\'https://tay.kinja.com/catching-summer-vibes-from-synthesizers-1826814475\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://tay.kinja.com/catching-summer-vibes-from-synthesizers-1826814475&#39;, {metric25:1})\'>Catching Summer Vibes From Synthesizers<\/a><strong>•<\/strong><a href=\'https://anitay.kinja.com/edens-zero-chapters-1-2-impressions-1827346440#_ga=2.57220138.1273086674.1531456381-438771944.1502456496\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://anitay.kinja.com/edens-zero-chapters-1-2-impressions-1827346440#_ga=2.57220138.1273086674.1531456381-438771944.1502456496&#39;, {metric25:1})\'><em>Eden’s Zero<\/em> - Chapters 1 &amp;amp; 2 Impressions<\/a><strong> • <\/strong><a href=\'https://badopinionshat.kinja.com/pro-wrestling-is-in-a-lot-of-games-except-in-pro-wrest-1826807568\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://badopinionshat.kinja.com/pro-wrestling-is-in-a-lot-of-games-except-in-pro-wrest-1826807568&#39;, {metric25:1})\'>Pro Wrestling Is In A Lot Of Games, Except In Pro Wrestling Games<\/a><br><\/p><p><a href=\'https://tay.kinja.com/today-s-selection-of-articles-from-kotaku-s-reader-run-1827598518\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'talk amongst yourselves'
            },
            {
              '@domain': '',
              '#text': 'tay'
            }
          ],
          'pubDate': 'Sat, 14 Jul 2018 01:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827598518'
          },
          'creator': 'Narelle Ho Sang on Talk Amongst Yourselves, shared by Riley MacLeod to Kotaku'
        },
        {
          'title': 'This Week In The Business: Struggles on Steam',
          'link': 'https://kotaku.com/this-week-in-the-business-struggles-on-steam-1827599401',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--T_22Ovno--/c_fit,fl_progressive,q_80,w_636/bzbs3a5ybbyt2vkxshqn.png\' /><p>QUOTE | “If they upped the cost of Steam Direct to something like $500 or even $1,000, that’s not going to deter a small developer who’s put years of work into a passion project - but it is going to make asset flipping and bottom-of-the-barrel games much less profitable.” – At Develop:Brighton, Tomas Rawlings of…<\/p><p><a href=\'https://kotaku.com/this-week-in-the-business-struggles-on-steam-1827599401\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'this week in the business'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Sat, 14 Jul 2018 14:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827599401'
          },
          'creator': 'Rebekah Valentine'
        },
        {
          'title': 'A beloved Japanese whisky is headed to the U.S., and we might even be able to afford it',
          'link': 'https://thetakeout.com/a-beloved-japanese-whisky-is-headed-to-the-u-s-and-we-1827579586',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--f0pJVPst--/c_fit,fl_progressive,q_80,w_636/x3m5voaevqe1a4tyu1dh.jpg\' /><p>To say that Japanese whisky (<a href=\'https://thetakeout.com/til-whiskey-vs-whisky-and-why-skey-1826107603\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://thetakeout.com/til-whiskey-vs-whisky-and-why-skey-1826107603&#39;, {metric25:1})\'>no ‘e’<\/a>) is having a moment would be inaccurate, as it’s been having a moment for quite awhile now and at a certain point, a moment becomes a mainstay. But while the buzz surrounding the spirit has extended beyond the realm of whiskey nerds, it’s still not found on all that many bar carts,…<\/p><p><a href=\'https://thetakeout.com/a-beloved-japanese-whisky-is-headed-to-the-u-s-and-we-1827579586\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'whiskey'
            },
            {
              '@domain': '',
              '#text': 'japan'
            },
            {
              '@domain': '',
              '#text': 'spirits'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 17:15:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827579586'
          },
          'creator': 'Allison Shoemaker on The Takeout, shared by Riley MacLeod to Kotaku'
        },
        {
          'title': 'Start Building Your Dolby Atmos Setup With This Upward-Firing Speaker Pair',
          'link': 'https://kinjadeals.theinventory.com/start-building-your-dolby-atmos-setup-with-this-upward-1827591675',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--b8hSWu2d--/c_fit,fl_progressive,q_80,w_636/mivsbhjgl282kiwmugeg.jpg\' /><p>Notice anything weird about <a href=\'https://massdrop.7eer.net/c/379647/252901/4148?u=https%3A%2F%2Fwww.massdrop.com%2Fbuy%2Fpioneer-elite-dolby-atmos-sp-ebs73-lr-speakers\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://massdrop.7eer.net/c/379647/252901/4148?u=https%3A%2F%2Fwww.massdrop.com%2Fbuy%2Fpioneer-elite-dolby-atmos-sp-ebs73-lr-speakers&#39;, {metric25:1})\'>those speakers<\/a>? Look on top. Yes, they have upward-firing drivers in addition to forward firing, making them perfect for a Dolby Atmos setup, and they’re only $350 for the pair today, or $150 off.<br><\/p><p><a href=\'https://kinjadeals.theinventory.com/start-building-your-dolby-atmos-setup-with-this-upward-1827591675\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kinja deals'
            },
            {
              '@domain': '',
              '#text': 'deals'
            },
            {
              '@domain': '',
              '#text': 'massdrop deals'
            },
            {
              '@domain': '',
              '#text': 'pioneer deals'
            }
          ],
          'pubDate': 'Sat, 14 Jul 2018 12:05:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827591675'
          },
          'creator': 'Shep McAllister on Kinja Deals, shared by Shep McAllister to Kotaku'
        },
        {
          'title': 'Anker Upgraded Our Readers\' Favorite Bluetooth Earbuds, and They\'re Just $20 Today',
          'link': 'https://kinjadeals.theinventory.com/our-readers-voted-anker-s-soundbuds-slims-as-their-favo-1824137728',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--whnNSStB--/c_fit,fl_progressive,q_80,w_636/m1qkesuzxaiv4ges73qo.jpg\' /><p>Our readers <a href=\'https://gear.lifehacker.com/ankers-soundbuds-are-your-favorite-cheap-bluetooth-earb-1770741170\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://gear.lifehacker.com/ankers-soundbuds-are-your-favorite-cheap-bluetooth-earb-1770741170&#39;, {metric25:1})\'>voted<\/a> Anker’s SoundBuds Slims as their favorite affordable Bluetooth headphones, but we may need a recount, as<a rel=\'nofollow\' data-amazonasin=\'B0756T7R5T\' data-amazonsubtag=\'[p|1824137728[a|B0756T7R5T[au|5727177402741770316[b|kotaku\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - Anker Upgraded Our Readers&amp;#39; Favorite Bluetooth Earbuds, and They&amp;#39;re Just $20 Today&#39;, &#39;B0756T7R5T&#39;);window.ga(&#39;unique.send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - Anker Upgraded Our Readers&amp;#39; Favorite Bluetooth Earbuds, and They&amp;#39;re Just $20 Today&#39;, &#39;B0756T7R5T&#39;);\' data-amazontag=\'kotakuamzn-20\' href=\'https://www.amazon.com/gp/product/B0756T7R5T?tag=kotakuamzn-20&amp;ascsubtag=69aa36ab05bbce23d4a6f33963c18a3ee783135a&amp;rawdata=[t|link[p|1824137728[a|B0756T7R5T[au|5727177402741770316[b|kotaku\'>Anker recently released the upgraded SoundBuds Slim+<\/a>, on sale for just $20 today, for Prime members only.<\/p><p><a href=\'https://kinjadeals.theinventory.com/our-readers-voted-anker-s-soundbuds-slims-as-their-favo-1824137728\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kinja deals'
            },
            {
              '@domain': '',
              '#text': 'deals'
            },
            {
              '@domain': '',
              '#text': 'audio'
            },
            {
              '@domain': '',
              '#text': 'tech'
            },
            {
              '@domain': '',
              '#text': 'anker'
            },
            {
              '@domain': '',
              '#text': 'headphones'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 11:40:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1824137728'
          },
          'creator': 'Shep McAllister on Kinja Deals, shared by Shep McAllister to Kotaku'
        },
        {
          'title': 'The ArenaNet Catastrophe Has The Whole Game Industry Rethinking Harassment Policies',
          'link': 'https://kotaku.com/in-the-wake-of-arenanet-firings-game-studios-rethink-t-1827591298',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--K06WLl9u--/c_fit,fl_progressive,q_80,w_636/aqz8pniciobuofsqoyl8.jpg\' /><p><a href=\'https://kotaku.com/guild-wars-2-writers-fired-for-calling-out-fan-on-twitt-1827401422\' rel=\'nofollow\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://kotaku.com/guild-wars-2-writers-fired-for-calling-out-fan-on-twitt-1827401422&#39;, {metric25:1})\'>One week ago<\/a>, two<em>Guild Wars 2<\/em> narrative designers, Jessica Price and Peter Fries, were fired after  Price called out a player of the game on Twitter, prompting widespread backlash. Since then, mobs have tried to employ similar tactics against more women, and game development studios have had to take a hard look at…<\/p><p><a href=\'https://kotaku.com/in-the-wake-of-arenanet-firings-game-studios-rethink-t-1827591298\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'arenanet'
            },
            {
              '@domain': '',
              '#text': 'guild wars 2'
            },
            {
              '@domain': '',
              '#text': 'social media'
            },
            {
              '@domain': '',
              '#text': 'harassment'
            },
            {
              '@domain': '',
              '#text': 'sexism'
            },
            {
              '@domain': '',
              '#text': 'obsidian'
            },
            {
              '@domain': '',
              '#text': 'hinterland'
            },
            {
              '@domain': '',
              '#text': 'double fine'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 23:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827591298'
          },
          'creator': 'Nathan Grayson'
        },
        {
          'title': 'Animal Crossing On GameCube Can Actually Play Any NES Game',
          'link': 'https://kotaku.com/someone-discovered-a-hidden-feature-in-animal-crossings-1827591135',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--dI0ChL9I--/c_fit,fl_progressive,q_80,w_636/onvvcu4zmqgbp27tzamd.png\' /><p>A deleted feature from <em>Animal Crossing<\/em> for the GameCube would have let you play a library of 8-bit Nintendo Entertainment System games off of your memory card, a passionate and talented fan of the game found out this week.<br><\/p><p><a href=\'https://kotaku.com/someone-discovered-a-hidden-feature-in-animal-crossings-1827591135\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'animal crossing'
            },
            {
              '@domain': '',
              '#text': 'gamecube'
            },
            {
              '@domain': '',
              '#text': 'emulation'
            },
            {
              '@domain': '',
              '#text': 'emulator'
            },
            {
              '@domain': '',
              '#text': 'homebrew'
            },
            {
              '@domain': '',
              '#text': 'nes'
            },
            {
              '@domain': '',
              '#text': 'memory card'
            },
            {
              '@domain': '',
              '#text': 'kotakucore'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 22:35:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827591135'
          },
          'creator': 'Ethan Gach'
        },
        {
          'title': 'The 10 Best Deals of July 13, 2018',
          'link': 'https://kinjadeals.theinventory.com/the-10-best-deals-of-july-13-2018-1827589280',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--plhpaPH_--/c_fit,fl_progressive,q_80,w_636/twbcew5wrs2wx9wcfjlv.gif\' /><p>We see a lot of deals around the web over on <a href=\'https://deals.kinja.com/\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://deals.kinja.com/&#39;, {metric25:1})\'>Kinja Deals<\/a>, but these were our ten favorites today.<br><\/p><p><a href=\'https://kinjadeals.theinventory.com/the-10-best-deals-of-july-13-2018-1827589280\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kinja deals'
            },
            {
              '@domain': '',
              '#text': 'deals'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 21:17:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827589280'
          },
          'creator': 'Erica Offutt on Kinja Deals, shared by Erica Offutt to Kotaku'
        },
        {
          'title': 'Racing Games Are Coming Back In A Big Way',
          'link': 'https://kotaku.com/racing-games-are-coming-back-in-a-big-way-1827589512',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--KLK8QOLo--/c_fit,fl_progressive,q_80,w_636/do0zeqxbixmqzxkbvuft.jpg\' /><p>Within the last month, some amazing racing games have released. It’s been something of a micro-renaissance. These games—<em>Wreckfest, Danger Zone 2<\/em>, and<em>Onrush—<\/em>succeed because they avoid more serious minded gear-grinding in favor of going fast and crashing loud. While there’s joy to be had in examining a 1958 Aston…<\/p><p><a href=\'https://kotaku.com/racing-games-are-coming-back-in-a-big-way-1827589512\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'racing games'
            },
            {
              '@domain': '',
              '#text': 'racing game'
            },
            {
              '@domain': '',
              '#text': 'onrush'
            },
            {
              '@domain': '',
              '#text': 'wreck'
            },
            {
              '@domain': '',
              '#text': 'burnout'
            },
            {
              '@domain': '',
              '#text': 'burnout paradise'
            },
            {
              '@domain': '',
              '#text': 'danger zone 2'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 21:30:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827589512'
          },
          'creator': 'Heather Alexandra'
        },
        {
          'title': 'What Are You Playing This Weekend?',
          'link': 'https://kotaku.com/what-are-you-playing-this-weekend-1827587835',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--2DYlYwBw--/c_fit,fl_progressive,q_80,w_636/sxddt1lgsi3def0odcar.jpg\' /><p>The weekend is for making the heck out of this <a href=\'https://thetakeout.com/recipe-bourbon-peach-cobbler-crock-pot-1827526303\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://thetakeout.com/recipe-bourbon-peach-cobbler-crock-pot-1827526303&#39;, {metric25:1})\'>slow cooker peach cobbler<\/a> from our excellent sister site<em>The Takeout<\/em>. Using the slow cooker means you can bake<em>and<\/em> play video games at the same time!<br><\/p><p><a href=\'https://kotaku.com/what-are-you-playing-this-weekend-1827587835\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'tell us'
            },
            {
              '@domain': '',
              '#text': 'tell us dammit'
            },
            {
              '@domain': '',
              '#text': 'kotakucore'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 21:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827587835'
          },
          'creator': 'Riley MacLeod'
        },
        {
          'title': 'The Left\'s Most Notorious Podcasters Are Divided On Video Games',
          'link': 'https://kotaku.com/the-lefts-most-notorious-podcasters-are-divided-on-vide-1827588122',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--KezVghr5--/c_fit,fl_progressive,q_80,w_636/gb6j5qumdybaq6kmawjx.jpg\' /><p>On a sticky summer night outside a bar in Brooklyn, Felix Biederman, one of the co-host of political podcast Chapo Trap House, was talking to me about <em>Metal Gear Solid 2<\/em> as I sipped a rosé. “It’s the most prophetic video game of all time,” he said.<br><\/p><p><a href=\'https://kotaku.com/the-lefts-most-notorious-podcasters-are-divided-on-vide-1827588122\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'chapo trap house'
            },
            {
              '@domain': '',
              '#text': 'politics'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 20:40:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827588122'
          },
          'creator': 'Gita Jackson'
        },
        {
          'title': 'Tips For Playing Octopath Traveler',
          'link': 'https://kotaku.com/tips-for-playing-octopath-traveler-1827586581',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--a_K9xjfb--/c_fit,fl_progressive,q_80,w_636/ctg07gvhudliinh2ggai.jpg\' /><p><em>Octopath Traveler<\/em> is a complicated game with a lot of intricate mechanics. I’ve played many hours of it. Now it is time for me to share my knowledge with you.<br><\/p><p><a href=\'https://kotaku.com/tips-for-playing-octopath-traveler-1827586581\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'octopath traveler'
            },
            {
              '@domain': '',
              '#text': 'tips'
            },
            {
              '@domain': '',
              '#text': 'before you start'
            },
            {
              '@domain': '',
              '#text': 'kotakucore'
            },
            {
              '@domain': '',
              '#text': 'nintendo'
            },
            {
              '@domain': '',
              '#text': 'switch'
            },
            {
              '@domain': '',
              '#text': 'square enix'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 20:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827586581'
          },
          'creator': 'Jason Schreier'
        },
        {
          'title': 'Trick Out Your Touch Bar With These Creative Apps and Games',
          'link': 'https://lifehacker.com/trick-out-your-touch-bar-with-these-creative-apps-and-g-1827583635',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--heAK92GC--/c_fit,fl_progressive,q_80,w_636/unwjcslkrqg9ajnuikoy.png\' /><p>I don’t know about you, but I’m still on the fence about Apple’s Touch Bar. The most use I get out of it is accidentally tapping the virtual “back” button in my browser when trying to press a number key. That, and I mainly use the Touch Bar to adjust my MacBook’s brightness and volume. (Perhaps I need to <a href=\'https://lifehacker.com/customize-your-macs-touch-bar-with-bettertouchtool-1827125801\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://lifehacker.com/customize-your-macs-touch-bar-with-bettertouchtool-1827125801&#39;, {metric25:1})\'>configure…<\/a><\/p><p><a href=\'https://lifehacker.com/trick-out-your-touch-bar-with-these-creative-apps-and-g-1827583635\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'the touch bar is fun and useful'
            },
            {
              '@domain': '',
              '#text': 'touch bar'
            },
            {
              '@domain': '',
              '#text': 'mac'
            },
            {
              '@domain': '',
              '#text': 'macos'
            },
            {
              '@domain': '',
              '#text': 'apple'
            },
            {
              '@domain': '',
              '#text': 'macbook'
            },
            {
              '@domain': '',
              '#text': 'laptop'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 19:15:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827583635'
          },
          'creator': 'David Murphy on Lifehacker, shared by Riley MacLeod to Kotaku'
        },
        {
          'title': 'Mondo\'s First Ghostbusters Poster Ever Is Coming to San Diego Comic-Con ',
          'link': 'https://io9.gizmodo.com/mondos-first-ghostbusters-poster-ever-is-coming-to-san-1827558559',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--8JCpTae6--/c_fit,fl_progressive,q_80,w_636/zbdsbkycfvp3drxbmw4j.jpg\' /><p>Go into the archive on Mondo’s website and search “Ghostbusters.” Incredibly, <a href=\'https://mondotees.com/search?type=product&amp;q=ghostbusters\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://mondotees.com/search?type=product&amp;q=ghostbusters&#39;, {metric25:1})\'>no posters come up<\/a>—which seems impossible, considering the company has been making movie posters based on basically<a href=\'https://io9.gizmodo.com/if-we-cant-have-new-universal-monster-movies-at-least-1822136655#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/if-we-cant-have-new-universal-monster-movies-at-least-1822136655#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>every<\/a><a href=\'https://io9.gizmodo.com/the-war-of-the-spider-man-homecoming-posters-rages-on-1796608570#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/the-war-of-the-spider-man-homecoming-posters-rages-on-1796608570#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>single<\/a><a href=\'https://io9.gizmodo.com/this-beautiful-classic-disney-inspired-art-show-is-a-t-1794631937#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/this-beautiful-classic-disney-inspired-art-show-is-a-t-1794631937#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>property<\/a><a href=\'https://io9.gizmodo.com/dont-freak-out-galaxy-quest-fans-but-youre-going-to-n-1779938049#_ga=2.76317589.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/dont-freak-out-galaxy-quest-fans-but-youre-going-to-n-1779938049#_ga=2.76317589.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>there<\/a><a href=\'https://io9.gizmodo.com/a-ton-of-unbelievable-new-star-wars-and-indiana-jones-a-1765250578#_ga=2.76317589.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/a-ton-of-unbelievable-new-star-wars-and-indiana-jones-a-1765250578#_ga=2.76317589.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>is<\/a> for<a href=\'https://io9.gizmodo.com/mondos-fantastic-pop-culture-posters-are-getting-a-gorg-1796719432#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://io9.gizmodo.com/mondos-fantastic-pop-culture-posters-are-getting-a-gorg-1796719432#_ga=2.87393432.1628579044.1531087785-1597414124.1522872342&#39;, {metric25:1})\'>over a decade<\/a>. That’s about to change. Today, io9 is excited to reveal Mondo’s first<em><\/em>…<\/p><p><a href=\'https://io9.gizmodo.com/mondos-first-ghostbusters-poster-ever-is-coming-to-san-1827558559\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'ghostbusters'
            },
            {
              '@domain': '',
              '#text': 'mondo'
            },
            {
              '@domain': '',
              '#text': 'exclusive'
            },
            {
              '@domain': '',
              '#text': 'tom whalen'
            },
            {
              '@domain': '',
              '#text': 'comic con 2018'
            },
            {
              '@domain': '',
              '#text': 'san diego comic con'
            },
            {
              '@domain': '',
              '#text': 'posters'
            },
            {
              '@domain': '',
              '#text': 'this is awesome'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 16:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827558559'
          },
          'creator': 'Germain Lussier on io9, shared by Riley MacLeod to Kotaku'
        },
        {
          'title': 'You Should Probably Replace Your Surge Protector, So Save 20% On a Great One',
          'link': 'https://kinjadeals.theinventory.com/you-should-probably-replace-your-surge-protector-so-sa-1827582039',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--6lQQam3z--/c_fit,fl_progressive,q_80,w_636/p72igxo21uebbkn70ybn.jpg\' /><p>Belkin’s 12-outlet surge protector is <a href=\'https://co-op.theinventory.com/these-are-your-favorite-home-theater-surge-protectors-1794260054\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;https://co-op.theinventory.com/these-are-your-favorite-home-theater-surge-protectors-1794260054&#39;, {metric25:1})\'>one of our readers’ favorites<\/a> (and the one I use behind my TV), and<a rel=\'nofollow\' data-amazonasin=\'B000J2EN4S\' data-amazonsubtag=\'[p|1827582039[a|B000J2EN4S[au|5727177402741770316[b|kotaku\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - You Should Probably Replace Your Surge Protector, So Save 20% On a Great One&#39;, &#39;B000J2EN4S&#39;);window.ga(&#39;unique.send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - You Should Probably Replace Your Surge Protector, So Save 20% On a Great One&#39;, &#39;B000J2EN4S&#39;);\' data-amazontag=\'kotakuamzn-20\' href=\'https://www.amazon.com/gp/product/B000J2EN4S/?tag=kotakuamzn-20&amp;ascsubtag=4529d2fe9ece60707b9f47910d060cb2f4e33b56&amp;rawdata=[t|link[p|1827582039[a|B000J2EN4S[au|5727177402741770316[b|kotaku\'>Prime members can get it for $14 today<\/a>, or 20% off. Just note that you won’t see the final price until you get to checkout.<a href=\'https://lifehacker.com/why-you-should-periodically-replace-your-surge-protecto-1693447062\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://lifehacker.com/why-you-should-periodically-replace-your-surge-protecto-1693447062&#39;, {metric25:1})\'>Surge protectors actually wear out over time<\/a>, so if you haven’t replaced yours in awhile,…<\/p><p><a href=\'https://kinjadeals.theinventory.com/you-should-probably-replace-your-surge-protector-so-sa-1827582039\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kinja deals'
            },
            {
              '@domain': '',
              '#text': 'deals'
            },
            {
              '@domain': '',
              '#text': 'amazon deals'
            },
            {
              '@domain': '',
              '#text': 'belkin deals'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 17:35:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827582039'
          },
          'creator': 'Shep McAllister on Kinja Deals, shared by Shep McAllister to Kotaku'
        },
        {
          'title': 'Rest In Pieces, Friday The 13th: The Game',
          'link': 'https://kotaku.com/rest-in-pieces-friday-the-13th-the-game-1827582283',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--sEvLCwvI--/c_fit,fl_progressive,q_80,w_636/yk7mlar3wwsdceqvdldc.jpg\' /><p>Last month, the ongoing legal battles over the rights of the <em>Friday the 13th<\/em> franchise caught up with the game,<a href=\'https://kotaku.com/friday-the-13th-game-loses-dlc-because-of-legal-battle-1826735815\' rel=\'nofollow\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://kotaku.com/friday-the-13th-game-loses-dlc-because-of-legal-battle-1826735815&#39;, {metric25:1})\'>bringing its development to a halt<\/a>. New content was “unfeasible now or in the future” due to the lawsuit, the game’s publisher<a href=\'http://f13game.com/news/end-of-content/\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;External link&#39;, &#39;http://f13game.com/news/end-of-content/&#39;, {metric25:1})\'>said<\/a>. Unlike Jason Voorhees,<em>Friday the 13th<\/em> ain’t coming back. It’s a terribly…<\/p><p><a href=\'https://kotaku.com/rest-in-pieces-friday-the-13th-the-game-1827582283\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'friday the 13th'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            },
            {
              '@domain': '',
              '#text': 'gun media'
            },
            {
              '@domain': '',
              '#text': 'illfonic'
            },
            {
              '@domain': '',
              '#text': 'horror games'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 19:30:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827582283'
          },
          'creator': 'Stacie Ponder'
        },
        {
          'title': 'The Only Rule Of Fortnite Golf Is Don\'t Get Killed ',
          'link': 'https://kotaku.com/the-only-rule-of-fortnite-golf-is-stay-alive-1827585187',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--zKTdIMfv--/c_fit,fl_progressive,q_80,w_636/anzhfykt04oqo4kfwh9s.png\' /><p><em>Fortnite<\/em> got<a href=\'https://kotaku.com/fortnites-season-5-patch-is-live-heres-whats-in-it-1827536732\' rel=\'nofollow\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://kotaku.com/fortnites-season-5-patch-is-live-heres-whats-in-it-1827536732&#39;, {metric25:1})\'>a huge patch yesterday<\/a> for the start of Season Five. Among other things like the addition of portals and some changes to how challenges work, the game also got a new emote that effectively lets players golf. For now, the mini-game feels barebones and disorganized. You have to keep score yourself and you…<\/p><p><a href=\'https://kotaku.com/the-only-rule-of-fortnite-golf-is-stay-alive-1827585187\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'fortnite battle royale'
            },
            {
              '@domain': '',
              '#text': 'fortnite'
            },
            {
              '@domain': '',
              '#text': 'epic games'
            },
            {
              '@domain': '',
              '#text': 'golf'
            },
            {
              '@domain': '',
              '#text': 'kotakucore'
            },
            {
              '@domain': '',
              '#text': 'season five'
            },
            {
              '@domain': '',
              '#text': 'simulation'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 19:10:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827585187'
          },
          'creator': 'Ethan Gach'
        },
        {
          'title': 'Where Are Prince Of Persia: The Sands Of Time\'s Developers Now?',
          'link': 'https://kotaku.com/where-are-prince-of-persia-the-sands-of-times-develope-1827556681',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--TOy4sg6W--/c_fit,fl_progressive,q_80,w_636/bqlih3urgqbbksrtnwwv.jpg\' /><p>Who makes a game? And where do they go when it’s over? For approximately 150 people who worked on <em>Prince of Persia: The Sands of Time<\/em>, the answer to the latter question can range from staying at Ubisoft to going indie to becoming a wedding photographer.<br><\/p><p><a href=\'https://kotaku.com/where-are-prince-of-persia-the-sands-of-times-develope-1827556681\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'prince of persia'
            },
            {
              '@domain': '',
              '#text': 'ubisoft'
            },
            {
              '@domain': '',
              '#text': 'prince of persia the sands of time'
            },
            {
              '@domain': '',
              '#text': 'assassins creed'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 18:30:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827556681'
          },
          'creator': 'Forest Lassman'
        },
        {
          'title': 'This $40 Keyboard Includes Ultra-Clicky Cherry MX Blue Switches',
          'link': 'https://kinjadeals.theinventory.com/this-40-keyboard-includes-ultra-clicky-cherry-mx-blue-1827574435',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--Igb22RbW--/c_fit,fl_progressive,q_80,w_636/ikpcsbkfvrriguqgs1lv.jpg\' /><p>$40 mechanical keyboard deals pop up fairly frequently, but they all use off-brand key switches. <a rel=\'nofollow\' data-amazonasin=\'B01BMJ0Y76\' data-amazonsubtag=\'[p|1827574435[a|B01BMJ0Y76[au|5727177402741770316[b|kotaku\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - This $40 Keyboard Includes Ultra-Clicky Cherry MX Blue Switches&#39;, &#39;B01BMJ0Y76&#39;);window.ga(&#39;unique.send&#39;, &#39;event&#39;, &#39;Commerce&#39;, &#39;kotaku - This $40 Keyboard Includes Ultra-Clicky Cherry MX Blue Switches&#39;, &#39;B01BMJ0Y76&#39;);\' data-amazontag=\'kotakuamzn-20\' href=\'https://www.amazon.com/dp/B01BMJ0Y76/?kinja_price=40&amp;tag=kotakuamzn-20&amp;ascsubtag=cf04f4d8ed780911dc9b6cfe82f268cb7691704b&amp;rawdata=[t|link[p|1827574435[a|B01BMJ0Y76[au|5727177402741770316[b|kotaku\'>This one from Gigabyte though features genuine, best-in-class Cherry MX switches<\/a>. They’re the Blue variety, which are<a href=\'https://lifehacker.com/how-to-choose-the-best-mechanical-keyboard-and-why-you-511140347\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://lifehacker.com/how-to-choose-the-best-mechanical-keyboard-and-why-you-511140347&#39;, {metric25:1})\'>very satisfying to type on<\/a>, but also extremely loud and clicky, so make sure your coworkers won’t mind.<br><\/p><p><a href=\'https://kinjadeals.theinventory.com/this-40-keyboard-includes-ultra-clicky-cherry-mx-blue-1827574435\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kinja deals'
            },
            {
              '@domain': '',
              '#text': 'deals'
            },
            {
              '@domain': '',
              '#text': 'amazon deals'
            },
            {
              '@domain': '',
              '#text': 'gigabyte deals'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 14:24:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827574435'
          },
          'creator': 'Shep McAllister on Kinja Deals, shared by Shep McAllister to Kotaku'
        },
        {
          'title': 'Please Don\'t Buy The Game I Released Two Years Ago',
          'link': 'https://kotaku.com/please-dont-buy-the-game-i-released-two-years-ago-1827581505',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--jLE1y4sF--/c_fit,fl_progressive,q_80,w_636/qn5htbnoobor0bp0lrku.jpg\' /><p><em>Fortnite<\/em> gets golf carts and portals, a<em>No Man’s Sky<\/em> dictator says he’s actually role-playing, the popular Chapo Trap House podcast starts a Twitch channel to get political about games, and<em>Octopath Traveler<\/em> is here. It’s all on today’s Kotaku XP, our weekly video series about hot game topics.<\/p><p><a href=\'https://kotaku.com/please-dont-buy-the-game-i-released-two-years-ago-1827581505\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'kotaku xp'
            },
            {
              '@domain': '',
              '#text': 'video'
            },
            {
              '@domain': '',
              '#text': 'kotaku core'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 17:30:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827581505'
          },
          'creator': 'Tim Rogers'
        },
        {
          'title': 'The modern era of the superhero movie begins in earnest with X-Men',
          'link': 'https://www.avclub.com/the-modern-era-of-the-superhero-movie-begins-in-earnest-1827373752',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--Qh2H7W7s--/c_fit,fl_progressive,q_80,w_636/jctnbfgwcctvvswtmvqy.jpg\' /><p>Throughout the ’80s and ’90s, the <a href=\'https://www.avclub.com/apocalypse-now-how-x-men-s-oldest-villain-brings-the-1798248024\' target=\'_blank\' rel=\'noopener\' onclick=\'window.ga(&#39;send&#39;, &#39;event&#39;, &#39;Embedded Url&#39;, &#39;Internal link&#39;, &#39;https://www.avclub.com/apocalypse-now-how-x-men-s-oldest-villain-brings-the-1798248024&#39;, {metric25:1})\'>X-Men comics<\/a> captured the collective imagination of the kids and teenagers who were willing to pay attention to comic books. The books had rich casts of characters. They had great storytelling, with long and ambitious plotlines that would stretch over months or even years. They had…<\/p><p><a href=\'https://www.avclub.com/the-modern-era-of-the-superhero-movie-begins-in-earnest-1827373752\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'age of heroes'
            },
            {
              '@domain': '',
              '#text': 'x men'
            },
            {
              '@domain': '',
              '#text': 'bryan singer'
            },
            {
              '@domain': '',
              '#text': 'hugh jackman'
            },
            {
              '@domain': '',
              '#text': 'halle berry'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 11:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827373752'
          },
          'creator': 'Tom Breihan on The A.V. Club, shared by Riley MacLeod to Kotaku'
        },
        {
          'title': 'How To Unlock Octopath Traveler\'s Post-Game Dungeon',
          'link': 'https://kotaku.com/how-to-unlock-octopath-travelers-post-game-dungeon-1827578949',
          'description': '<img src=\'https://i.kinja-img.com/gawker-media/image/upload/s--DUw-FV_K--/c_fit,fl_progressive,q_80,w_636/m6op3l4f0sknfddbbx0n.jpg\' /><p>Once you’re past the credits of <em>Octopath Traveler<\/em>, there’s one major hidden dungeon left to find. It’s not obvious how to get there, though, so allow us to give you a guide.<br><\/p><p><a href=\'https://kotaku.com/how-to-unlock-octopath-travelers-post-game-dungeon-1827578949\'>Read more...<\/a><\/p>',
          'category': [
            {
              '@domain': '',
              '#text': 'octopath traveler'
            },
            {
              '@domain': '',
              '#text': 'kotakucore'
            },
            {
              '@domain': '',
              '#text': 'nintendo'
            },
            {
              '@domain': '',
              '#text': 'nintendo switch'
            },
            {
              '@domain': '',
              '#text': 'square enix'
            }
          ],
          'pubDate': 'Fri, 13 Jul 2018 17:00:00 GMT',
          'guid': {
            '@isPermaLink': 'false',
            '#text': '1827578949'
          },
          'creator': 'Jason Schreier'
        }
      ]);
  }
}