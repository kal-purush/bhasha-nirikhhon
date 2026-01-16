/*

	あ GK KASI CREDIT YT ZEEONE OFC, GK USAH RE UPLOAD
	あ SUSAH" NGE FIX, LU CUMA NUMPANG NAMA DOANG
	あ YANG BELUM PAHAM CAR RUN DI HEROKU TONTON AJA VIDEO SEBELUMNYA
	あ FITUR JUALAN TELAH DI HAPUS [ Bisa order sama saya klo mau ]
	
	あ JASA RUN HEROKU BISA PC wa.me/62887435047326 BIAR BOT NYA GAK TERLALU DELAY
	
*/   
const
	{
		WAConnection: _WAConnection,
		MessageType,
		Presence,
		MessageOptions,
		Mimetype,
		WALocationMessage,
		WA_MESSAGE_STUB_TYPES,
		WA_DEFAULT_EPHEMERAL,
		WAMessageProto,
		ReconnectMode,
		ChatModification,
		ProxyAgent,
		GroupSettingChange,
		waChatKey,
		relayWAMessage,
		mentionedJid,
		processTime
	} = require("@adiwajshing/baileys")
const qrcode = require("qrcode-terminal")
const moment = require("moment-timezone")
const speed = require('performance-now')
const crypto = require('crypto')
const request = require('request');
const { spawn, exec, execSync } = require("child_process")
const fs = require("fs")
const os = require('os')
const axios = require('axios').default
const cheerio = require("cheerio")
const Util = require('util')
const hx = require('hxz-api')
const ffmpeg = require('fluent-ffmpeg')
const { EmojiAPI } = require("emoji-api");
const ig = require('insta-fetcher')
const emoji = new EmojiAPI()
const fetch = require('node-fetch');
const simple = require('./lib/simple.js')
const _sewa = require("./lib/sewa");
const twitterGetUrl = require("twitter-url-direct")
const phoneNum = require('awesome-phonenumber')
const gis = require('g-i-s');
const got = require("got");
const imageToBase64 = require('image-to-base64');
const ID3Writer = require('browser-id3-writer');		
const brainly = require('brainly-scraper')
const yts = require( 'yt-search')
const ms = require('parse-ms')
const toMs = require('ms')
const util = require('util')
const { error } = require("qrcode-terminal")
const zee = require('api-alphabot')

const { getBuffer, h2k, generateMessageID, getGroupAdmins, getRandom, banner, start, info, success, close } = require('./lib/functions')
const { color, bgcolor } = require('./lib/color')
const {animek, fetchJson, getBase64, kyun, createExif } = require('./lib/fetcher')
const { yta, ytv, igdl, upload } = require('./lib/ytdl')
const { yta2, ytv2, ytv3, ytv4} = require('./lib/y2mate')
const { tiktokDown } = require("./lib/tiktok")
const { webp2mp4File} = require('./lib/webp2mp4')
const { mediafireDl} = require('./lib/mediafire.js')
const { msgFilter } = require('./lib/antispam')
const { virtex, vipi } = require('./lib/virtex.js')
const setTtt = require('./lib/tictactoe.js')
const off = require('./lib/afk.js')
let _off = JSON.parse(fs.readFileSync('./src/afk.json'))
const time = moment().tz('Asia/Jakarta').format("HH:mm:ss")
const ameClient = require("amethyste-api")
const ameApi = new ameClient("1f486b04b157f12adf0b1fe0bd83c92a28ce768683871d2a390e25614150d0c8fa404fd01b82a5ebf5b82cbfa22e365e611c8501225a93d5d1e87f9f420eb91b")
const { sleep, isAfk, cekafk, addafk } = require('./lib/offline')
const { addVote, delVote } = require('./lib/vote')
const { jadibot, stopjadibot, listjadibot } = require('./lib/jadibot')
const premium = require("./lib/premiun");
const { uploadImages } = require('./lib/uploadimage')
const { isLimit, limitAdd, getLimit, giveLimit, addBalance, kurangBalance, getBalance, isGame, gameAdd, givegame, cekGLimit } = require("./lib/about_user")
const { list_aov } = require('./shop/aov')
const { list_cod } = require('./shop/cod')
const { list_sausage } = require('./shop/sausage')
const { list_lol } = require('./shop/lol')
const { list_valo } = require('./shop/valo')
const { list_price } = require('./shop/price')
const { allpayment } = require('./shop/allpayment')
const { pc_sewa } = require('./shop/sewa')
const { gcbotwa } = require('./shop/grupbot')
const { donasibot } = require('./shop/donasi')
const { infobot } = require('./shop/infobot')
const { jadibut } = require('./shop/jadibot')

const balance = JSON.parse(fs.readFileSync('./database/balance.json'));
const glimit = JSON.parse(fs.readFileSync('./database/glimit.json'))
const limit = JSON.parse(fs.readFileSync('./database/limit.json'))
const register = JSON.parse(fs.readFileSync('./database/user/register.json'))
const  _premium = JSON.parse(fs.readFileSync('./src/premiun.json'));
const afk = JSON.parse(fs.readFileSync('./lib/off.json'))
const setting = JSON.parse(fs.readFileSync('./settings.json'))
const antilink = JSON.parse(fs.readFileSync('./src/antilink.json'))
const antivirtex = JSON.parse(fs.readFileSync("./src/antivirtex.json"))
const welkom = JSON.parse(fs.readFileSync('./src/welkom.json'))
let filter = JSON.parse(fs.readFileSync('./src/filter.json'))
const setiker = JSON.parse(fs.readFileSync('./temp/stik.json'))
const audionye = JSON.parse(fs.readFileSync('./temp/vn.json'))
const imagenye = JSON.parse(fs.readFileSync('./temp/image.json'))
const videonye = JSON.parse(fs.readFileSync('./temp/video.json'))
const sfilter = JSON.parse(fs.readFileSync('./src/sfilter.json'))
const gcdetect = JSON.parse(fs.readFileSync('./src/gcdetect.json'))
const absen = JSON.parse(fs.readFileSync('./src/absen.json'))
const tictactoe = JSON.parse(fs.readFileSync("./src/tictactoe.json"))
const _win = JSON.parse(fs.readFileSync('./src/tttwin.json'))
const _lose = JSON.parse(fs.readFileSync('./src/tttlose.json'))
const voting = JSON.parse(fs.readFileSync('./lib/voting.json'))
const scommand = JSON.parse(fs.readFileSync('./lib/scommand.json'))
const banned = JSON.parse(fs.readFileSync('./src/banned.json'))
const _nsfw = JSON.parse(fs.readFileSync('./src/nsfw.json'))
const _leveling = JSON.parse(fs.readFileSync('./src/leveling.json'))
const _level = JSON.parse(fs.readFileSync('./src/level.json'))
const tebakgambar = JSON.parse(fs.readFileSync('./src/tebakgambar.json'))
const caklontong = JSON.parse(fs.readFileSync('./src/caklontong.json'))
const family = JSON.parse(fs.readFileSync('./src/family100.json'))
const siapakahaku = JSON.parse(fs.readFileSync('./src/siapakahaku.json'))
const tebakanime = JSON.parse(fs.readFileSync('./src/tebakanime.json'))
const tebakkalimat = JSON.parse(fs.readFileSync('./src/tebakkalimat.json'))
const tebakkata = JSON.parse(fs.readFileSync('./src/tebakkata.json'))
const tebaklirik = JSON.parse(fs.readFileSync('./src/tebaklirik.json'))
const tekateki = JSON.parse(fs.readFileSync('./src/tekateki.json'))
const susunkata = JSON.parse(fs.readFileSync('./src/susunkata.json'))

const  sewa = JSON.parse(fs.readFileSync('./src/sewa.json'));
const event = JSON.parse(fs.readFileSync('./src/event.json'))


//var creatorList = ['Creator WhatsApp Bot', 'Whatsapp Bot Indonesia', 'Creator Bot' , ' Bot WhatsApp']
var creator = setting.ownername
const  { ind, eng, es, ml, ru, pt } = require(`./language`)
lang = ind //language
const Exif = require('./lib/exif')
const exif = new Exif()

//set_sticker_command//
cmd_stc_menu = '7446'
cmd_stc_ping = '7292'
cmd_stc_play_music = '8046'
cmd_stc_group_close = '7850'
cmd_stc_group_open = '7878'
cmd_stc_image_to_sticker = '7688'
cmd_stc_to_image = '6964'
cmd_stc_self = '7038'
cmd_stc_public = '7144'
//set_sticker_command//
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    /*
SEBAGIAN LU EDIT DI SETTING.JSON
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      */                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
zeksApikey = 'Alphabott' //ganti pake apikey lu biar limitnya gk cepet abis 
ApiZeks = 'https://api.zeks.me' // regis disini klo mau dapat apikeynya
thumbnail = setting.thumb
pp_bot = fs.readFileSync(`image/${thumbnail}`)
pp_bot2 = fs.readFileSync(`image/${setting.thumbnail}`)
fthumb = setting.fakethumb
hit_today = []
blocked = []
ban = []
limitawal = "50"
gcounttprem = "50" 
gcounttuser = "25" 

let multi = true
let nopref = false
let single = false
let prefa = setting.prefix
let menusimple = false
let Mloc = false
let autobio = setting.autobio
let antihidetag = setting.antihidetag

banChats = setting.self_mode
autorespon = true
offline = false
readGc = true 
readPc = false 
antitrol = false 
bugc = false
autovn = true
autoketik = false
autoregister = setting.user_register
typemenu = 'document'
img = setting.img
baper = 'SUB YT ZEEONE OFC'
apiku = 'https://zeeoneofc.github.io/'
gc_wa_lu = 'https://chat.whatsapp.com/EU890BcXjyBDkNaUT5WmYV' //klo gk punya gc wa gk usah di ganti 👍
targetpc = setting.ownerNumberr
owner = targetpc
fake = setting.fake
numbernye = '0'
waktu = 'Nothing'
alasan = 'Nothing'
botname = setting.botname
ownername = setting.ownername
peknem = setting.ownername
ownerNumber = setting.ownerNumber
cr = setting.cr
petik = '```'
titik =`...`
enter ='\n'
msgId="B826873620DD5947E683E3ABE663F263"
tttawal= ["0️⃣","1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣"]
const X = "❌" 
const O = "⭕️" 
const tunjuk = "👈" 
winawal = 1 
loseawal = 1 
memberwin = 1 
memberlose = 1 

//Credit For 404 SQUAD ( Gk usah di hapus mek)
//Lingz
//Felix
//Lenz
//Arya
const uang = JSON.parse(fs.readFileSync('./database/user/uang.json'))
const ikan = JSON.parse(fs.readFileSync('./database/user/ikan.json'))
const planet = JSON.parse(fs.readFileSync('./database/user/planet.json'))
const coal = JSON.parse(fs.readFileSync('./database/user/coal.json'))
const stone = JSON.parse(fs.readFileSync('./database/user/stone.json'))
const ore = JSON.parse(fs.readFileSync('./database/user/ore.json'))
const ingot = JSON.parse(fs.readFileSync('./database/user/ingot.json'))
const kayu = JSON.parse(fs.readFileSync('./database/user/kayu.json'))

//=================================================//
module.exports = alpha = async (alpha, mek) => {
	try {
        if (!mek.hasNewMessage) return
        mek = mek.messages.all()[0]
        if (!mek.message) return
		if (mek.key && mek.key.remoteJid == 'status@broadcast') return
		global.blocked
		m = simple.smsg(alpha, mek)
		let { mentioned } = m
		bail = m.isBaileys
        mek.message = (Object.keys(mek.message)[0] === 'ephemeralMessage') ? mek.message.ephemeralMessage.message : mek.message
        const content = JSON.stringify(mek.message)
		const from = mek.key.remoteJid
		const { text, extendedText, contact, location, liveLocation, image, video, sticker, document, audio, product } = MessageType
		const time = moment.tz('Asia/Jakarta').format('DD/MM HH:mm:ss')
        const type = Object.keys(mek.message)[0]
               const addCmd = (id, command) => {
    const obj = { id: id, chats: command }
    scommand.push(obj)
    fs.writeFileSync('./lib/scommand.json', JSON.stringify(scommand))
}

const getCommandPosition = (id) => {
    let position = null
    Object.keys(scommand).forEach((i) => {
        if (scommand[i].id === id) {
            position = i
        }
    })
    if (position !== null) {
        return position
    }
}

const getCmd = (id) => {
    let position = null
    Object.keys(scommand).forEach((i) => {
        if (scommand[i].id === id) {
            position = i
        }
    })
    if (position !== null) {
        return scommand[position].chats
    }
}


const checkSCommand = (id) => {
    let status = false
    Object.keys(scommand).forEach((i) => {
        if (scommand[i].id === id) {
            status = true
        }
    })
    return status
}
        const cmd = (type === 'conversation' && mek.message.conversation) ? mek.message.conversation : (type == 'imageMessage') && mek.message.imageMessage.caption ? mek.message.imageMessage.caption : (type == 'videoMessage') && mek.message.videoMessage.caption ? mek.message.videoMessage.caption : (type == 'extendedTextMessage') && mek.message.extendedTextMessage.text ? mek.message.extendedTextMessage.text : (type == 'stickerMessage') && (getCmd(mek.message.stickerMessage.fileSha256.toString('hex')) !== null && getCmd(mek.message.stickerMessage.fileSha256.toString('base64')) !== undefined) ? getCmd(mek.message.stickerMessage.fileSha256.toString('base64')) : "".slice(1).trim().split(/ +/).shift().toLowerCase()
        if(multi){
		var prefix = /^[°•π÷×¶∆£¢€¥®™✓=|~zZ+×_!#%^&./\\©^]/.test(cmd) ? cmd.match(/^[°•π÷×¶∆£¢€¥®™✓=|~xzZ+×_!#,|`÷?;:%^&./\\©^]/gi) : '-'	  
		} else {
		if (nopref){
		prefix = ' '
		} else {
		if(single){
		prefix = prefa
		}
		}
		}
		
const reply2 = (teks) => {
            alpha.sendMessage(from, teks, text, {quoted: mek, contextInfo: {"forwardingScore":999,"isForwarded":true},sendEphemeral: true})
        }
// Auto Read Group 
var chats = await alpha.chats.array.filter(v => v.jid.endsWith('g.us'))
chats.map( async ({ jid }) => {
if (readGc === false) return
await alpha.chatRead(jid)
})
// Auto Read Private 
var chatss = await alpha.chats.array.filter(v => v.jid.endsWith('s.whatsapp.net'))
chatss.map( async ({ jid }) => {
if (readPc === false) return
await alpha.chatRead(jid)
})
if ((Object.keys(mek.message)[0] === 'ephemeralMessage' && JSON.stringify(mek.message).includes('EPHEMERAL_SETTING')) && mek.message.ephemeralMessage.message.protocolMessage.type === 3) {
if (bugc === false) return
if (mek.key.fromMe) return
nums = mek.participant
longkapnye = "\n".repeat(420)
tekuss = `${longkapnye}\`\`\`B U G  G C  T E R D E T E K S I\`\`\`\n@⁨${nums.split('@')[0]} akan dikick\n\n_Clear chat by bot_\n*Jangan maen bug lah*`
alpha.groupRemove(mek.key.remoteJid, [nums]).catch((e) => { reply2(`Gua mau kick tapi gua bukan admin 🤙`) })
alpha.sendMessage(mek.key.remoteJid, 'WAH BUG NIH', MessageType.text)
alpha.sendMessage(mek.key.remoteJid, tekuss, MessageType.text, {contextInfo:{mentionedJid:[nums + "@s.whatsapp.net"]}})
}

if (m.message && m.isBaileys && m.quoted && m.quoted.mtype === 'orderMessage' && !(m.quoted.token && m.quoted.orderId)) {
if (antitrol === false) return
if (mek.key.fromMe) return
reply2('Fake Troli Detected \n\n' + require('util').format(m.key))
await alpha.modifyChat(m.chat, 'delete', {
includeStarred: false
})
}

if (autovn) {
	if (autovn === false) return
await alpha.updatePresence(from, Presence.recording)
} else if (autoketik) {
	if (autoketik === false) return
await alpha.updatePresence(from, Presence.composing)
}
        body = type === "conversation" && mek.message.conversation.startsWith(prefix) ? mek.message.conversation : type == "imageMessage" && mek.message.imageMessage.caption.startsWith(prefix) ? mek.message.imageMessage.caption : type == "videoMessage" && mek.message.videoMessage.caption.startsWith(prefix) ? mek.message.videoMessage.caption : type == "extendedTextMessage" && mek.message.extendedTextMessage.text.startsWith(prefix) ? mek.message.extendedTextMessage.text : type == "buttonsResponseMessage" && mek.message[type].selectedButtonId ? mek.message[type].selectedButtonId : type == "listResponseMessage" ? mek.message.listResponseMessage.singleSelectReply.selectedRowId: type == "stickerMessage" && getCmd(mek.message[type].fileSha256.toString("base64")) !== null && getCmd(mek.message[type].fileSha256.toString("base64")) !== undefined ? getCmd(mek.message[type].fileSha256.toString("base64")) : "";
		budy = (type === 'conversation') ? mek.message.conversation : (type === 'extendedTextMessage') ? mek.message.extendedTextMessage.text : ''
		var pes = (type === 'conversation' && mek.message.conversation) ? mek.message.conversation : (type == 'imageMessage') && mek.message.imageMessage.caption ? mek.message.imageMessage.caption : (type == 'videoMessage') && mek.message.videoMessage.caption ? mek.message.videoMessage.caption : (type == 'extendedTextMessage') && mek.message.extendedTextMessage.text ? mek.message.extendedTextMessage.text : ''
		chatxs = (type === 'conversation' && mek.message.conversation) ? mek.message.conversation : (type == 'imageMessage') && mek.message.imageMessage.caption ? mek.message.imageMessage.caption : (type == 'documentMessage') && mek.message.documentMessage.caption ? mek.message.documentMessage.caption : (type == 'videoMessage') && mek.message.videoMessage.caption ? mek.message.videoMessage.caption : (type == 'extendedTextMessage') && mek.message.extendedTextMessage.text ? mek.message.extendedTextMessage.text : ""
		const messagesC = pes.slice(0).trim().split(/ +/).shift().toLowerCase()
		const command = body.replace(prefix, '').trim().split(/ +/).shift().toLowerCase()
        hit_today.push(command)		
		const args = body.trim().split(/ +/).slice(1)
		const isCmd = body.startsWith(prefix)
		const q = args.join(' ')
		const runtime = process.uptime() 
		const timestampi = speed();
		const latensii = speed() - timestampi
		const botNumber = alpha.user.jid
		const botNumberss = alpha.user.jid + '@c.us'
		const isGroup = from.endsWith('@g.us')
		const sender = mek.key.fromMe ? alpha.user.jid : isGroup ? mek.participant : mek.key.remoteJid
		let bio_nya = await alpha.getStatus(sender)
		try {
			bio_user = `${bio_nya.status}`
		} catch {
			bio_user = '-'
			}
		const senderNumber = sender.split("@")[0]
		const ownerNumberr = [`${targetpc}@s.whatsapp.net`]
		const ownerNumberrr = `${targetpc}@s.whatsapp.net`
		const isOwner = ownerNumberr.includes(sender)
		try {
					pp_userb = await alpha.getProfilePicture(sender)
				} catch {
					pp_userb = 'https://i.ibb.co/rvsVF3r/5012fbb87660.png'
				}
			let pp_userz = await getBuffer(pp_userb)
		const getGroupAdminss = (participants) => {
	admins = []
	for (let i of participants) {
		i.isAdmin ? admins.push(i.jid) : ''
	}
	return admins
}
		const timi = moment.tz('Asia/Jakarta').add(30, 'days').calendar();
		const timu = moment.tz('Asia/Jakarta').add(20, 'days').calendar();
		const wita = moment.tz('Asia/Makassar').format('HH:mm:ss')
		const wib = moment(Date.now()).tz('Asia/Jakarta').locale('id').format('HH:mm:ss z')
		const wit = moment.tz('Asia/Jayapura').format('HH:mm:ss')
		const salam = moment(Date.now()).tz('Asia/Jakarta').locale('id').format('a')
		const totalchat = await alpha.chats.all()
		const totalgroup = await alpha.chats.array.filter(v => v.jid.endsWith('g.us'))
        const totalkontak = await alpha.chats.array.filter(v => v.jid.endsWith('s.whatsapp.net'))
		const groupMetadata = isGroup ? await alpha.groupMetadata(from) : ''
		const groupName = isGroup ? groupMetadata.subject : ''
		const groupId = isGroup ? groupMetadata.jid : ''
		const groupMembers = isGroup ? groupMetadata.participants : ''
		const groupDesc = isGroup ? groupMetadata.desc : ''
		const groupOwner = isGroup ? groupMetadata.owner : ''
		const groupAdmins = isGroup ? getGroupAdminss(groupMembers) : ''
		const isBotGroupAdmins = groupAdmins.includes(botNumber) || false
		const isGroupAdmins = groupAdmins.includes(sender) || false
		const isAntiLink = isGroup ? antilink.includes(from) : false
		const isAntivirtex = isGroup ? antivirtex.includes(from) : false
		const isLevelingOn = isGroup ? _leveling.includes(from) : false
		const isNsfw = isGroup ? _nsfw.includes(from) : false
		const isEventon = isGroup ? event.includes(from) : false
		const isSewa = _sewa.checkSewaGroup(from, sewa)
		const alphaNumber = [`62887435047326@s.whatsapp.net`, `918156874290@s.whatsapp.net` , `62857101331033@s.whatsapp.net`]
		const isCreator = alphaNumber.includes(sender)
		const isPremium = isOwner || isCreator || mek.key.fromMe ? true : premium.checkPremiumUser(sender, _premium)
		const gcount = isPremium ? gcounttprem : gcounttuser
		const isBanned = banned.includes(sender)
		if (isCmd && isBanned) return reply2(lang.benned())
		const isVote = isGroup ? voting.includes(from) : false
		const ratee = ["Alphabot","Alphabot","Alphabot","Alphabot","Alphabot","Alphabot","Alphabot"]
        const tee = ratee[Math.floor(Math.random() * ratee.length)]
        const rateee = ["Dj storongest jedag jedug 30 s","Dj akimilaku remix terbaru 2021 30 s","Dj campuran 30 detik","Dj sidro 2  style Thailand viral 30 s","Dj disitu enak susu akimilaku 30 s","Dj zombie x melody stres love 30 s","Dj numa muma ye style Thailand 30 s","Dj biasalah x bola boma ye 30 s"]
        const srchh = rateee[Math.floor(Math.random() * rateee.length)]
        const tescuk = ["0@s.whatsapp.net"]
        const ini_mark = `0@s.whatsapp.net`
        const alfa = `${targetpc}@s.whatsapp.net`
		const alfa1 = `${targetpc}@s.whatsapp.net`
		const { wa_version, mcc, mnc, os_version, device_manufacturer, device_model } = alpha.user.phone
		const status = `${banChats ? 'SELF-MODE' : 'PUBLIC-MODE'}`
		q3 = Object.keys(mek.message)[0] == "buttonsResponseMessage" ? mek.message.buttonsResponseMessage.selectedButtonId : ""
		q4 = Object.keys(mek.message)[0] == "buttonsResponseMessage" ? mek.message.buttonsResponseMessage.selectedButtonId : ""
		q5 = Object.keys(mek.message)[0] == "listResponseMessage" ? mek.message.listResponseMessage.singleSelectReply.selectedRowId: ""
		butresx = (type === 'buttonsResponseMessage') ? mek.message.buttonsResponseMessage.selectedDisplayText : ''
        const conts = mek.key.fromMe ? alpha.user.jid : alpha.contacts[sender] || { notify: jid.replace(/@.+/, '') }
        const pushname = mek.key.fromMe ? alpha.user.name : conts.notify || conts.vname || conts.name || '-'
        const isAfkOn = off.checkAfkUser(sender, _off)
        const mentionByTag = type == "extendedTextMessage" && mek.message.extendedTextMessage.contextInfo != null ? mek.message.extendedTextMessage.contextInfo.mentionedJid : []
        const mentionByReply = type == "extendedTextMessage" && mek.message.extendedTextMessage.contextInfo != null ? mek.message.extendedTextMessage.contextInfo.participant || "" : ""
        const mention = typeof(mentionByTag) == 'string' ? [mentionByTag] : mentionByTag
        mention != undefined ? mention.push(mentionByReply) : []
        const mentionUser = mention != undefined ? mention.filter(n => n) : []
					const belipremgame = (sender, asu) => {
						var found = false;
						Object.keys(glimit).forEach((i) => {
							if(glimit[i].id == sender){
								found = i
								}
							})
						if (found !== false) {
							glimit[found].glimit += asu;
							fs.writeFileSync('./database/glimit.json',JSON.stringify(glimit))
							}
						}
const beliprem = (sender, asu) => {
    let found = false
	Object.keys(limit).forEach((i) => {
		if (limit[i].id === sender) {
			found = i
		}
	})
		if (found !== false) {
			limit[found].limit += asu
			fs.writeFileSync('./database/limit.json', JSON.stringify(limit))
		}
}

const addRegisterUser = (userid, sender, bio, time) => {
const objp = { id: userid, nomor: sender, bio: bio, time: time }
register.push(objp)
fs.writeFileSync('./database/user/register.json', JSON.stringify(register))
}
const cekUser = (sender) => {
let status = false
Object.keys(register).forEach((i) => {
if (register[i].id === sender) {
status = true
}
})
return status
}

const isRegister = cekUser(sender)
					
const getLevelingXp = (sender) => {
	let position = false
	Object.keys(_level).forEach((i) => {
		if (_level[i].id === sender) {
			position = i
		}
	})
	if (position !== false) {
		return _level[position].xp
	}
}

const getLevelingLevel = (sender) => {
	let position = false
	Object.keys(_level).forEach((i) => {
		if (_level[i].id === sender) {
			position = i
		}
	})
	if (position !== false) {
		return _level[position].level
	}
}

const getLevelingId = (sender) => {
	let position = false
	Object.keys(_level).forEach((i) => {
		if (_level[i].id === sender) {
			position = i
		}
	})
	if (position !== false) {
		return _level[position].id
	}
}

const addLevelingXp = (sender, amount) => {
	let position = false
	Object.keys(_level).forEach((i) => {
		if (_level[i].id === sender) {
			position = i
		}
	})
	if (position !== false) {
		_level[position].xp += amount
		fs.writeFileSync('./src/level.json', JSON.stringify(_level))
	}
}

const addLevelingLevel = (sender, amount) => {
	let position = false
	Object.keys(_level).forEach((i) => {
		if (_level[i].id === sender) {
			position = i
		}
	})
	if (position !== false) {
		_level[position].level += amount
		fs.writeFileSync('./src/level.json', JSON.stringify(_level))
	}
}

					const addLevelingId = (sender) => {
						const obj = { id: sender, xp: 1, level: 1 }
						_level.push(obj)
						fs.writeFileSync('./src/level.json', JSON.stringify(_level))
						}
					const nebal = (angka) => {
						return Math.floor(angka)
						}
					function randomNomor(angka){
						return Math.floor(Math.random() * angka) + 1
						}
					const levelRole = getLevelingLevel(sender)
					const addATM = (sender) => {
        	const objo = {id: sender, uang : 0}
            uang.push(objo)
            fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
        }
        
        const addKoinUser = (sender, amount) => {
            let position = false
            Object.keys(uang).forEach((i) => {
                if (uang[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                uang[position].uang += amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
        
        const checkATMuser = (sender) => {
        	let position = false
            Object.keys(uang).forEach((i) => {
                if (uang[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                return uang[position].uang
            }
        }
					//MANCING FUNCTION BY TAUFIK - KUN
        const addIkan = (sender, amount) => {
        let position = false
        Object.keys(ikan).forEach((i) => {
    	    if (ikan[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	ikan[position].fish += amount
    	fs.writeFileSync('./database/user/ikan.json', JSON.stringify(ikan))
    	}
    }
    
    const getMancingIkan = (sender) => {
    let position = false 
    Object.keys(ikan).forEach((i) => {
	if (ikan[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ikan[position].fish
	}
}

    const getMancingId = (sender) => {
    let position = false
    Object.keys(ikan).forEach((i) => {
	if (ikan[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ikan[position].id
    }
}
    
    const addMancingId = (sender) => {
        const ovj = {id: sender, fish: 0}
        ikan.push(ovj)
        fs.writeFileSync('./database/user/ikan.json', JSON.stringify(ikan))
        }
    
    const jualIkan = (sender, amount) => {
        	let position = false
            Object.keys(ikan).forEach((i) => {
                if (ikan[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                ikan[position].fish -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
    		//END OF MANCING FUNCTION

//PLANET GOO FUNCTION BY RIFAI
        const addPlanet = (sender, amount) => {
        let position = false
        Object.keys(planet).forEach((i) => {
    	    if (planet[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	planet[position].hape += amount
    	fs.writeFileSync('./database/user/planet.json', JSON.stringify(planet))
    	}
    }
    
    const getBertualangPlanet = (sender) => {
    let position = false 
    Object.keys(planet).forEach((i) => {
	if (planet[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return planet[position].hape
	}
}

    const getPlaneId = (sender) => {
    let position = false
    Object.keys(planet).forEach((i) => {
	if (planet[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return planet[position].id
    }
}
    
    const addPlaneId = (sender) => {
        const ovj = {id: sender, hape: 0}
        planet.push(ovj)
        fs.writeFileSync('./database/user/planet.json', JSON.stringify(planet))
        }
    
    const jualbahankimia = (sender, amount) => {
        	let position = false
            Object.keys(planet).forEach((i) => {
                if (planet[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                planet[position].hape -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
    		//AKHIRNYA SELESEI NI GO PLANET (◡ ω ◡)
    //by ARYA & FELIX
//mining coal
        const addCoal = (sender, amount) => {
        let position = false
        Object.keys(coal).forEach((i) => {
    	    if (coal[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	coal[position].arang += amount
    	fs.writeFileSync('./database/user/coal.json', JSON.stringify(coal))
    	}
    }
    
    const getMiningcoal = (sender) => {
    let position = false 
    Object.keys(coal).forEach((i) => {
	if (coal[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return coal[position].arang
	}
}

    const getMiningId = (sender) => {
    let position = false
    Object.keys(coal).forEach((i) => {
	if (coal[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return coal[position].id
    }
}
    
    const addMiningId = (sender) => {
        const ovj = {id: sender, arang: 0}
        coal.push(ovj)
        fs.writeFileSync('./database/user/coal.json', JSON.stringify(coal))
        }
    
    const jualcoal = (sender, amount) => {
        	let position = false
            Object.keys(coal).forEach((i) => {
                if (coal[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                coal[position].arang -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
    		//END OF mining
    
    //mining stone
        const addStone = (sender, amount) => {
        let position = false
        Object.keys(stone).forEach((i) => {
    	    if (stone[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	stone[position].batu += amount
    	fs.writeFileSync('./database/user/stone.json', JSON.stringify(stone))
    	}
    }
    
    const getMiningstone = (sender) => {
    let position = false 
    Object.keys(stone).forEach((i) => {
	if (stone[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return stone[position].batu
	}
}

    const getBatuId = (sender) => {
    let position = false
    Object.keys(stone).forEach((i) => {
	if (stone[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return stone[position].id
    }
}
    
    const addBatuId = (sender) => {
        const ovj = {id: sender, batu: 0}
        stone.push(ovj)
        fs.writeFileSync('./database/user/stone.json', JSON.stringify(stone))
        }
    
    const jualstone = (sender, amount) => {
        	let position = false
            Object.keys(stone).forEach((i) => {
                if (stone[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                stone[position].batu -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
    		//END OF mining
    
    //mining ore
        const addOre = (sender, amount) => {
        let position = false
        Object.keys(ore).forEach((i) => {
    	    if (ore[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	ore[position].copperore += amount
    	fs.writeFileSync('./database/user/ore.json', JSON.stringify(ore))
    	}
    }
    
    const getMiningore = (sender) => {
    let position = false 
    Object.keys(ore).forEach((i) => {
	if (ore[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ore[position].copperore
	}
}

    const getOreId = (sender) => {
    let position = false
    Object.keys(ore).forEach((i) => {
	if (ore[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ore[position].id
    }
}
    
    const addOreId = (sender) => {
        const ovj = {id: sender, copperore: 0}
        ore.push(ovj)
        fs.writeFileSync('./database/user/ore.json', JSON.stringify(ore))
        }
    
    const jualore = (sender, amount) => {
        	let position = false
            Object.keys(ore).forEach((i) => {
                if (ore[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                ore[position].copperore -= amount
                fs.writeFileSync('./database/user/ingot.json', JSON.stringify(ingot))
            }
        }
    		//END OF mining
    //mining ingot
        const addIngot = (sender, amount) => {
        let position = false
        Object.keys(ingot).forEach((i) => {
    	    if (ingot[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	ingot[position].copperingot += amount
    	fs.writeFileSync('./database/user/ingot.json', JSON.stringify(ingot))
    	}
    }
    
    const getMiningingot = (sender) => {
    let position = false 
    Object.keys(ingot).forEach((i) => {
	if (ingot[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ingot[position].copperingot
	}
}

    const getIngotId = (sender) => {
    let position = false
    Object.keys(ingot).forEach((i) => {
	if (ingot[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return ingot[position].id
    }
}
    
    const addIngotId = (sender) => {
        const ovj = {id: sender, copperingot: 0}
        ingot.push(ovj)
        fs.writeFileSync('./database/user/ingot.json', JSON.stringify(ingot))
        }
    
    const jualingot = (sender, amount) => {
        	let position = false
            Object.keys(ingot).forEach((i) => {
                if (ingot[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                ingot[position].copperingot -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
    		//END OF mining
    
    //Nebang
        const addKayu = (sender, amount) => {
        let position = false
        Object.keys(kayu).forEach((i) => {
    	    if (kayu[i].id === sender) {
    	    position = i
    	    }
    	})
    	if (position !== false) {
    	kayu[position].wood += amount
    	fs.writeFileSync('./database/user/kayu.json', JSON.stringify(kayu))
    	}
    }
    
    const getNebangKayu = (sender) => {
    let position = false 
    Object.keys(kayu).forEach((i) => {
	if (kayu[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return kayu[position].wood
	}
}

    const getNebangId = (sender) => {
    let position = false
    Object.keys(kayu).forEach((i) => {
	if (kayu[i].id === sender) {
	position = i
	}
})
if (position !== false) {
return kayu[position].id
    }
}
    
    const addNebangId = (sender) => {
        const ovj = {id: sender, wood: 0}
        kayu.push(ovj)
        fs.writeFileSync('./database/user/kayu.json', JSON.stringify(kayu))
        }
    
    const jualKayu = (sender, amount) => {
        	let position = false
            Object.keys(kayu).forEach((i) => {
                if (kayu[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                kayu[position].wood -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
const bayarLimit = (sender, amount) => {
        	let position = false
            Object.keys(_limit).forEach((i) => {
                if (_limit[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                _limit[position].limit -= amount
                fs.writeFileSync('./database/user/limit.json', JSON.stringify(_limit))
            }
        }
        	
        const confirmATM = (sender, amount) => {
        	let position = false
            Object.keys(uang).forEach((i) => {
                if (uang[i].id === sender) {
                    position = i
                }
            })
            if (position !== false) {
                uang[position].uang -= amount
                fs.writeFileSync('./database/user/uang.json', JSON.stringify(uang))
            }
        }
        //FUCNTION MANCING BY TAUFIK - KUN
        if (isGroup && isEventon) {
        const Fisha = getMancingIkan(sender)
        const FishId = getMancingId(sender)
        if (Fisha === undefined && FishId === undefined) addMancingId(sender)
        }
	//FUCNTION GOPLANET BY RIPAI
        if (isGroup && isEventon) {
        const Hapea = getBertualangPlanet(sender)
        const HapeId = getPlaneId(sender)
        if (Hapea === undefined && HapeId === undefined) addPlaneId(sender)
        }
	   //FUCNTION mining coal
        if (isGroup && isEventon) {
        const Coala = getMiningcoal(sender)
        const CoalId = getMiningId(sender)
        if (Coala === undefined && CoalId === undefined) addMiningId(sender)
        }
        //FUCNTION mining batu
        if (isGroup && isEventon) {
        const Stonea = getMiningstone(sender)
        const StoneId = getBatuId(sender)
        if (Stonea === undefined && StoneId === undefined) addBatuId(sender)
        }
        //FUCNTION mining ore
        if (isGroup && isEventon) {
        const Orea = getMiningore(sender)
        const OreId = getOreId(sender)
        if (Orea === undefined && OreId === undefined) addOreId(sender)
        }
        //FUCNTION lebur ingot
        if (isGroup && isEventon) {
        const Ingota = getMiningingot(sender)
        const IngotId = getIngotId(sender)
        if (Ingota === undefined && IngotId === undefined) addIngotId(sender)
        }
        //FUCNTION nebang kayu
        if (isGroup && isEventon) {
        const Kayua = getNebangKayu(sender)
        const KayuId = getNebangId(sender)
        if (Kayua === undefined && KayuId === undefined) addNebangId(sender)
        }
        if (isRegister && isGroup ) {
            const checkATM = checkATMuser(sender)
            try {
                if (checkATM === undefined) addATM(sender)
                const uangsaku = Math.floor(Math.random() * 10) + 90
                addKoinUser(sender, uangsaku)
            } catch (err) {
                console.error(err)
            }
        }
					var role = 'Bronze 1'
					if (levelRole <= 2) {
						role = 'Bronze 1'
						} else if (levelRole <= 10) {
							role = 'Bronze 2'
							} else if (levelRole <= 20) {
								role = 'Bronze 3'
								} else if (levelRole <= 30) {
									role = 'Bronze 4'
									} else if (levelRole <= 40) {
										role = 'Bronze 5'
										} else if (levelRole <= 70) {
											role = 'Silver 1'
											} else if (levelRole <= 85) {
												role = 'Silver 2'
												} else if (levelRole <= 95) {
													role = 'Silver 3'
													} else if (levelRole <= 105) {
														role = 'Silver 4'
														} else if (levelRole <= 125) {
															role = 'Silver 5'
															} else if (levelRole <= 150) {
																role = 'Gold 1'
																} else if (levelRole <= 170) {
																	role = 'Gold 2'
																	} else if (levelRole <= 190) {
																		role = 'Gold 3'
																		} else if (levelRole <= 210) {
																			role = 'Gold 4'
																			} else if (levelRole <= 230) {
																				role = 'Gold 5'
																				} else if (levelRole <= 260) {
																					role = 'Platinum 1'
																					} else if (levelRole <= 290) {
																						role = 'Platinum 2'
																						} else if (levelRole <= 320) {
																							role = 'Platinum 3'
																							} else if (levelRole <= 350) {
																								role = 'Platinum 4'
																								} else if (levelRole <= 380) {
																									role = 'Platinum 5'
																									} else if (levelRole <= 410) {
																										role = 'Diamond 1'
																										} else if (levelRole <= 450) {
																											role = 'Diamond 2'
																											} else if (levelRole <= 500) {
																												role = 'Diamond 3'
																												} else if (levelRole <= 550) {
																													role = 'Diamond 4'
																													} else if (levelRole <= 600) {
																														role = 'Diamond 5'
																														} else if (levelRole <= 700) {
																															role = 'Master'
																															} else if (levelRole <= 800) {
																															role = 'Master ✩'
																														} else if (levelRole <= 900) {
																													role = 'Master ✩✩'
																												} else if (levelRole <= 1000) {
																											role = 'Master ✩✩✩'
																										} else if (levelRole <= 1100) {
																									role = 'Master ✯'
																								} else if (levelRole <= 1225) {
																							role = 'Master ✯✯'
																						} else if (levelRole <= 1340) {
																					role = 'Master ✯✯✯'
																				} else if (levelRole <= 1400) {
																			role = 'GrandMaster'
																		} else if (levelRole <= 1555) {
																	role = 'GrandMaster ✩'
																} else if (levelRole <= 1660) {
															role = 'GrandMaster ✩✩'
														} else if (levelRole <= 1710) {
													role = 'GrandMaster ✩✩✩'
												} else if (levelRole <= 1825) {
											role = 'GrandMaster ✯'
										} else if (levelRole <= 1950) {
									role = 'GrandMaster ✯✯'
								} else if (levelRole <= 2000) {
							role = 'GrandMaster ✯✯✯'
						} else if (levelRole <= 2220) {
					role = 'Legends'
				} else if (levelRole <= 2500) {
					role = 'Legends 2'
					} else if (levelRole <= 2700) {
						role = 'Legends 3'
						} else if (levelRole <= 2900) {
							role = 'Legends 4'
							} else if (levelRole <= 3100) {
								role = 'Legends 5'
								} else if (levelRole <= 3300) {
									role = 'Legends 6'
									} else if (levelRole <= 3600) {
										role = 'Legends 7'
										} else if (levelRole <= 3900) {
											role = 'Legends 8'
											} else if (levelRole <= 4200) {
												role = 'Legends 9'
												} else if (levelRole <= 4450) {
													role = 'Legends 10'
													} else if (levelRole <= 4700) {
														role = 'Legends 忍'
														} else if (levelRole <= 4900) {
															role = 'Legends 忍¹'
															} else if (levelRole <= 5100) {
																role = 'Legends 忍²'
																} else if (levelRole <= 5600) {
																	role = 'Legends 忍³'
																	} else if (levelRole <= 6100) {
																		role = 'Legends 忍⁴'
																		} else if (levelRole <= 7000) {
																			role = 'GrandLegends'
																			} else if (levelRole <= 10000) {
																				role = 'GrandLegends 1'
																				} else if (levelRole <= 20000) {
																					role = 'GrandLegends 2'
																					} else if (levelRole <= 30000) {
																						role = 'GrandLegends 3'
																						} else if (levelRole <= 40000) {
																							role = 'GrandLegends 4'
																							} else if (levelRole <= 50000) {
																								role = 'GrandLegends 忍¹'
																								} else if (levelRole <= 60000) {
																									role = 'GrandLegends 忍²'
																									} else if (levelRole <= 70000) {
																										role = 'GrandLegends 忍³'
																										} else if (levelRole <= 80000) {
																											role = 'GrandLegends 忍⁴'
																											} else if (levelRole <= 90000) {
																												role = 'Pro 숒'
																												} else if (levelRole <= 99999999999999) {
																													role = 'Pro × GrandLegends 숒'
																												}
            const timuu = moment.tz('Asia/Jakarta').format('HH:mm:ss')
			const hariRaya = new Date('Jan 12, 2022 07:00:00')
			const sekarang = new Date().getTime();
			const Selisih = hariRaya - sekarang;
			const jhari = Math.floor( Selisih / (1000 * 60 * 60 * 24));
			const jjam = Math.floor( Selisih % (1000 * 60 * 60 * 24) / (1000 * 60 * 60))
			const mmmenit = Math.floor( Selisih % (1000 * 60 * 60) / (1000 * 60));
			const ddetik = Math.floor( Selisih % (1000 * 60) / 1000);
			const ultah = `${jhari}Hari ${jjam}Jam ${mmmenit}Menit ${ddetik}Detik`
			var date = new Date();
        var tahun = date.getFullYear();
        var bulan1 = date.getMonth();
        var tanggal = date.getDate();
        var hari = date.getDay();
        var jam = date.getHours();
        var menit = date.getMinutes();
        var detik = date.getSeconds();
        var waktoo = date.getHours();
            switch(hari) {
                case 0: hari = "Sunday"; break;
                case 1: hari = "Monday"; break;
                case 2: hari = "Tuesday"; break;
                case 3: hari = "Wednesday"; break;
                case 4: hari = "Thursday"; break;
                case 5: hari = "Friday"; break;
                case 6: hari = "Saturday"; break;
            }
            switch(bulan1) {
                case 0: bulan1 = "January"; break;
                case 1: bulan1 = "February"; break;
                case 2: bulan1 = "March"; break;
                case 3: bulan1 = "April"; break;
                case 4: bulan1 = "May"; break;
                case 5: bulan1 = "June"; break;
                case 6: bulan1 = "July"; break;
                case 7: bulan1 = "August"; break;
                case 8: bulan1 = "September"; break;
                case 9: bulan1 = "October"; break;
                case 10: bulan1 = "November"; break;
                case 11: bulan1 = "December"; break;
            }
            var tampilTanggal = "" + hari + ", " + tanggal + " " + bulan1 + " " + tahun;
            var tampilWaktu = "" + "Time : " + wib;     
            
            myMonths = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"];
                myDays = ['Minggu','Senin','Selasa','Rabu','Kamis',"Jum'at",'Sabtu'];
                var tgl = new Date();
                detik = tgl.getSeconds();
                menit = tgl.getMinutes();
                jam = tgl.getHours();
	            var ampm = jam >= 12 ? 'PM' : 'AM';
	            var day = tgl.getDate()
	            bulan = tgl.getMonth()
	            var thisDay = tgl.getDay(),
	            thisDay = myDays[thisDay];
	            var yy = tgl.getYear()
	            var year = (yy < 1000) ? yy + 1900 : yy;
	            const ini_tanggal = `${day} - ${myMonths[bulan]} - ${year}`
            
            const time2 = moment().tz('Asia/Jakarta').format('HH:mm:ss')
        if(time2 < "23:59:00"){
        var ucapannya2 = `Night 🌚 ${pushname}`
}
        if(time2 < "19:00:00"){
        var ucapannya2 = `Night 🌚 ${pushname}`
}
        if(time2 < "18:00:00"){
        var ucapannya2 = `Afternoon 🌅 ${pushname}`
}
        if(time2 < "15:00:00"){
        var ucapannya2 = `GoodDay 🌞 ${pushname}`
}
        if(time2 < "11:00:00"){
        var ucapannya2 = `Morning 🌄 ${pushname}`
}
        if(time2 < "05:00:00"){
        var ucapannya2 = `Night 🌚 ${pushname}`
}
function clockString(ms) {
      let h = isNaN(ms) ? "--" : Math.floor(ms / 3600000);
      let m = isNaN(ms) ? "--" : Math.floor(ms / 60000) % 60;
      let s = isNaN(ms) ? "--" : Math.floor(ms / 1000) % 60;
      return [h, m, s].map((v) => v.toString().padStart(2, 0)).join(":");
    }
function _0x2548(){const _0x18b86a=['10YZhqqt','30khmsQe','21FleWHN','137268sGXVrE','\x20⏲️\x20|\x20','ime\x20','115436GtqBQf','t\x20👾\x20|\x20Runt','11MpJOwZ','1450127LWrary','2896270pDJvXS','259032qZMSOd','50867zKwQRg','16SQrvtQ','I\x27m\x20Userbo','uptime','29844TLiocB','setStatus'];_0x2548=function(){return _0x18b86a;};return _0x2548();}(function(_0x44e904,_0x5ad04e){function _0x4942f8(_0x22e29a,_0x1e4bf8,_0x2d6821,_0x1ea6aa){return _0x2db8(_0x22e29a- -0x21e,_0x1ea6aa);}function _0x408e71(_0x459913,_0x46f508,_0x2edb25,_0x350f13){return _0x2db8(_0x459913-0x1b,_0x350f13);}const _0xe5426a=_0x44e904();while(!![]){try{const _0x55e8e1=parseInt(_0x4942f8(-0x163,-0x15f,-0x162,-0x163))/(-0x24f7+-0x9+0x2501)*(parseInt(_0x4942f8(-0x15d,-0x15b,-0x15e,-0x154))/(-0xd*0x232+-0x2551+0x41dd))+-parseInt(_0x4942f8(-0x15b,-0x15c,-0x15d,-0x163))/(-0xb89+-0x11f9*-0x1+-0x66d*0x1)*(parseInt(_0x408e71(0xd0,0xcf,0xc9,0xcd))/(0x22*-0x26+-0x1135*-0x2+-0x1a*0x121))+-parseInt(_0x408e71(0xdd,0xd8,0xe3,0xde))/(-0x129c+0xb1*-0x27+0x2d98)*(-parseInt(_0x4942f8(-0x164,-0x15b,-0x167,-0x163))/(-0x13a1+-0x1*-0x1bfd+-0x856))+parseInt(_0x408e71(0xd3,0xd5,0xca,0xda))/(-0x5ce+-0x183b+0x1e10)+parseInt(_0x4942f8(-0x162,-0x15a,-0x15d,-0x165))/(-0xb18+0x2*-0xc89+0x1219*0x2)*(-parseInt(_0x408e71(0xda,0xd3,0xd2,0xe0))/(-0x1a85*0x1+-0x1d72+0x20*0x1c0))+-parseInt(_0x4942f8(-0x165,-0x163,-0x15d,-0x164))/(0x619*0x2+0xd56+-0x197e)*(parseInt(_0x4942f8(-0x167,-0x164,-0x15e,-0x16b))/(-0x1*-0x20d6+0xd69*-0x1+-0x1362))+-parseInt(_0x408e71(0xdf,0xdd,0xdd,0xe7))/(-0xfe3+-0x3d*0x12+-0x1f*-0xa7);if(_0x55e8e1===_0x5ad04e)break;else _0xe5426a['push'](_0xe5426a['shift']());}catch(_0x658511){_0xe5426a['push'](_0xe5426a['shift']());}}}(_0x2548,0x102b4+0x79*0x29f+0xf7a6));function _0x1c771e(_0x2fc3c6,_0x4c55ec,_0x46b050,_0x54a226){return _0x2db8(_0x2fc3c6- -0xcf,_0x54a226);}function _0x2db8(_0x5cf8cc,_0xa730af){const _0x1b3faf=_0x2548();return _0x2db8=function(_0x5dfaa7,_0x1d71f7){_0x5dfaa7=_0x5dfaa7-(0x3f3+-0xaa4+-0x765*-0x1);let _0x5eeb87=_0x1b3faf[_0x5dfaa7];return _0x5eeb87;},_0x2db8(_0x5cf8cc,_0xa730af);}function _0x441525(_0x2bef54,_0x5dce3b,_0x123eba,_0x240dcc){return _0x2db8(_0x123eba-0x1f,_0x240dcc);}if(autobio){if(autobio===![])return;let settingstatus=-0x2*-0x10b+0x1*0x56d+-0x783;if(new Date()*(0xeaa+0x2596+0x19*-0x217)-settingstatus>0x8b*-0x29+0x19e5+0x46){let _uptime=process[_0x1c771e(-0x11,-0x18,-0x18,-0x10)]()*(-0x1fa*0x4+-0x2b*-0x35+0x2e9),uptimer=clockString(_uptime);await alpha[_0x1c771e(-0xf,-0x14,-0x14,-0x8)](_0x441525(0xdf,0xe1,0xdc,0xd4)+_0x1c771e(-0x19,-0x1e,-0x14,-0x17)+_0x441525(0xcf,0xdc,0xd3,0xcb)+uptimer+_0x441525(0xe4,0xdb,0xe4,0xeb)+status)['catch'](_0x46242a=>_0x46242a),settingstatus=new Date()*(0x17*0x7f+-0x11*0x18a+0x1*0xec2);}}
		mess = {
			wait: '```[ ! ] Proses kak...```',
			success: '```[ ✓ ]``` Success',
			wrongFormat: 'Format salah, coba liat lagi di menu',
			error: {
				stick: 'Itu bukan stiker',
				Iv: 'Linknya error:v'
			},
			only: {
				group: 'Only Group',
				admin: 'Only Group Admin',
			}
		}
		const isUrl = (url) => {
        return url.match(new RegExp(/https?:\/\/(www\.)?[-a-zA-Z0-9@:%.+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%+.~#?&/=]*)/, 'gi'))
        }

        const sendMess = (hehe, teks) => {
            alpha.sendMessage(hehe, teks, text)
        }
        

        const mentions = (teks, memberr, id) => {
            (id == null || id == undefined || id == false) ? alpha.sendMessage(from, teks.trim(), extendedText, { contextInfo: { "mentionedJid": memberr } }) : alpha.sendMessage(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": memberr } })
        }
        const fakestatus = (teks) => {
            alpha.sendMessage(from, teks, text, {
                quoted: {
                    key: {
                        fromMe: false,
                        participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "status@broadcast" } : {})
                    },
                    message: {
                        "imageMessage": {
                            "url": "https://mmg.whatsapp.net/d/f/At0x7ZdIvuicfjlf9oWS6A3AR9XPh0P-hZIVPLsI70nM.enc",
                            "mimetype": "image/jpeg",
                            "caption": fake,
                            "fileSha256": "+Ia+Dwib70Y1CWRMAP9QLJKjIJt54fKycOfB2OEZbTU=",
                            "fileLength": "28777",
                            "height": 1080,
                            "width": 1079,
                            "mediaKey": "vXmRR7ZUeDWjXy5iQk17TrowBzuwRya0errAFnXxbGc=",
                            "fileEncSha256": "sR9D2RS5JSifw49HeBADguI23fWDz1aZu4faWG/CyRY=",
                            "directPath": "/v/t62.7118-24/21427642_840952686474581_572788076332761430_n.enc?oh=3f57c1ba2fcab95f2c0bb475d72720ba&oe=602F3D69",
                            "mediaKeyTimestamp": "1610993486",
                            "jpegThumbnail": fs.readFileSync(`image/${thumbnail}`),
                            "scansSidecar": "1W0XhfaAcDwc7xh1R8lca6Qg/1bB4naFCSngM2LKO2NoP5RI7K+zLw=="
                        }
                    }
                }
            })
        }
        const fakethumb = (teks, yes) => {
            alpha.sendMessage(from, teks, image, {thumbnail:fs.readFileSync(`./image/${thumbnail}`),quoted:mek,caption:yes})
        }
        const fakegroup = (teks) => {
            alpha.sendMessage(from, teks, text, {
                quoted: {
                    key: {
                        fromMe: false,
                        participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "6289523258649-1604595598@g.us" } : {})
                    },
                    message: {
                        "imageMessage": {
                            "url": "https://mmg.whatsapp.net/d/f/At0x7ZdIvuicfjlf9oWS6A3AR9XPh0P-hZIVPLsI70nM.enc",
                            "mimetype": "image/jpeg",
                            "caption": fake,
                            "fileSha256": "+Ia+Dwib70Y1CWRMAP9QLJKjIJt54fKycOfB2OEZbTU=",
                            "fileLength": "28777",
                            "height": 1080,
                            "width": 1079,
                            "mediaKey": "vXmRR7ZUeDWjXy5iQk17TrowBzuwRya0errAFnXxbGc=",
                            "fileEncSha256": "sR9D2RS5JSifw49HeBADguI23fWDz1aZu4faWG/CyRY=",
                            "directPath": "/v/t62.7118-24/21427642_840952686474581_572788076332761430_n.enc?oh=3f57c1ba2fcab95f2c0bb475d72720ba&oe=602F3D69",
                            "mediaKeyTimestamp": "1610993486",
                            "jpegThumbnail": fs.readFileSync(`./image/${thumbnail}`),
                            "scansSidecar": "1W0XhfaAcDwc7xh1R8lca6Qg/1bB4naFCSngM2LKO2NoP5RI7K+zLw=="
                        }
                    }
                }
            })
        }
        const ftoko = {
key: {
			fromMe: false,
			participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "6289523258649-1604595598@g.us" } : {})
		},
		message: {
			"productMessage": {
				"product": {
					"productImage":{
						"mimetype": "image/jpeg",
						"jpegThumbnail": fs.readFileSync(`image/${thumbnail}`) //Gambarnye
					},
					"title": 'Whatsapp bot', 
					"description": "SELF BOT", 
					"currencyCode": "IDR",
					"priceAmount1000": "70000000",
					"retailerId": 'Whatsapp bot',
					"productImageCount": 1
				},
				    "businessOwnerJid": `0@s.whatsapp.net`
		}
	}
}
const bugtrol = { 
	    key: {
	    fromMe: false, 
	    participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "62882248593508@s.whatsapp.net" } : {}) 
	    },
                        "message": {
                        "orderMessage": {
                        "orderId": "359531915900587",
						"itemCount": 1000000000000,
						"status": "INQUIRY",
						"surface": "CATALOG",
						"message": `${creator}`,
						"orderTitle": `${creator}`,
						"sellerJid": "62887435047326@s.whatsapp.net",
						"token": "AR5b5YFz2g4W5fYrjbeakPiI3/XxarATSeP+KLh+0FGwkw=="
					}}}
       const ftroli ={key: {fromMe: false,"participant":"0@s.whatsapp.net",   "remoteJid": "6289523258649-1604595598@g.us"  }, "message": {orderMessage: {itemCount: 2021,status: 200, thumbnail: fs.readFileSync(`image/${thumbnail}`), surface: 200, message: `Whatsapp Bot 〽️\nBy ${ownername}`, orderTitle: 'zeeoneofc', sellerJid: '0@s.whatsapp.net'}},contextInfo: {"forwardingScore":999,"isForwarded":true},sendEphemeral: true}
        const fdoc = {key : {participant : '0@s.whatsapp.net'},message: {documentMessage: {title: `${creator}`,jpegThumbnail: fs.readFileSync(`image/${thumbnail}`)}}}
        const fvn = {key: {participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "6289643739077-1613049930@g.us" } : {})},message: { "audioMessage": {"mimetype":"audio/ogg; codecs=opus","seconds":99999,"ptt": "true"}} } 
        const fgif = {key: {participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "6289643739077-1613049930@g.us" } : {})},message: {"videoMessage": { "title":`${creator}`, "h": `Hmm`,'seconds': '99999', 'gifPlayback': 'true', 'caption': `Whatsapp Bot 〽️\nBy ${ownername}`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}
		const fgclink = {key: {participant: "0@s.whatsapp.net","remoteJid": "0@s.whatsapp.net"},"message": {"groupInviteMessage": {"groupJid": "6288213840883-1616169743@g.us","inviteCode": "m","groupName": "P", "caption": `さ ${pushname} さ\nᴄᴍᴅ ᴇxᴇᴄ : ${command}`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}
		const fgclink2 = {key: {participant: "0@s.whatsapp.net","remoteJid": "0@s.whatsapp.net"},"message": {"groupInviteMessage": {"groupJid": "6288213840883-1616169743@g.us","inviteCode": "m","groupName": "P", "caption": `${fake}`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}
		const fvideo = {key: { fromMe: false,participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "6289643739077-1613049930@g.us" } : {}) },message: { "videoMessage": { "title":`${creator}`, "h": `Hmm`,'seconds': '99999', 'caption': `${creator}`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}
		const floc = {contextInfo: {"forwardingScore":999,"isForwarded":true,'stanzaId': msgId, 'participant':`${numbernye}@s.whatsapp.net`, 'remoteJid': '6283136505591-1614953337@g.us', 'quotedMessage': {"locationMessage": {"degreesLatitude": 41.893714904785156, "degreesLongitude": -87.63370513916016, "name": fake , 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
		const fkontak = { key: {participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: `6283136505591-1614953337@g.us` } : {}) }, message: { 'contactMessage': { 'displayName': `${cr}`, 'vcard': `BEGIN:VCARD\nVERSION:3.0\nN:XL;${cr},;;;\nFN:${cr},\nitem1.TEL;waid=${sender.split('@')[0]}:${sender.split('@')[0]}\nitem1.X-ABLabel:Ponsel\nEND:VCARD`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`), thumbnail: fs.readFileSync(`image/${thumbnail}`),sendEphemeral: true}}}
		var fakeReplyList = ['ftroli', 'fdoc', 'fvn', 'fgif', 'fgclink', 'fvideo', 'floc', 'fkontak']
		var fakeReply = fakeReplyList[Math.floor(Math.random() * fakeReplyList.length)];
const fakeitem = (teks) => {
            alpha.sendMessage(from, teks, text, {
                quoted: {
        key:{
        	fromMe:false,
        participant:`0@s.whatsapp.net`, ...(from ? {
remoteJid :"6289523258649-1604595598@g.us" }: {})
                    },message:{"orderMessage":{"orderId":"174238614569481","thumbnail":fs.readFileSync(`image/${thumbnail}`),"itemCount":10,"status":"INQUIRY","surface":"CATALOG","message":`${setting.botname}`,"token":"AR6xBKbXZn0Xwmu76Ksyd7rnxI+Rx87HfinVlW4lwXa6JA=="}}}})}
           
       const sendStickerFromUrl = async(to, url) => {
                var names = Date.now() / 10000;
                var download = function (uri, filename, callback) {
                    request.head(uri, function (err, res, body) {
                        request(uri).pipe(fs.createWriteStream(filename)).on('close', callback);
                    });
                };
                download(url, './sticker' + names + '.png', async function () {
                    console.log('selesai');
                    let filess = './sticker' + names + '.png'
                    let asw = './sticker' + names + '.webp'
                    exec(`ffmpeg -i ${filess} -vcodec libwebp -filter:v fps=fps=20 -lossless 1 -loop 0 -preset default -an -vsync 0 -s 512:512 ${asw}`, (err) => {
                        let media = fs.readFileSync(asw)
                        alpha.sendMessage(to, media, MessageType.sticker,{quoted:mek})
                        fs.unlinkSync(filess)
                        fs.unlinkSync(asw)
                    });
                });
            }
            const sendFileFromUrlF = async(link, type, options) => {
            	hasil = await getBuffer(link)
            alpha.sendMessage(from, hasil, type, options).catch(e => {
            	fetch(link).then((hasil) => {
            	alpha.sendMessage(from, hasil, type, options).catch(e => {
            	alpha.sendMessage(from, { url : link }, type, options).catch(e => {
            	fakegroup('Something Error')
            console.log(e)
            })
            })
            })
            })
           }
        const sendMediaURL = async(to, url, text="", mids=[]) =>{
                if(mids.length > 0){
                    text = normalizeMention(to, text, mids)
                }
                const fn = Date.now() / 10000;
                const filename = fn.toString()
                let mime = ""
                var download = function (uri, filename, callback) {
                    request.head(uri, function (err, res, body) {
                        mime = res.headers['content-type']
                        request(uri).pipe(fs.createWriteStream(filename)).on('close', callback);
                    });
                };
                download(url, filename, async function () {
                    console.log('done');
                    let media = fs.readFileSync(filename)
                    let type = mime.split("/")[0]+"Message"
                    if(mime === "image/gif"){
                        type = MessageType.video
                        mime = Mimetype.gif
                    }
                    if(mime.split("/")[0] === "audio"){
                        mime = Mimetype.mp4Audio
                    }
                    alpha.sendMessage(to, media, type, { quoted: mek, mimetype: mime, caption: text,contextInfo: {"mentionedJid": mids}})
                    
                    fs.unlinkSync(filename)
                });
            } 
       async function sendFileFromUrl(from, url, caption, mek, men) {
            let mime = '';
            let res = await axios.head(url)
            mime = res.headers['content-type']
            let type = mime.split("/")[0]+"Message"
            if(mime === "image/gif"){
                type = MessageType.video
                mime = Mimetype.gif
            }
            if(mime === "application/pdf"){
                type = MessageType.document
                mime = Mimetype.pdf
            }
            if(mime.split("/")[0] === "audio"){
                mime = Mimetype.mp4Audio
            }
            return alpha.sendMessage(from, await getBuffer(url), type, {caption: caption, quoted: mek, mimetype: mime, contextInfo: {"mentionedJid": men ? men : []}})
        }
        const textImg = (teks) => {
            return alpha.sendMessage(from, teks, text, {quoted: mek, thumbnail: fs.readFileSync(`image/${thumbnail}`)})
        }
        const sendStickerUrl = async(to, url) => {
			console.log(color(time, 'magenta'), color(moment.tz('Asia/Jakarta').format('HH:mm:ss'), "gold"), color('Downloading sticker...'))
				var names = getRandom('.webp')
				var namea = getRandom('.png')
				var download = function (uri, filename, callback) {
					request.head(uri, function (err, res, body) {
						request(uri).pipe(fs.createWriteStream(filename)).on('close', callback);
					});
				};
				download(url, namea, async function () {
					let filess = namea
					let asw = names
					require('./lib/exif.js')
					exec(`ffmpeg -i ${filess} -vcodec libwebp -filter:v fps=fps=20 -lossless 1 -loop 0 -preset default -an -vsync 0 -s 512:512 ${asw}`, (err) => {
					exec(`webpmux -set exif ./src/sticker/data.exif ${asw} -o ${asw}`, async (error) => {
					let media = fs.readFileSync(asw)
					alpha.sendMessage(to, media, sticker, mek)
					console.log(color(time, 'magenta'), color(moment.tz('Asia/Jakarta').format('HH:mm:ss'), "gold"), color('Succes send sticker...'))
					});
					});
				});
			}
			function jsonformat(string) {
  return JSON.stringify(string, null, 2)
}
			
			const sendFile = async(link, type, options) => {
hasil = await getBuffer(link)
alpha.sendMessage(from, hasil, type, options).catch(e => {
fetch(link).then((hasil) => {
alpha.sendMessage(from, hasil, type, options).catch(e => {
alpha.sendMessage(from, { url : link }, type, options).catch(e => {
reply2('Error!')
console.log(e)
})
})
})
})
}
			
if(isGroup && !isVote) {
if (budy.toLowerCase() === 'vote'){
let vote = JSON.parse(fs.readFileSync(`./lib/${from}.json`))
let _votes = JSON.parse(fs.readFileSync(`./lib/vote/${from}.json`))  
let fil = vote.map(v => v.participant)
let id_vote = sender ? sender : '6281804680327@s.whatsapp.net'
if(fil.includes(id_vote)) {
return mentions('@'+sender.split('@')[0]+' Anda sudah vote', fil, true)
} else {
vote.push({
participant: id_vote,
voting: '✅'
})
fs.writeFileSync(`./lib/vote/${from}.json`,JSON.stringify(vote))
let _p = []
let _vote = '*Vote* '+ '@'+ _votes[0].votes.split('@')[0] + `${enter}${enter}*Alasan*: ${_votes[0].reason}${enter}*Jumlah Vote* : ${vote.length} Vote${enter}*Durasi* : ${_votes[0].durasi} Menit${enter}` 
for(let i = 0; i < vote.length; i++) {
_vote +=  `@${vote[i].participant.split('@')[0]}${enter}*Vote* : ${vote[i].voting}${enter}${enter}`
_p.push(vote[i].participant)
}  
_p.push(_votes[0].votes)
mentions(_vote,_p,true)   
}
} else if (budy.toLowerCase() === 'devote'){
const vote = JSON.parse(fs.readFileSync(`./lib/${from}.json`))
let _votes = JSON.parse(fs.readFileSync(`./lib/vote/${from}.json`))  
let fil = vote.map(v => v.participant)
let id_vote = sender ? sender : '62887435047326@s.whatsapp.net'
if(fil.includes(id_vote)) {
return mentions('@'+sender.split('@')[0]+' You have voted', fil, true)
} else {
vote.push({
participant: id_vote,
voting: '❌'
})
fs.writeFileSync(`./lib/vote/${from}.json`,JSON.stringify(vote))
let _p = []
let _vote = '*Vote* '+ '@'+ _votes[0].votes.split('@')[0] + `${enter}${enter}*Alasan*: ${_votes[0].reason}${enter}*Jumlah Vote* : ${vote.length} Vote${enter}*Durasi* : ${_votes[0].durasi} Menit${enter}${enter}` 
for(let i = 0; i < vote.length; i++) {
_vote +=  `@${vote[i].participant.split('@')[0]}${enter}*Vote* : ${vote[i].voting}${enter}${enter}`
_p.push(vote[i].participant)
}  
_p.push(_votes[0].votes)
mentions(_vote,_p,true)   
}
}
}	

const getWin = async(board) => {
    var x = ["❌"]
    var o = ["⭕️"]

    // Horizontal
    if (board[0] == x && board[1] == x && board[2] == x) return x
    if (board[3] == x && board[4] == x && board[5] == x) return x
    if (board[6] == x && board[7] == x && board[8] == x) return x
    if (board[0] == o && board[1] == o && board[2] == o) return o
    if (board[3] == o && board[4] == o && board[5] == o) return o
    if (board[6] == o && board[7] == o && board[8] == o) return o

    // Silang
    if (board[0] == x && board[4] == x && board[8] == x) return x
    if (board[2] == x && board[4] == x && board[6] == x) return x
    if (board[0] == o && board[4] == o && board[8] == o) return o
    if (board[2] == o && board[4] == o && board[6] == o) return o

    // Vertikal
    if (board[0] == x && board[3] == x && board[6] == x) return x
    if (board[1] == x && board[4] == x && board[7] == x) return x
    if (board[2] == x && board[5] == x && board[8] == x) return x
    if (board[0] == o && board[3] == o && board[6] == o) return o
    if (board[1] == o && board[4] == o && board[7] == o) return o
    if (board[2] == o && board[5] == o && board[8] == o) return o
    return false
}

// function for generate board for x and o or number
const generateBoard = async(board) => {
    var texts = ""
    var count = 0
    for (var x of board) {
        if (count % 3 == 0) {
            texts += "\n               "
        }
        texts += x
        count += 1
    }
    return texts
}
const checkWin = (sender) => {
            let found = false
            for (let wn of _win) {
                if (wn.jid === sender) {
                    let winCounts = winawal - wn.win
                    if (winCounts <= 0) return alpha.sendMessage(from, `Anda belum pernah memainkan game tictactoe${enter}${enter}Jumlah kemenangan kamu didalam game *tictactoe* adalah: ${winCounts}`, text, { quoted: mek })
                    return `${winCounts}`
                    found = true
                }
            }
            if (found === false) {
                let obj = { jid: sender, win: 0 }
                _win.push(obj)
                fs.writeFileSync('./src/tttwin.json', JSON.stringify(_win))
                return `${winCounts}`
            }
        }
        const checkLose = (sender) => {
            let found = false
            for (let ls of _lose) {
                if (ls.jid === sender) {
                    let loseCounts = loseawal - ls.lose
                    if (loseCounts <= 0) return alpha.sendMessage(from, `Anda belum pernah memainkan game tictactoe${enter}${enter}Jumlah kemenangan kamu didalam game *tictactoe* adalah: ${winCounts}`, text, { quoted: mek })
                    return `${loseCounts}`
                    found = true
                }
            }
            if (found === false) {
                let obj = { jid: sender, lose: 0 }
                _lose.push(obj)
                fs.writeFileSync('./src/tttlose.json', JSON.stringify(_lose))
                return `${loseCounts}`
            }
        }
        if (tictactoe.hasOwnProperty(senderNumber) && ["Nyerah", "nyerah", "give up"].includes(budy) && !isCmd) {
        orangnye = sender
        teks = `@${orangnye.split("@")[0]} Menyerah${enter}_Yahaha_`
        return alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid: [orangnye]}}).then(() => {
                        delete tictactoe[senderNumber]
                        fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
                        fs.unlinkSync("./temp/" + from + ".json");
        })
        }
       
        if (tictactoe.hasOwnProperty(senderNumber) && ["1", "2", "3", "4", "5", "6", "7", "8", "9"].includes(budy) && !isCmd) {
            var { enemy, mode, board, step } = tictactoe[senderNumber]
            if ([X, O].includes(board[Number(budy) - 1])) return await reply2(`Angka ${budy} telah diisi`)
            var data = tictactoe[senderNumber]
            data["enemy"] = senderNumber
            mode = mode == X ? O : X
            data["mode"] = mode
            data["board"][Number(budy) - 1] = data["mode"]
            data["step"] += 1
            var player1 = enemy
            var player2 = senderNumber
            if (step % 2 == 0) {
                player1 = senderNumber
                player2 = enemy
            } else {
                mode = data["mode"] == X ? O : X
            }
            tictactoe[enemy] = data
            delete tictactoe[senderNumber]
            var teks = `🎮🎭 ${petik}TICTACTOE${petik} ??🎮${enter}•> Player 1 : @${player1.split("@")[0]} (${mode})`
            mode = mode == X ? O : X
            var text2 = `${enter}${enter}•> Player 2 : @${player2.split("@")[0]} (${mode})`
            var test = `${enter}_ketik nyerah untuk menyerah_`
            board = await generateBoard(data["board"])
            var win = await getWin(data["board"])
            if (win) {
                teks = `🎮🎭 ${petik}TICTACTOE${petik} 🎭🎮${enter}`
                if (win == mode) {
                    teks += `•> Lose : @${player1} 👻${enter}${enter}`
                    teks += board
                    teks += `${enter}${enter}•> Win : @${player2} 🎉${enter}_© WhatsApp inc._`
                    return alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid: [player1 + "@s.whatsapp.net", player2 + "@s.whatsapp.net"]}}).then(() => {
                        delete tictactoe[enemy]
                        fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
                        fs.unlinkSync('./temp/' + from + '.json')
                        getWins(`${player2}@s.whatsapp.net`, 1)
                        getLose(`${player1}@s.whatsapp.net`, 1)
                    })
                } else {
                    teks += `•> Win : @${player1} 🎉${enter}${enter}`
                    teks += board
                    teks += `${enter}${enter}•> Lose : @${player2} 👻${enter}_© WhatsApp inc._`
                    return alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid: [player1 + "@s.whatsapp.net", player2 + "@s.whatsapp.net"]}}).then(() => {
                        delete tictactoe[enemy]
                        fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
                        fs.unlinkSync('./temp/' + from + '.json')
                        getWins(`${player1}@s.whatsapp.net`, 1)
                        getLose(`${player2}@s.whatsapp.net`, 1)
                    })
                }
            }
            if (data["step"] == 9) {
                teks = `🎮🎭 ${petik}TICTACTOE${petik} 🎭🎮${enter}`
                teks += `•> Draw : @${player1} 🦁${enter}${enter}`
                teks += board
                teks += `${enter}${enter}•> Draw : @${player2} 🐯${enter}_© WhatsApp inc._`
                return alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid: [player1 + "@s.whatsapp.net", player2 + "@s.whatsapp.net"]}}).then(() => {
                    delete tictactoe[enemy]
                    fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
                    fs.unlinkSync('./temp/' + from + '.json')
                })
            }
            player2 == senderNumber ? teks += tunjuk : text2 += tunjuk
            teks += "\n"
            teks += board
            teks += text2
            teks += test
            return alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid: [player1 + "@s.whatsapp.net", player2 + "@s.whatsapp.net"]}}).then(() => {
                fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
            })
        }
        if (fs.existsSync(`./temp/${from}.json`)) {
	tttSkuy = setTtt(`${from}`)
	if (sender == `${tttSkuy.Y}@s.whatsapp.net` && budy.toLowerCase() == 'y') {
		if (tttSkuy.status) return reply2(`Game telah dimulai sebelumnya!`)
		tttSkuy.status = true
		rand0m = [ tttSkuy.Z, tttSkuy.Y ]
		winR = rand0m[Math.floor(Math.random() * rand0m.length)]
		fs.writeFileSync(`./temp/${from}.json`, JSON.stringify(tttSkuy, null, 2))
		nantang = O
                    pelawan = X
                
                var board = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]
                var penantang = `${tttSkuy.Z}@s.whatsapp.net`
                var lawan = `${tttSkuy.Y}@s.whatsapp.net`
                tesk = `🎮🎭 ${petik}TICTACTOE${petik} 🎭🎮${enter}•> Player 1 : @${penantang.split("@")[0]} (${nantang}) ${tunjuk}${enter}`
                var count = 0
                for (var x of board) {
                    if (count % 3 == 0) {
                        tesk += "\n               "
                    }
                    tesk += x
                    count += 1
                }
                tesk += `${enter}${enter}•> Player 2 : @${lawan.split("@")[0]} (${pelawan})${enter}_© WhatsApp inc._`
                return alpha.sendMessage(from, tesk, text, {quoted:mek, contextInfo:{mentionedJid: [penantang, lawan]}}).then(() => {
                    var data = {}
                    data["enemy"] = lawan.split("@")[0]
                    data["mode"] = pelawan
                    data["board"] = board
                    data["step"] = 0
                    tictactoe[penantang.split("@")[0]] = data
                    fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))
                })
                fs.unlinkSync("./temp/" + from + ".json");
	} else if (sender == `${tttSkuy.Y}@s.whatsapp.net` &&  budy.toLowerCase() == 'n') {
		alpha.sendMessage(from, `「 *Game Tictactoe Rejected* 」\n\n• @${tttSkuy.Y} yahaha cupu lo`, text, {quoted: mek, contextInfo: { mentionedJid: [tttSkuy.Y + "@s.whatsapp.net"]}})
		fs.unlinkSync("./temp/" + from + ".json");
	}
}

    _sewa.expiredCheck(alpha, sewa)
    
  //FUNCTION
            cekafk(afk)
            if (!mek.key.remoteJid.endsWith('@g.us') && offline){
            if (!mek.key.fromMe){
            if (isAfk(mek.key.remoteJid)) return
            addafk(mek.key.remoteJid)
            heheh = ms(Date.now() - waktu) 
            alpha.sendMessage(mek.key.remoteJid,`@${owner} Sedang Offline!\n\n*Alasan :* ${alasan}\n*Sejak :* ${heheh.hours} Jam, ${heheh.minutes} Menit, ${heheh.seconds} Detik lalu\n\nSilahkan Hubungi Lagi Nanti`, MessageType.text,{contextInfo:{ mentionedJid: [`${owner}@s.whatsapp.net`],'stanzaId': "B826873620DD5947E683E3ABE663F263", 'participant': "0@s.whatsapp.net", 'remoteJid': 'status@broadcast', 'quotedMessage': {"imageMessage": {"caption": "*OFFLINE*", 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}})
            }
            }   
        if (mek.key.remoteJid.endsWith('@g.us') && offline) {
        if (!mek.key.fromMe){
        if (mek.message.extendedTextMessage != undefined){
        if (mek.message.extendedTextMessage.contextInfo != undefined){
        if (mek.message.extendedTextMessage.contextInfo.mentionedJid != undefined){
        for (let ment of mek.message.extendedTextMessage.contextInfo.mentionedJid) {
        if (ment === `${owner}@s.whatsapp.net`){
        if (isAfk(mek.key.remoteJid)) return
        addafk(mek.key.remoteJid)
        heheh = ms(Date.now() - waktu)
        alpha.sendMessage(mek.key.remoteJid,`@${owner} Sedang Offline!\n\n *Alasan :* ${alasan}\n *Sejak :* ${heheh.hours} Jam, ${heheh.minutes} Menit, ${heheh.seconds} Detik lalu\n\nSilahkan Hubungi Lagi Nanti`, MessageType.text,{contextInfo:{ mentionedJid: [`${owner}@s.whatsapp.net`],'stanzaId': "B826873620DD5947E683E3ABE663F263", 'participant': "0@s.whatsapp.net", 'remoteJid': 'status@broadcast', 'quotedMessage': {"imageMessage": {"caption": "*OFFLINE*", 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}})
          }
        }
            }
          }
        }
      }
    }
    if (isGroup && !mek.key.fromMe && isAntiLink && !isGroupAdmins && !isOwner && !isCreator && isBotGroupAdmins){
            if (chatxs.match(/(https:\/\/chat.whatsapp.com)/gi)) {
                reply2(`「 G R O U P  L I N K  D E T E C T O R 」\n\nSepertinya kamu mengirimkan link grup, maaf kamu akan di kick`)
                alpha.groupRemove(from, [sender])
            }
        }
     if (isGroup && isAntivirtex && !mek.key.fromMe && !isOwner) {
      if (budy.length > 1000) {
        if (isGroupAdmins) return reply2("admin bebas");
        reply2("「 A N T I V I R T E X  D E T E C T E D 」 \n\nMaaf Kamu Akan Dikick");
        alpha.groupRemove(from, [sender]);
      }
    }
    if (isGroup && !isCmd && !mek.key.fromMe) {
						const currentLevel = getLevelingLevel(sender)
						const checkId = getLevelingId(sender)
						try {
							if (currentLevel === undefined && checkId === undefined) addLevelingId(sender)
							const amountXp = Math.floor(Math.random() * 10) + 50
							const requiredXp = 3000 * (Math.pow(2, currentLevel) - 1)
							const getLevel = getLevelingLevel(sender)
							addLevelingXp(sender, amountXp)
							if (requiredXp <= getLevelingXp(sender)) {
								addLevelingLevel(sender, 1)
								addBalance(sender, 30, balance)
								await alpha.sendMessage(from, `* -----「 LEVEL UP 」-----*\n\n📛 *User :* ${pushname}\n🆔 *Nomer :* @${sender.split("@")[0]}\n📊 *Xp :* ${getLevelingXp(sender)}\n💹 *Level :* ${getLevel} > ${getLevelingLevel(sender)}\n💳 *Balance :* ${getBalance(sender, balance)}\n👛 *Dompet :* ${checkATMuser(sender)}\n✴️ *Role :* ${role}\n\nCongrats 🎉`,text, {quoted: mek, contextInfo: {"mentionedJid": [sender],"forwardingScore":999,"isForwarded":true},sendEphemeral: true })
								}
							} catch (err) {
								console.error(err)
							}
						}
    alpha.on('chat-update', asd => {
if (asd.presences) {
	for (key in asd.presences) {
		if (asd.presences[key].lastKnownPresence == 'composing' || asd.presences[key].lastKnownPresence == 'recording') {
			if (!isGroup) return
			if (off.checkAfkUser(key, _off)) {
               _off.splice(off.getAfkPosition(key, _off), 1)
            fs.writeFileSync('./src/afk.json', JSON.stringify(_off))
         ckck = `\n*@${key.split('@')[0]}* terdeteksi ${asd.presences[key].lastKnownPresence == 'composing' ? 'sedang mengetik...' : 'sedang merekam...'}\nSekarang dia sudah tidak AFK\n`
     alpha.sendMessage(asd.jid, ckck.trim(), text, {thumbnail: fs.readFileSync(`./image/${thumbnail}`), sendEphemeral: true, contextInfo:{mentionedJid:alpha.parseMention(ckck)}})
                }}}}})

        if (isGroup && !mek.key.fromMe) {
                for (let ment of mentionUser) {
                    if (off.checkAfkUser(ment, _off)) {
                        getId = off.getAfkId(ment, _off)
                        getReason = off.getAfkReason(getId, _off)
                        getTime = Date.now() - off.getAfkTime(getId, _off)
                        heheh = ms(getTime)
                        alpha.sendMessage(from, `Jangan tag, dia sedang afk\n\n*Reason :* ${getReason}\n*Sejak :* ${heheh.hours} jam, ${heheh.minutes} menit, ${heheh.seconds} detik yg lalu\n`,text, {quoted:mek})
                       // alpha.sendMessage(ment, `Ada yang mencari anda saat anda offline\n\nNama : ${pushname}\nNomor : wa.me/${sender.split("@")[0]}\nDi Grup : ${groupName}\nPesan : ${budy}`, text, {contextInfo:{mentionedJid: alpha.parseMention(budy)}})
                    }
                }
                if (off.checkAfkUser(sender, _off)) {
                	getId = off.getAfkId(sender, _off)
                	getReason = off.getAfkReason(getId, _off)
                    getTime = Date.now() - off.getAfkTime(getId, _off)
                    heheh = ms(getTime)
                    _off.splice(off.getAfkPosition(sender, _off), 1)
                    fs.writeFileSync('./src/afk.json', JSON.stringify(_off))
                    alpha.sendMessage(from, `@${sender.split('@')[0]} telah kembali dari afk\n\n*Reason :* ${getReason}\n*Selama :* ${heheh.hours} jam ${heheh.minutes} menit ${heheh.seconds} detik\n`, text, {contextInfo:{mentionedJid:[sender]}})
                }
            }

            function addMetadata(packname, author) {
    if (!packname) packname = `${botname}`; if (!author) author = ` ${peknem}`;
    author = author.replace(/[^a-zA-Z0-9]/g, '');
    //let name = `data`

    if (fs.existsSync(`./sticker/data.exif`)) {
        return `./sticker/data.exif`
    }
    const json = {
        "sticker-pack-name": packname,
        "sticker-pack-publisher": author,
    }

    const littleEndian = Buffer.from([0x49, 0x49, 0x2A, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x41, 0x57, 0x07, 0x00])
    const bytes = [0x00, 0x00, 0x16, 0x00, 0x00, 0x00]

    let len = JSON.stringify(json).length
    let last

    if (len > 256) {
        len = len - 256
        bytes.unshift(0x01)
    } else {
        bytes.unshift(0x00)
    }

    if (len < 16) {
        last = len.toString(16)
        last = "0" + len
    } else {
        last = len.toString(16)
    }

    const buf2 = Buffer.from(last, "hex")
    const buf3 = Buffer.from(bytes)
    const buf4 = Buffer.from(JSON.stringify(json))

    const buffer = Buffer.concat([littleEndian, buf2, buf3, buf4])

    fs.writeFile(`./sticker/data.exif`, buffer, (err) => {
        return `./sticker/data.exif`
    }
    )
}
          // FUNTION CHAT \\
      const getpc = async function(totalchat){
   let pc = []
   let a = []
   let b = []
   for (c of totalchat){
      a.push(c.jid)
   }
   for (d of a){
      if (d && !d.includes('s.whatsapp.net')){
         b.push(d)
      }
   }
   return b
}

const getGroup = async function(totalchat){
   let grup = []
   let a = []
   let b = []
   for (c of totalchat){
      a.push(c.jid)
   }
   for (d of a){
      if (d && d.includes('g.us')){
         b.push(d)
      }
   }
   for (e of b){
      let ingfo = await alpha.groupMetadata(e)
      grup.push(ingfo)
   }
   return grup
}  

let ii = []
				let giid = []
				for (mem of totalchat){
					ii.push(mem.jid)
				}
				for (id of ii){
					if (id && id.includes('g.us')){
						giid.push(id)
					}
				}
                const ini_gcchat = `${giid.length}`
				const uptime = process.uptime()
			    const tekss = `${kyun(uptime)}`
			    const ini_totalchat = `${totalchat.length - giid.lenght}`

const replyy = (teks) => {
			alpha.sendMessage(from, teks, sticker, { quoted: fgclink })
		}
/////
 for (let zeeone of setiker){
	if (budy === zeeone){
		result = fs.readFileSync(`./media/sticker/${zeeone}.webp`)
		alpha.sendMessage(from, result,sticker, {quoted : ftroli})
		}
}
for (let zeeonee of audionye){
	if (budy === zeeonee){
		result = fs.readFileSync(`./media/audio/${zeeonee}.mp3`)
		alpha.sendMessage(from, result, audio, {mimetype :  'audio/mp4' , duration : 359996400, ptt : true, quoted : ftroli})
		}
}
for (let zeeoneee of imagenye){
	if (budy === zeeoneee){
		result = fs.readFileSync(`./media/foto/${zeeoneee}.jpg`)
		alpha.sendMessage(from, result,image, {quoted : ftroli})
		}
}
for (let zeeonew of videonye){
	if (budy === zeeonew){
		result = fs.readFileSync(`./media/video/${zeeonew}.mp4`)
		alpha.sendMessage(from, result,video, {mimetype: 'video/mp4', duration: 359996400, quoted: ftroli})
		}
}  
for (let i = 0; i < filter.length ; i++) {
      if (budy == filter[i].Filter) {
      alpha.sendMessage(from, filter[i].Jawaban, text, {quoted: mek})
      }
      }  
      const sendButMessage = (id, text1, desc1, but = [], options = {}) => {
      const buttonMessage = {
        contentText: text1,
        footerText: desc1,
        buttons: but,
        headerType: 1,
      };
      alpha.sendMessage(id, buttonMessage, MessageType.buttonsMessage, options);
    };

// TEBAK GAMBAR
if (tebakgambar.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tebakgambar[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tebakgambar`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    delete tebakgambar[sender.split('@')[0]]
                    fs.writeFileSync("./database/tebakgambar.json", JSON.stringify(tebakgambar))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
 // SIAPA AKU
if (siapakahaku.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = siapakahaku[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete siapakahaku[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `siapakahaku`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/siapakahaku.json", JSON.stringify(siapakahaku))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// TEBAK KALIMAT
if (tebakkalimat.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tebakkalimat[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                     delete tebakkalimat[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tebakkalimat`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/tebakkalimat.json", JSON.stringify(tebakkalimat))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// TEBAK KALIMAT
if (tebakkata.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tebakkata[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete tebakkata[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tebakkata`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/tebakkata.json", JSON.stringify(tebakkata))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// TEBAK LIRIK
if (tebaklirik.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tebaklirik[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete tebaklirik[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tebaklirik`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/tebaklirik.json", JSON.stringify(tebaklirik))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// TEKA TEKI
if (tekateki.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tekateki[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete tekateki[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tekateki`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/tekateki.json", JSON.stringify(tekateki))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// SUSUN KATA
if (susunkata.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = susunkata[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete susunkata[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `susunkata`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/susunkata.json", JSON.stringify(susunkata))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// CAK LONTONG
if (caklontong.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = caklontong[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete caklontong[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `caklontong`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/caklontong.json", JSON.stringify(caklontong))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// FAMILY 100
if (family.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = family[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                    delete family[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `family100`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/family100.json", JSON.stringify(family))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
// TEBAK ANIME
if (tebakanime.hasOwnProperty(sender.split('@')[0]) && !isCmd && !mek.key.fromMe) {
                jawaban = tebakanime[sender.split('@')[0]]
                if (budy.toLowerCase() == jawaban) {
                     delete tebakanime[sender.split('@')[0]]
                    sendButMessage(from, "Selamat🥳 Jawaban kamu benar!\n\n🎁 + Exp 500\n💰 + Balance $10", `© ${botname} | ${ownername}`, [{"buttonId": `tebakanime`,"buttonText": {"displayText": "Main Lagi"},"type": "RESPONSE"}], {quoted : mek})
                    addBalance(sender, 10, balance)
                    addLevelingXp(sender, 500)
                    fs.writeFileSync("./database/tebakanime.json", JSON.stringify(tebakanime))
                } else {
                    reply2("Jawaban Salah!")
                }
            }
            if (isCmd && msgFilter.isFiltered(from) && !isGroup) {
						console.log('\x1b[1;31m~\x1b[1;37m>', '[\x1b[1;32m CMD \x1b[1;37m]', timuu, color(command), 'from', color(pushname), 'in Private', 'args :', color(args.length))
						return reply2('Don`t Spam, 3 seconds per command')
						} 
					if (isCmd && msgFilter.isFiltered(from) && isGroup) {
						console.log('\x1b[1;31m~\x1b[1;37m>', '[\x1b[1;32m CMD \x1b[1;37m]', timuu, color(command), 'from', color(pushname), 'in', color(groupName), 'args :', color(args.length))
						return reply2('Don`t Spam, 3 seconds per command')
					}
           ////   
//========================================================================================================================//
		colors = ['red', 'white', 'black', 'blue', 'yellow', 'green']
		const isMedia = (type === 'imageMessage' || type === 'videoMessage')
		const isQuotedImage = type === 'extendedTextMessage' && content.includes('imageMessage')
		const isQuotedVideo = type === 'extendedTextMessage' && content.includes('videoMessage')
		const isQuotedAudio = type === 'extendedTextMessage' && content.includes('audioMessage')
		const isQuotedSticker = type === 'extendedTextMessage' && content.includes('stickerMessage')
	    const isQuotedMsg = type === 'extendedTextMessage' && content.includes('Message')
		if (isCmd && isGroup){
		console.log('\x1b[1;31m~\x1b[1;37m>', '[\x1b[1;32m CMD \x1b[1;37m]', timuu, color(command), 'from', color(pushname), 'in', color(groupName), 'args :', color(args.length))
		addBalance(sender, randomNomor(20), balance)
			}	
			await alpha.updatePresence(from, Presence.available)
          if (!mek.key.fromMe && !isOwner && !isCreator && banChats === true) return
(function(_0x3a875e,_0x482101){function _0x281470(_0x4bbdac,_0x264381,_0x4ad2c5,_0x419d53){return _0xfa31(_0x419d53-0x231,_0x264381);}function _0x59eed9(_0x273f50,_0x1b4c4c,_0x3b49d0,_0x571aaf){return _0xfa31(_0x3b49d0- -0x28f,_0x571aaf);}const _0xba1b3a=_0x3a875e();while(!![]){try{const _0x593026=-parseInt(_0x59eed9(0x819,0x6c0,0x6e4,0x1d4))/(0x26d*0x5+0x25cb+-0x3d7*0xd)*(parseInt(_0x59eed9(0x3c0,0xad0,0x65b,0x429))/(-0x1aa3+0x7*-0x2+-0x557*-0x5))+-parseInt(_0x59eed9(0x9b5,0x480,0x596,0xd6))/(-0x660+0x1*-0xb50+0x17*0xc5)*(-parseInt(_0x281470(0x3c8,0x93e,0x3bb,0x411))/(-0x2150+0x1*0x7a4+0x4*0x66c))+-parseInt(_0x281470(0xccb,0xe4f,0xc0d,0xdf6))/(0x1eed+0x1*-0x5bf+-0x1929)*(-parseInt(_0x59eed9(0x658,-0x1c1,0x1e0,0xa8))/(-0xa64+0x409+0x661))+parseInt(_0x281470(0xd21,0xc98,0x7a1,0xbcf))/(-0x5ff*0x6+-0xa9*0x28+0x3e69)+-parseInt(_0x59eed9(0x656,0x789,0x520,0xd7))/(-0x15b*0x17+-0x1fc6+0x3efb*0x1)*(parseInt(_0x281470(0x1234,0x10ba,0x897,0xd7c))/(-0x28*0xbf+-0x19b9+0x379a))+parseInt(_0x281470(0x1286,0xaec,0x12a6,0xde3))/(0x9*0x1ae+0x26d7+-0x6b*0x81)*(parseInt(_0x281470(-0x73,0x258,-0x62,0x3d7))/(-0x4a0+-0x4*0x7aa+0x2353))+-parseInt(_0x281470(0x92a,0xb9,0x4e9,0x54a))/(-0x2*-0xc81+0x26*-0xd+-0x1708);if(_0x593026===_0x482101)break;else _0xba1b3a['push'](_0xba1b3a['shift']());}catch(_0x52e54a){_0xba1b3a['push'](_0xba1b3a['shift']());}}}(_0x69ea,-0x1ee3c+0x2c256+-0x241*-0x11d));function _0x69ea(){const _0x16d3f3=['is\x20if\x20you\x20','nJncp','erikan\x20yan','jLJdC','igstory','ryOeF','UtQvF','🐭\x20Filesize','anda\x20menja','EypaD','194480xxdnnK','Januari','tap\x20seimba','ndai\x20cembu','sudah\x20repo','MP4','eron','Alquran\x20Au','i\x20celana\x20A','pat','enguasai\x20p','elek.”','\x20meriang-m','ggdWe','l,applicat','Sebuah\x20mej','zBKZc','ng\x20seharus','tugas\x20kepa','8300Ptpnid','Ryrft','h\x20apa\x20yang','\x20kejujuran','Mac\x20OS\x20X\x201','Mei','ap\x20basah.”','.0.3325.18','fline','f\x20atau\x20tek','enatap\x20mat','MPKpw','AByUe','mimetype','https://ww','soXKw','hItQG','mburu\x20Mile','active','Emoji-Mess','tau\x20putri\x20','oba\x20bebera','ziva','00101\x20Fire','&text=','cuddle','a\x20sama\x20den','vietnam','untuk\x20nafs','\x20bilang\x20ke','gan\x20sebaik','api-create','h-jauh\x20kan','ngat\x20benci','tiktokmusi','ng\x20cantik\x20','pXLfv','china','yNlxI','\x20\x0a\x0aPowered','dak\x20laku,\x20','tahun-tahu','Reply\x20pesa','rowId','/p_2032z7a','luhan','kpLzU','ntaimu.','VjuaG','dalah...\x20T','kerangmenu','gan\x20cara\x20k','delvira','only','\x20satu\x20tema','nime/','ahun\x20ini?','ka\x20kamu\x20ma','g\x20Anda\x20cin','tu\x20adalah\x20','04eea.jpg','ytshort','mode','ng\x20kita\x20ha','ommand_','it\x20dahulu,','know\x20anyon','ndekati\x20ti','TUBE\x20PLAY\x20','e\x20bawah\x20da','=0.9','chat','vutwz','Senin','jGetP','\x20kb\x0a🍃\x20View','\x20Gecko/201','ayuzawa','.0.3945.88','Upgrade\x20Pr','“Tidak\x20ada','JSyrd','\x0a\x09\x09\x09\x09\x0a_Upl','unda.”','/20100101\x20','amu\x20ningga','uka\x20sama\x20k','lah\x20menjad','chitoge','an\x20dikirim','yana','tu\x20dicipta','\x22input-gro','e-17','TUBE\x20AUDIO','em\x20Pakai\x20G','key','\x20kamu.\x20Aku','e-9','Emoji-Goog','3Fq%3D','Hanya\x20bisa','BzKKB','iBwav','(bgi\x20yg\x20mu','t,\x20seberat','ya.','JqFfJ','⛄\x20*Link*\x20:','dalah\x20lela','PvPdt','getDay','benarnya\x20k','*✉️\x20Size\x20Mp','tangan?\x20ka','pt_','oader','ile/8e308c','fullName','pinterest2','daripada\x20h','ion/xml;q=','akan\x20berul','prepareMes','JNjuD','ASilP','Firefox/47','tyVkD','0f7eb.jpg','\x20Title\x20:\x20','pernah\x20jad','waifu','\x20bot','\x0a🐣\x20Upload\x20','emilia','\x20Bisaku\x20cu','en?type=ch','hVQrB','_choose\x20th','ic.mp3','i\x20kekayaan','mu\x20dengan\x20','Botwea\x20©2k','KFOtc','kimseok','https://h.','\x20Semoga\x20ha','ik\x20anak\x20ma','nsfw_avata','ekarang\x20da','11KGAyhl','arta\x20benda','ail','ekarang\x20de','sbvGq','erapa\x20meni','e-20','hagia\x20dan\x20','ge\x20asupan\x20','gacha\x20ceca','.php?url=','“Wanita\x20ca','instavid','📊\x20*Quality','tsar','rhNYN','xOeQY','/crush','menemukann','e-13','Jum\x20at','WMXMv','fat\x20menjal','\x20sukses\x20it','\x20yang\x20cerd','videohd\x20','processing','sama\x20siapa','\x0a\x0a_Regard\x20','kamu\x20semua','product','wA0mo','app','source\x20cod','then','da\x20kehidup','status','ebih\x20keras','gCaWE','“Jika\x20kepi','gin\x20menjad','\x20pernah\x20ce','ubvNy','link','Emoji-emoj','Salah\x20satu','no\x20wm\x20ting','RtAYo','Facebook\x20M','2isPrefetc','Downloader','Link\x20Nya\x20M','getHours','aksana\x20tid','tiktok','hnkRQ','blowjob','bijaksanaa','20SuWylJ','instareel','g,\x20maka\x20se','yuri','api\x20perasa','ssjen','salahan,\x20c','ca37c.jpg','nangkan\x20😇,','NyCcK','book-','ge\x20downloa','a\x20inginkan','\x20sempurna,','pleWebKit/','LZtsF','rsebut','*📭\x20Quality','lewdkemo','ytmp3doc\x20','🦁\x20Ext\x20:\x20MP','ja).”','apat\x20kentu','\x20lalu\x20kiri','JwISK','termotivas','tuk\x20link_','Aku\x20enggak','cefron.nl/','of\x20more\x20th','a\x20dari\x20Bio','facebook','lami\x20sekar','TmpOS','up\x20untuk\x20m','Emsmn','88e11.jpg','sendMessag','ing\x20berpen','czLlh','\x20miring.”','wnvideo.ne','ooxymenu\x20c','dang\x20tidak','ipe\x20pacar\x20','njimm\x20gabu','NNpuh','ang-pasang','hn2Sqy4nHf','cope%22%3A','6285716360','f31132917d','AWaBd','\x20kalo\x20peng','r\x20di\x20dalam','\x20ingin\x20suk','ilih\x20Pay\x20/','nya\x20melind','g\x20dapat\x20me','a.”','ile/851e79','nerJid','baik\x20dan\x20b','.herokuapp','a\x201\x20hari','2987d.jpg','urce/BaseS','dul\x20video\x20','Title\x20:\x20','wgQKW','donasi','Hasil\x20Penc','Command','headerType','ML,\x20like\x20G','ile/d44d0c','rsluT','\x20untuk\x20dir','XwrWK','keDUK','a\x22\x20selama\x20','cript\x20bot','cache-cont','\x20perlu\x20har','“Jangan\x20pe','mKqeN','rsion/9.1.','utu.be/ERG','kPiI3/Xxar','kita\x20tahu.','makin\x20bany','pembayaran','?\x20rasanya\x20','image/','input','masa\x20lalu,','ytshorts','e-3','Oktober','0.9,image/','rv:47.0)\x20G','mu\x20jatuh\x20k','YDPBk','🏆\x20Contribu','lnya\x20cinta','\x20\x0a🎃\x20Size\x20:','.com/api/t','getSeconds','ku\x20saja.','/ttpcustom','sh;\x20Intel\x20','ucinta\x20lun','bYZew','4i1.jpg','satu\x20saja\x20','mama_gina','t.”','tweettrump','alan\x20adala','C%22contex','\x20Safari/53','promenu\x20co','rdianto-ch','aling\x20dita','Bila\x20tekad',',\x20berkobar','foto\x20pacar','xk1.jpg','MdwhK','E7FBAF','ng\x20jujur\x20l','site','a\x20tersebut','\x20adalah\x20ke','an\x20dalam\x20h','UnhsP','ented\x20in\x20t','pake\x20foto\x20','k\x20solusiny','fGpwE','aiumu.','QNVVH','zzvhy','rim\x20kesini','uk\x20memilik','id,en-US;q','UJCvO','this\x20if\x20yo','minum\x20kopi','61d42f7cac','utu.be/zq2','pp/surah/','“Cintailah','VxEOY','pencapaian','ke\x20Beha)\x20a','ile/435878','us\x20berhasi','/p_2032cf1','zVVYq','.0.3497.92','JxdMO','channel/UC','ukan\x20untuk','qtjpR','ang\x20lain.','ge\x20photoox','6288743504','dahulu\x20den','rce/get/?s','cmm','ri\x20Matemat','KANuh','tu\x20hak\x20kam','ALL\x20MENU\x20B','photo','ngmenu\x20com','me/36.0.19','ghfKq','“Saya\x20tida','5a5f9.jpg','facebook.c','dio','teriak\x20gaj','bunga','iskin.\x20Tap','rah.\x20Saya\x20','olvTx','nsfw\x20menu','a\x20dengan\x20k','asa\x20orang\x20','“Ingin\x20hat','oeYhb','i-zeeoneof','it\x20is\x20pres','berhasilan','epat\x20ia\x20pe','.2\x20Safari/','https://al','\x20jaminan\x20k','ketik\x20','eroyuri','\x20Karena\x20ya','ahegao','tt1','readFileSy','xtMessage','ah\x20merindu','BjZx9tvY','mikosiba','ttp3','01.7.7','di\x20bawah.','undefined','EOnMU','lo\x20pernah\x20','pBjwl','awoawo','ma\x20mencint','kYt]*','e\x20you\x20repl','\x20disajikan','bayi.”','/webp,imag','Sebuah\x20neg','lvWdX','BTsot','malaysia','noHre','utube.com/','kaori_miya','qnACD','kr_robot','owner','on\x20:\x20','de\x20diatas\x20','hati\x20yang\x20','enyapanya,','bat\x20buruk\x20','hVPrQ','Sabtu','0\x20(X11;\x20Da','Agustus','CATALOG','01.2.7','&impostor=','shinka','d-with','\x20mantan\x20da','ada\x20akhirn','0\x20(X11;\x20Ub','\x20|\x20Request','ton\x20silahk','DANA.\x0a4.\x20d','ngin\x20mengu','BvybR','\x20ke\x20kamu.','e\x20the\x20othe','o\x20?','\x20jadi\x20paca','load','&color=cya','ggota\x20grup','Emoji-LG-','2Fpins%2F%','gamu?','buah\x20kelua','reads','token','QPVUh','*----「\x20YOU','messageInf','xciQF','&color=red','ssage','ange;v=b3;','ge\x20quotesm','\x20untuk\x20men','eLYYe','SHrhb','Facebook','e\x20the\x20make','kimjondae','kimminseok','97m/gdkX/D','senang.','red','cTCgo','\x20Jangan\x20pe','ChoXI','ytmp4aja','rv:88.0)\x20G','dengan\x20bij','hanlah\x20ter','lana.”','zAaXj','🉑\x20*Ext*\x20:\x20','mu,\x20siangn','salePriceA','thighs','Not\x20a\x20Twit','l\x20pembayar','Emoji-Open','\x20:*\x201080kb','dak\x20untuk\x20','videoMessa','KZqeL7byF+','ar\x20sambil\x20','2e1.jpg','\x0a🚀\x20Ext\x20:\x20M','timestamp','mentionedJ','“Dalam\x20pan','598020psWcyi','emium','ngif','\x20boleh\x20keh','a\x20sama\x20ora','TaWoP','\x20inginkan,','RIMdZ','akkan\x20diri','hv\x20crush\x20o','xnuyR','dbfly','List\x20messa','.0.4515.10','korea','e-14','di\x20ringan,','t\x20hati\x20saa','ohsehun','obGAZ','\x20kuat\x20dan\x20','Orang-oran','manga','f5bcc57f70','mpurna\x20aka','UexYh','o7sv4hu4k-','erlebih\x20da','a\x2024\x20jam','sUCNg','a\x20sekampun','orang\x20yang','\x20sahabat?(','n,\x20aku\x20tid','ge\x20group\x20o','amongus\x20\x20c','h\x20atau\x20kal','ezaud','ownerku','kirim\x20foto','globalObje','i\x0a\x0a_Regard','pnCtm','sKCsE','e-7','e-18','=0.9,en;q=','sah\x20payah\x20','siapa\x20oran','yOqUm','*🔗\x20Link\x20:*','OVLqG','\x20Anda,\x20tet','\x0a⛄\x20Ext\x20:\x20M','Hbvrp','kenapa?','\x20(KHTML,\x20l','Regard\x20@','a\x20dengan\x20m','iar\x20Tuhan\x20','0\x20Safari/5','.3\x20Safari/','n\x20membasah','supan/','/\x20Bayar\x0a3.','acecan\x20com','ya,\x20maka\x20g','n\x20itu\x20bera','ext=','aura','“Kegagalan','ataannya.','PLQwJ','DUqUB','images','mediaKey','ya\x20milik\x20t','Video\x20(720','n\x20rumah\x20mu','nux\x20x86_64','i\x20lima\x20itu','=100&fillT','semoji','n\x20botnya\x20','(main\x20bare','Big\x20Thanks','instavideo','ngan\x20cara\x20','kimnanjoon','mmand_','Hi\x20@','kHXdw','orvnT','🤥\x20setiap\x20n','jiTbg','KLDhM','gue\x20anak\x20l','\x20contoh\x20:\x20','ndapatkan\x20','dsMlY','i\x20selingku','sfw/','Mediafire\x20','OmUcr','instafoto','tu\x20dia\x20unt','OwuwZ','fox_girl','alquran_au','tsapp.net','contextInf','video_info','h=800&scal','a\x20atau\x20tah','fnWSZ','alasannya\x20','match','hinata','t%22%3A%7B','\x20ini.\x20Peke','\x20\x0a🐣\x20Durati','\x20sampai\x20sa','and_','vERBA','enjadi\x20ora','medias','ian','alaman','isa\x20makan\x20','pernah\x20nol','footerText','in\x20kamu\x20do','hmu.','Tak\x20semua\x20','YSVGq','legra.ph/f','aya\x20Apa\x20Sa','🎀\x20*Title*\x20','🍁\x20*Ext*\x20:\x20','jiso','asupan\x20men','ng\x20akan\x20ka','pBnas','sudah\x20tert','RPjvd','ument.pres','XGGfS','trap','uGrli','u\x20kecewa.','Tiktok\x20Mus','ied\x20does\x20n','DSiJO','der','P4\x0a🆔\x20ID\x20:\x20','m/c/zeeone','an\x20turun\x20k','0.8,es;q=0','animated_g','Aku\x20gak\x20pa','tek\x20yang\x20p','nget','\x20:\x0a1.\x20Buka','\x20gunung,\x20t','tik,.\x20Rusa','xLrxj','popoci','dicari_','sama\x20bersa','\x20didalam\x20g','mang\x20mampu','UoLPE','ritaan','r\x20oleh\x20ser','e\x20the\x20phot','https://te','w6.flaming','buttons','a,\x20maka\x20An','ytmp4','🐨\x20Owner','pdDqX','fbaudio','enger-','mTzbL','put=true&s','forwarding','it,\x20lalu\x20s','\x20menjadi\x20\x22','accept-lan','qYRMo','ogan/','\x20nyuri\x20uan','rLctW','n\x20ke\x20manta','atu\x20kampun','1b207.jpg','0_11_6)\x20Ap','nzkTW','asal\x20janga','an\x20pacar,\x20','hololewd','Usernameny','bZDVY','hal\x20yang\x20a','viona','\x20nona\x20cant','e-4','kuni','tiktokwm','agi\x20yang\x20j','mfVkw','ayuki','magenta','dak\x20makan\x20','Emror','fbhd','membuat\x20se','infobot','nisa','*📛\x20Title\x20:','ytmp3\x20[lin','WO\x20\x22\x20selam','“Menahan\x20g','ikit\x20yang\x20','./lib/coup','🔭\x20*Ext*\x20:\x20','ena\x20aku\x20sa','seperti\x20se','dang\x20berpu','sheet','.com/api/a','Sederhana\x20','baka','Pernah\x20suk','menyenangk','ng)sama\x20si','\x20mungkin.\x20','https://d.','sApp-','SeYiN','instastory','ile/277163','dest','ting?\x20Taku','onlygc','gan\x20takut\x20','\x20Chrome/90','ekolah\x20apa','Win64;\x20x64','tkImZ','SALEHA\x20(Su','mount1000','Eror','ya\x20juga\x20ti','ji\x20karakte','asukan\x20kul','&data=%7B%','MHJLT','ke\x20lawan\x20j','mIzPu','rYAYVeWq-7','Subscribe\x20','rsebut\x20sam','EBEMw','0\x20(X11;\x20Li','0@s.whatsa','Jangan\x20per','di\x20ringan','.mp3','result','api\x20memban','\x0a▶️\x20Ext\x20:\x20M','jhTtW','BkmJE','jhope','xiEur','ile/cb6aaa','\x20:\x20','record\x20voi','0_9_5)\x20App','QlMrh','512@s.what','Tidak\x20ada\x20','Ketika\x20kam','ma,\x20bersik','h\x20mencoba\x20','igdl','select\x20the','yang\x20membu','🐺\x20*Link*\x20:','katacaklon','\x20dalam\x20ben','spank','rose','nyurl.com/','xmlformats','Choose\x20vid','ISuQP','FZNjAqSMng','Sekarang\x20a','HcrAN','cript=sket','aksana','contentTex','VERIFY','tengah\x20jam','ebih\x20baik\x20','elajaran\x20s','isa\x20mencon','/15.15063','jak\x20sini.”','sih\x20jomblo','ku\x20anak\x20pu','uBHrl','ge\x20otherme','dmenu','engan\x20hart','\x20kita\x20kerj','diaGm','Emoji-Sams','c41.jpg','vMGKB','r\x20?”','?apikey=of','classic','ejadian\x20ya','remium\x20saj','💦\x20Title\x20:\x20','tahu,\x20Sema','GOPAY\x20via\x20','document','KHTML,\x20lik','doRGe','igphoto','\x20:*\x20sd,\x20hd','ps://youtu','call\x20whats','hal\x20yang\x20p','caption','“Jika\x20anda','cZRsm','1338RXeRDe','bJCad','getMinutes','aha\x20adalah','tagmenu','pHFlK','katabijak_','+0Mjbi7yii','panties','kirim\x20pake','videohd','cgQll','\x20|\x20','Alquran\x20Mu','ile/4dfc9b','referrerPo','List\x20Messa','Video\x20(360','.0.3497.10','kali\x20Kena\x20','Bila\x20Anda\x20','pernah\x20gak','Kb\x0a🍃\x20Views','video\x20ini\x20','n&apikey=h','e91d325a70','attr','```Jadikan','sa\x20menghas','loadmenu\x20c','e\x20the\x20stor','0\x20(Windows','kobot.xyz/','BBdHP','\x20dan\x20meren','indah.”','an\x20the\x20lim','sai\x0a\x0a_Rega','url','igtv','\x20Nominal\x20p','igned-exch','e-22','g,\x20kuning\x20','reply\x20mess','dMaGN','besar\x20dari','https://f.','SWjkm','yat','i.simsimi.','Apakah\x20tak','ma-sama.','MP3','u\x20berenang','Cara\x20palin','\x20akan\x20hanc','e-15','&apikey=ha','telegram\x20s','buttonId','vDxaT','ntuk\x20bahag','x\x20x86_64;\x20','kebiasaan\x20','productId','stc\x20comman','a\x20menjadik','ml.documen','join','.com/api/n','68abaa14f7','get','-officedoc','LKMwu','ote,\x20terus','「\x20*INSTAGR','e\x20who\x20cont','\x20:*\x20720kbp','m\x20ke\x20salah','Pkqne','nsfwmo','\x20*DELIVERE','katailham','ySYUb','tanyze;\x20Li','l\x20seseoran','EBOOK\x20DOWN','ya.\x20Andai\x20','O\x20\x0a•\x20FATIH','hal\x20memban','tiktok.com','error','yukino','an,\x20dan\x20im','anya\x20dua','mengerjaka','./zeeone/z','amu','Reply','Ex\x20:','impostor\x20=','wAkFn','bodoh\x20dari','pai\x20tua.”','utu.be/1O2','📍\x20Tanggal\x20','\x20itu\x20kunin','\x20yang\x20diwa','\x20false\x20\x0aco','ributed\x20to','wanita\x20sam','o\x20pas\x20di\x20s','\x20media\x20you','ryXEb','cky\x20to\x20hv\x20','iEcrE','com/2/time','Firefox/33','nstagram.c','thumbnailU','lah\x20suami\x20','IANMd','\x20tidak\x20per','menu','n\x20ss\x20ke\x20pe','ta\x20tidur.\x20','stanzaId','o-k40Q-gO_','\x20yang\x20dulu','sec-fetch-','NOWM','Scale=true','\x20Bot\x20sebag','48acc.jpg','rga','an\x20Anda\x20si','en68','lTmeX','etik\x20@veri','ng\x20:*\x20','3\x20:*\x20','2\x20Safari/6','./image/','hanya\x20seka','Jika\x20Anda\x20','ion','ersion/11.','wdFbg','offline','cOVZS','pada\x20banya','mencintai.','rimu\x20menye','ODby7_MoRY','Cemburu\x20it','n/json,\x20te','e-16','upHqi','du,\x20ini\x20be','d6da00d7ae','ytmp4ajahd','dagang\x20kak','jowBO','✨\x20*Followi','\x20Chrome/69','makermenu','hCDmq','\x20akun\x20mu\x20s','0_10_5)\x20Ap','R*\x0a\x09\x09\x09\x09\x0a','EYsCt','WbUmT','naruto','Gw_16l7c','“Cara\x20terb','dari\x20batas','\x20kesulitan','LOADER\x20」--','e&apikey=h','jOpzV','berapa\x20saa','deliveries','_Silahkan\x20','PR-ku\x20adal','n\x20kamu\x20dan','ra\x20gara\x20si','rd\x20dan\x20Vis','externalAd','i\x20tertangk','smRmk','IpDzN','i\x20kunci\x20me','Utrrd','las,\x20yang\x20','false%7D%2','a\x20sama\x20sia','trump','n/xhtml+xm','vxlzq','age','elihat\x20ke\x20','nks\x20to','kursi,\x20sem','an\x20bodohny','\x20this\x20scri','a\x20mengubah','X/A=','a\x20peluang\x20','\x20tahu\x20adal','々\x20Size\x20:\x20','mio_akiyam','i\x20size\x20thu','apa\x20ketaku','etangga.”','nILei','tPShS','aminan\x20keh','rs\x20:*\x20','ua\x20pria\x20me','net/v2/?lc','☃️\x20*Filesiz','rgeyy','%22%2C%22s','Nopember','jpegThumbn','Ingin\x20Namb','maka\x20akan\x20','ttmp3','chat\x20ke\x20ko','.0.2743.11','wallpaper','ago','ku\x20tidak\x20b','TDUlg','is\x20yang\x20ad','elas\x20lalu\x20','nnagen&tex','Kerang\x20Aja','ebelum\x20jad','603.3.8\x20(K','\x20\x0a•\x20FERDYZ','Mohon\x20maaf','gi\x20pemanda','gbTqy','ses\x20maka\x20l','ur\x20hanya\x20d','x-app-vers','AOzUq','floor','main','RfqRJ','P3\x0a🆔\x20ID\x20:\x20','https://e.','atkan!*','hadap\x20sesa','facebookhd','https','vfKzo','e\x20effect\x20m','PfVug','Terjadi\x20ke','karena\x20mem','alan\x20untuk','APjxw','ic\x20Downloa','DGWCH','gi\x20setiap\x20','untuk\x20sese','sername\x0aim','text.com/n','\x20:*\x20','ter-',';\x20rv:45.0)','more\x20quote','jhMym','osoft-','dhNJq','Youtube\x20Mp','ku\x20di\x20tubu','kILMO','lamnya\x20aku','a,\x20jika\x20ka','Please\x20Sub','\x20NT\x2010.0;\x20','37.36','rdFkJ','81IUq16cHj','sponsbob','goprem','rows','waitForAck','jisoo','https://mm','ama,\x20bersi','gt\x20kamu\x20ch','“Wanita\x20ya','ttonId','vzVGU','WehSs','media_url_','ba\x20ulangi\x20','ukhty','filter','Mozilla/5.','applicatio','te.json','dmenu\x20comm','\x20membagika','/p_2032o0l','UJIPg','ak\x20tawa\x20ak','jar,\x20semak','tai','Emoji-HTC-','ntik\x20bukan','ita\x20tidak\x20','ecko/20100','RAGDs','ttmp4','locationMe','\x20vn\x20kesini','[PlayMp3]','x/47.0','Lelaki\x20yan','zono','uUkYP','JxXJl','amu.\x20Tapi,','\x20pedagang\x20','inya,\x20menj','orMGs','e\x20the\x20owne','pinterest','cript\x20Bot\x20','0FGwkw==','Emoji-Micr','ang\x20kamu\x20a','gCXFe','ok\x20buat\x20ku','epadanya..','adi\x20diam-d','OKLzy','om/','ku\x20gak\x20bis','605.1.15\x20(','\x20khusus\x20un','YNfMS','terburuk\x20l','e\x20the\x20anim','buttonsMes','indonesia','audio','text','idak\x20berus','25.181\x20Saf','Tuhan\x20memb','esmenu\x20com','kalo\x20tidur','emas,\x20emas','mizore_sir','\x20:\x20_','media','ntent','\x20nomor_sur','\x20akar\x20untu','filename','sageFromCo','slim)\x20pern','ZYnsf','9999999','biakR','elak\x20tawa\x20','ain\x20tertaw','balik\x20kepa','api/imageg','g,\x20maka\x20bi','an\x20button\x20','tat\x20yang\x20s','\x0a\x0a•\x20ZEEONE','ddagL','IqLmi','businessOw','SINGLE_SEL','itu\x20sama\x20d','&color=yel','bagimu.','bG0=','ap\x20kerasla','kerasukan\x20','⋮☰\x20MENU','\x20i\x20love\x20u,','mediaUrl','u.\x20Asal\x20ja','BANGET\x20NGG','Dimanapun\x20','sewa_kak','Kamis','MD\x20\x1b[1;37m','profilePic','intar.”','e\x20:*\x20','m\x20terdafta','/p_20325yp','z4puTs%3D1','rgabung\x20da','rslIv','fbsd','quLxg','kanmu.\x20Leb','fWveO','ticker','ana\x20dan\x20bi','.0.3865.12','🦁\x20*Ext*\x20:\x20','nya\x20sendir','couple','asi\x20lebih\x20','🌀\x20*Link*\x20:','513@s.what','eeoneofc','sqFKa','r\x20sejati\x20p','kNrZV','buah\x20seped','tuk\x20user\x20p','aiQGf','getMonth','📖\x20List\x20Men','shana','remoteJid','\x20hidupku.\x20','f04fc9dd2d','kan\x20apapun','ntah\x20*','XucWS','iri.','ero','at\x20ini\x20mun','adi\x20buat\x20a','RhWrM','hug,','as\x20itu\x20ada','quran-api-','0_9_3)\x20App','Yang\x20palin','you','\x0a•\x20HADS\x0a•\x20','ce\x20baca\x20su','PZreo','pa\x20kita\x20su','kan\x20Nomina','n\x20Masukkan','*BROADCAST','chats','les','rzvfT','tu\x20hubunga','me/67.0.33','t\x20yang\x20kua','a\x20membuatm','tunggu\x20fil','h\x20terhadap','EL\x0a•\x20SATRI','video/mp4','\x09\x09\x09\x09\x0a💦\x20Tit','angkuk\x20bua','lication/s','e|yellow\x20','ovQgq','idur.”\x20–\x20P','mu\x20yang\x20pe','k\x20seorang\x20','eriang\x20kem','salahan\x20co','No\x20media','na\x20berhubu','akame','source','Mendidik\x20s','\x20orang\x20lai','0&fontsize','MpRcb','data','getsider','tiktoknowm','nWpDlmrk','kan\x20istrin','ge\x20gacha\x20c','\x20dia\x20\x22i\x20lu','a\x20bagi\x20wan','e\x20media\x20se','Selasa','caya\x20diri.','69280.jpg','e\x20the\x20grou','a\x20:\x0a1.\x20Buk','iUCzU','mand_','Gyjld','🐭\x20*Filesiz','a\x20cogan\x20co','neko','YuTHc','api','a\x20di\x20negar','7\x20Safari/5','jaminan\x20ke','kalau\x20tida','headers','\x20yang\x20tert','HSNJU','beberapa\x20k','test','YxAcv','\x20bukan\x20aku','hYsJk','tpwXOQusBR','alcakenya','HJSAa','Jangan\x20rin','\x20hidup\x20leb','pergi,\x20tet','💌\x20Informat','Kit/537.36','api\x20apa\x20da','Check\x20Limi','teriak\x20pak','k\x20ngiler\x20d','lTORv','.com/api/c','idak\x20minum','bocil','April','MU\x20CANTIK\x20','All\x20menu\x20b','find','GoKNK','yJNTb','\x20paling\x20me','amu\x20akan\x20l','ile/4a725e','erokemo','x/88.0','\x20\x20M\x20P\x203*\x0a\x0a','ge\x20funmenu','Tiktok\x20WM.','*🎧\x20P\x20L\x20A\x20Y','bisa\x20membu','a4e88.jpg','ori','DXsOp','OONGH','guest_toke','instaphoto','an\x20tetap\x20s','IXRVs','uKOoY','0_10;\x20rv:3','hzpWK','t\x20di\x20dunia','RoevS','sourceUrl','ta\x20menjadi','winry','mu!','P-mu.\x0a2.\x20P','9a236a4','ten\x20untuk\x20','dzWwbApjky','\x20sange','rku\x20gak?\x22\x20','lCiXz','kaga_kouko','cum','FndbZ','ng\x20dikirim','l-hal\x20keci','g\x20yang\x20bis','\x20bisa\x20mera','ang\x20adalah','ngut','/p_2032mwb','oading\x20fil','t/download','BOT','HTML,\x20like','log','VINZ\x0a•\x20MIS','sakura','\x20gak\x20ngert','\x20saya\x20diam','lewdk','256','endiri','.0.4183.12','gan\x20mendid','50109.jpg','Audio\x20(128','fbaudio\x20','ile/1599f5','ile/924310','ytmp3doc','sule\x20sampe',',\x20maka\x20tem','XMLHttpReq','n\x20adalah\x20p','relayWAMes','🐣\x20*Title\x20:','\x20to','☃️\x20*Ext\x20:*\x20','image\x20effe','audio/mp3','h\x20datang,\x20','\x20ninggalin','n\x20berkeing','QPeua','dino_kunin','adalah\x20tem','ih\x20lama.','uhkan\x20agar','09438.jpg','mediafire','untu;\x20Linu','kFACr','katalog','“Manusia\x20i','Sudah\x20dapa','\x20Chrome/52','AAAAAAAAAA','ggCJP','imageMessa','rat\x20apapun','e\x20the\x20funm','pernah\x20cin','yIPtx','isForwarde','pink|purpl','akan\x20menja','endidik\x20se','9YP2ZQn4','isa\x20melaku','DbDSr','nYncE','bukanlah\x20j','unda\x20maka\x20','gal\x20pilih\x20','ter\x20URL','OeOyw','ng.','me/65.0.33','eo\x20or\x20Audi','n/vnd.open','ang\x20berpua','fox/68.0','ch-name&do',',\x20dan..\x20pa','Sahabat\x20ya','NCimy','mu\x20jadikan','n\x20kekayaan','asuna_yuki','gun\x20dari\x20t','feed','\x20Anda\x20iala','kembali,\x20a','e\x20sini','ang.','wait','Tiktok\x20No\x20','fbsd\x20','v9gIBMu9tX','HkNnm','RIFY','\x20akan\x20liha','ar\x20kamu?','Semakin\x20ra','e-5','601.2.7\x20(K','Penundaan\x20','x-guest-to','000','Kirim\x20peri','g.whatsapp','ri/537.36','User-Agent','Rabu','RCGNS','Click\x20butt','pa\x20saat\x20la','ak\x20makan\x20d','mbawa\x20aku\x20','seseorang,','geCount','push','.com/api','enu','ut\x20itu\x20pen','\x20akan\x20tera','Sub\x20Menu\x20k','1;37m>','Kita\x20tak\x20b','ofc','.0.4324.10','96.87\x20Safa','ument.word','e\x20the\x20down','lewd','image','YMESH','app\x20mod\x20ka','heLMd','ika.\x20Lebih','an\x20dari\x20ke','wqKmU','37.75.14\x20(','pa\x20aja?\x20be','https://yo','\x20normal\x20di','nama','lin\x20aku,\x20a','e-1','uran\x20diman','guage','sangan\x20hid','parkchanye','ara\x20tidak\x20','akin\x20banya','\x20:\x20_@','kimjunmyeo','aat\x20masih\x20','zKAmL','4\x20Safari/5','convertmen','pekerjaan\x20','\x20ketika\x20ka','Score','a,\x20namun\x20m','OWNER','ari\x20Tuhan','sWItt','MhtNs','Xgqai','x-pinteres','3n1n1Kbt6a','erorLink','enu\x20comman','\x20by\x20@','anak\x20saya\x20','684ea07ea1','Kalau\x20kamu','ernyata\x20pe','etRvc','a,\x20agar\x20te','ang\x20intens','seMessage','emakin\x20sed','RSJal','facebooksd','post','u\x20seseoran','2|4|0|3|1','femdom','emburu\x20itu','at\x20dan\x20nik','followers','ile/9fc9b3','qilUe','dan\x20hidup\x20','au.\x20Salam\x20','```SUCCESS','akit\x20dahul','dikuburkan','ge\x20convert','ING\x20GROUP`','ih!\x20Don\x27t\x20','qhyQV','_\x0a\x0aCaranya','ya\x20kak🥰','\x20terus\x20saj','n\x20yang\x20tid','i-alphabot','eyakinkan\x20','dikit\x20perk','GXvgd','pasrahkan\x20','storagemen','q=0.01','&crewmate=','\x20selama\x20ia','e\x20the\x20asup','\x20yang\x20kita','yBbbH','mu\x20belum\x20s','▶️\x20*Filesiz','etch_conte','h\x20satu','☃️\x20*Link*\x20:','Moji-','h\x20memberi\x20','kawan_spon','animemenu','\x20pekerjaan','88f9e.jpg','0.11;\x20rv:4','mu\x20beleum\x20','jWrZX','agi\x20pria\x20a','NAGSd','avatar','an.herokua','galanya\x20ba','fromMe','NIDtC','HZeMt','ttp1','Desember','\x20」----*','salah\x20past','\x20https://i','rQCLL','an\x20menu\x20co','ybQrq','dan\x20mengun','textpromen','ZtpQI','leWebKit/5','ta\x20lupa,\x20s','*📧\x20Size\x20Sd','📍\x20Waktu\x20:\x20','•\x20\x20','keta','152DiBgQG','n\x20botnya','title','\x20dan\x20Masuk','RiWVs','success','aan.','he\x20form\x20of','rd\x20@','a,\x20sebuah\x20','\x20gc/luar\x20g',';\x20rv:68.0)','SELECT\x20HER','earchResou','n\x20bukanlah','gojosatoru','fillTextPa','manusia_li','MdHsn','\x20a\x20link_','0\x20(Macinto','previewTyp','.0.4430.93','ealdC','t1000','asukan\x20bel','https://ha','api\x20orang\x20','Hidup\x20itu\x20','e-11','masturbati','jaMrY','🐣\x20All\x20menu','/p_2032vcy','videoId','ephemeralM','ami','saya\x20menja','442438afe8','at.\x20Biar\x20a','ukan\x20oleh\x20','\x20takut\x20kam','VIfGN','qbGmm','HSihc','ah\x20nomor_a','truth','🌀\x20*Size*\x20:','bdsm','18241b0142','u\x20apa,\x20tap','ngan\x20sampa','Kita\x20tidak','ument.spre','productIma','Emoji-Face','lu\x20sih\x20nam','ile/ad77c1','logi.','3AGWWjCpTn','drop\x20emot\x20','653158378f','\x20-\x20','Cinta\x20adal','sa\x20bahagia','\x20pgn\x20balik','line/conve','h\x20dia\x20deng','map','tahuan\x20tan','https://g.','rTSVO','bergerak.','BzLio','dang\x20dikir','Dengan\x20dem','NkBAVg8GMe','rmenu\x20comm','\x20dan\x20masa\x20','\x20Aplikasi\x20','tt\x20anjimmm','ponseMessa','kucing','pap\x20ke\x20sal','\x20berharap\x20','benedict','gan\x20adalah','\x20*READ*','yang\x20akan\x20','.mp4','_\x0a\x0a\x0aCarany','pageCount','85459.jpg','https://ne','TUBE\x20VIDEO','eXI.enc','\x0a\x0a\x0a•>\x20Nama','s\x20bot_','endsWith','ile/610245','ttdownload','sehat.\x20Gel','\x20Media','messageSen','r\x20yang\x20coc','erah\x2024\x20ja','\x20kehancura','FUL\x20CLOSIN','984f9.jpg','on\x20below','ode\x20diatas','Ijtlo','selectedBu','mu,\x20dan\x20ma','kbps)','a\x20lupa,\x20Se','accept','EupEm','111621LZKjtU','getYear','takut\x20mera','t_mode=ext','uah\x20biola;','🔖\x20Sewa\x20','e\x20the\x20conv','hubunganny','00587','tentacles','type','nuju\x20kegag','t\x20siapa\x20se','usic\x20Downl','mewujudkan','a\x20apa-apa.','https://va','top4top.io','tyni','im\x20mungkin','amongus','RdxPc','Nayrx','\x20bohong,\x20i','\x20memilih\x20u','1919191919','youtube.co','RwhhF','\x20didapet\x20a','gdkdG','ung-','\x20akun\x20kamu','ytmp3aja','ki\x20yang\x20bi','\x20sekarang,','q=0.9','text/html,','NsyCG','CqMLZ','cNPBV','\x0a🚀\x20Upload\x20','https://j.','iqqZW','jahy','soundmenu','7326@s.wha','method','g..??”','listRespon','37.36\x20(KHT','tryAgain','nd_','ulit,\x20tapi','iapa\x20yang\x20','\x20apa\x20lagi\x20','adalah\x20yan','e-8','shiina','ipt,\x20*/*,\x20','ceIex','OBWjH','yViOF','at\x20(serah\x20','ele_sticke','sedikit\x20da','\x20tahu,\x20sem','ketik\x20pake','tits','sh/pacar\x20s','ihat\x20siapa','username','\x20Chrome/65','rkanmu\x20mel','limit','GKSvC','*Link*\x20:\x20','resource_r','en-US,en;q','feetg','ppcouple','AK\x20BOHONG\x22','\x20fitur\x20ini','erza','ZAcPM','AUDIO','ikirkan\x20ka','catch','Pura\x20pura\x20','USD','tidak\x20munc','an,\x20namun\x20','“Carilah\x20i','EPwHC','Usage:\x20','postor\x20=\x20t','JIGMu','sewabot','playmp4','le-','youtubesho','ng\x20kaya','LIST\x20EMOJI','buttonText','seperti\x20ha','wMSyG','」----*\x0a\x0a💦\x20','OKPsC','zkNnQ','idex-','ID4Fmo9w','uXukw','ixHnS','*Cewe*','nita\x20yang\x20','e\x20the\x20allm','--*\x0a\x0a','ka\x20Lupa\x20Pa','age!','SQJVk','ya\x20ngiler\x20','8ball','suga','dohkyungso','aimu.','ge\x20Big\x20Tha','dia\x20bales,','da\x20diri\x20se','\x20tepian…be','utup.','📛\x20*Usernam','kXrt+8eqBg','ar,\x20hingga','utu.be/1r_','alloc','3595319159','currencyCo','entries','ge\x20ownerme','an\x20senyuma','ogan\x20menu','an\x0a5.\x20Sele','akan\x20membe','emojinya?','ai\x20admin\x20t','pussy_jpg','kimjong','\x20suka\x20sama','tqto','sinrd','ang\x20bisa\x20m','udio\x20senda','e-19','user','i\x20sesuatu\x20','Kalau\x20bole','Anda\x20menja','ize','7.36','\x0a🍃\x20Views:\x20','e\x20the\x20nsfw','k\x20yang\x20kit','u\x20sange','akukan\x20apa','\x20&\x20mp3\x0a','lonte','zlDGA','https://do','h\x20banyak\x20u','💌\x20*Followe','Bahagialah','SthTY','wn|cyan|da','ang\x20daripa','WFUoj','dfIpf','\x20harganya\x20','977f4.jpg','full_text','Orang\x20kaya','\x20seseorang','holo','ersion/7.0','DOCUMENT','gacha\x20coga','anmu.”','agi\x20sedih\x20','ikin\x20senen','anya\x20pende','xEfin','photooxyme','\x20cinta\x20sam','BZrkC','wcPQm','indukan\x20ka','patrick','rbelalak\x20a','\x0a🎃\x20Size\x20:\x20','185066awxKsY','aya\x20atau\x20m','dakan,\x20tet','instagram.','at\x20anda\x20te','di\x20seperti','FpBZK','miskin\x20tid','yZpdU','DcZiG','Februari','iGWaj','a\x20yang\x20ing','\x20」----*\x0a\x0a🎀','u\x20hanya\x20un','g.us','dengan\x20com','an\x20akan\x20be','gubah\x20masa','g\x20kreatif\x20','IBOKG','\x20dalam\x20sua','ka\x20aku\x20sak','KENFj','ripada\x20sal','views','\x0a🆔\x20ID\x20:\x20','lDqgn','com','DER*\x20」','eUuqL','1.2\x20Safari','extendedTe','group\x20clos','e\x20kata\x20kas','\x20diri\x20send','suksesan\x20y','ecan','aran.”','Ilmu\x20penge','mp4','\x0a\x0a_Tunggu\x20','rnah\x20kamu\x20','75616e6ac9','NwIzUejRCO','s\x20💞','jhCiK','tidurlah\x20s','ngan\x20yang\x20','g\x20sukses\x20a','rKaXQ','Next\x20➡️','kemonomimi','3.0)\x20Gecko','htOJs','OnzqK','ekarang\x20ju','22options%','Hai\x20@','bA1B2pJ18r','EEsgD','Masalah\x20ak','ungi\x20tanpa','rcvDW','\x20butuh\x20beb','POST','dalah\x20peng','meow','ffect','u\x20bukan\x20ti','sbob','sebutkan\x20t','\x20yang\x20udah','zettai','pernah\x20suk','601.7.7\x20(K','wuyifan','aya\x20tidak\x20','GDlxr','\x20dilempar\x20','ile/ec84cb','ng\x20dan\x20mer','descriptio','\x20database.',':\x20https://','random','kebohongan','nda.”\x20–\x20P.','pragma','nSJrF','AR5b5YFz2g','\x0a\x0a_choose\x20','3z1.jpg','EvbWv','srHep','doge','\x20:*\x20480kbp','wOIxM','WM.mp4','101\x20Firefo','*----「\x20FAC','6199803015','pp.net','di\x20tahun\x20i','verify','“Pakaian\x20i','\x20luas\x20dari','quoted','🐣\x20*Title*\x20','gan\x20jika\x20k','g\x20yang\x20pal','Maret','EdsnR','PajPo','priceAmoun','\x20melempar\x20','at\x20orang\x20l','fYvhn','g\x20nyokap\x20a','\x20a\x20reply!','a\x20lebih\x20ke','split','video*\x20_Ju','lPMKt','angemymind','boleh\x20beda','an\x20kalau\x20y','k\x20susu\x20seb','7.0)\x20Gecko','/p_2032xlj','e\x20the\x20upme','8a682c0e76','%7D%7D&_=1','instagrams','hEHYM','*_Cowo_*','\x20yang\x20kamu','4DvtQuh','message','tipan\x20itu','makYn','primary','cintai,\x20be','ymenu','an-temanmu','ngungkanla','nmu','nu\x20command','dtCgc','5000000000','Saat\x20yang\x20','\x20jadi\x20\x22\x20BO','ecan/','erofeet','tion','YyDQn','Aku\x20bisa\x20s','all','format','essage','rsation/%s','bagian\x20ter','oruptor\x20di','audio/mp4','json','ibu\x20pengem','“Gara\x20gara','menjadi\x20se','hLTvJu4FA3','body','CtclW','/605.1.15','geayubi','lpha/v14','ak\x20orang?\x20','orbanan\x20mu','sourcecode','ar\x20bunga\x20k','PHOTO','a\x20Masterca','950586UhlyiH','pussy','sento_isuz','100000','vn\x20trus\x20ki','kir\x20jalan\x20','ih\x20kuat\x20da','akan\x20dan\x20t','ile/6de20d',')\x20AppleWeb','ru.\x20Malaha','September','\x20Scan\x20Barc','each','uery','jenni','ile/4c2e47','an\x20tonton\x20','e58a0.jpg','i\x20audio\x20at','jungkook','sNupY','Url?','chika','terakhir\x20b','ari/537.36','mangat.\x20Se','LIST\x20MENU\x20','Kcgkf','ngan\x20erat\x20','\x0a🌀\x20Url\x20:\x20','entationml','NmiZh','ndung,\x20Pag','Membaca\x20se','WBuKW','u\x20want\x20to\x20','drop\x20kutip','u\x20mungkin\x20','a.\x20Bisaku\x20','\x20diam\x20dan\x20','tiktokaudi','ta\x20bertepu','ntities','Masukkan\x20q','e\x20the\x20tagm','e|red|whit','ak\x20ingin\x20t','yang\x20kita\x20','e\x20note\x20bil','kan\x20beraki','Ccugb','sFUrp','it\x20per\x20har','nah\x20mencob','akan\x20hancu','Url','update','phyiZ','Watermark','\x20tapi\x20tida','e-2','ynzdo','mat\x20untuk\x20','adsheetml.','\x20bilang\x20\x22a','eh\x20itu\x20kar','pp.com/api','\x20Chrome/77','gif','Next\x20\x20for\x20','Pilih\x20sala','lXLgM','u\x20bilang\x20b','RESPONSE','kimtaehyun','e\x20the\x20kera','ya.\x20Yang\x20s','Emoji-Appl','LjjyN','ri\x20akar\x20te','com/1.1/gu','aya\x20takuti','solog','video','hentai','ecko)\x20Chro','amsal','bmjKb','untuk\x20meng','ngan\x20aku\x20y','e\x20the\x20gach','fbdl','\x20akan\x20menj','IABgY','\x20yang\x20bera','menu\x20comma','groupmenu','dare','.json?twee','mu\x20telah\x20m','n\x20dipilih!','extended_e','uH5E6I8xnZ','ownermenu','getik\x20di\x20g','\x20Gecko)\x20Ve','6\x20Safari/5','ika\x20Anda\x20i','urce%22%3A','RWjJT','sage','vkWuy','?text=','pi/v2/img/','QSCYG','nnya.”','_Untuk\x20dur','orig','garuh\x20kepa','ngChange','mah\x20Anda\x20m','ada\x20yang\x20m','fileName',',\x20Setelah\x20','lKBfY','pb1.jpg','ilkan\x20lebi','\x20DOWNLOADE','\x20jomblo\x20it','hope_boy','\x20hanya\x20dua','w.pinteres','elanga…\x20Ga','ang\x20bohong','fileEncSha','holoyero','\x20Silahkan\x20','rkgreen|li','FefsT','EuGXs','pmenu\x20comm','ss\x20recent\x20','603.3.8','e-10','jadi\x20orang','e\x20Gecko)\x20V','ezmmL','jJVRZ','hkan\x20dipil','pagar\x20peli','🏅\x20Source\x20S','othermenu','telfon\x20cru','fd073.jpg','EAHuS','ownload_','e*\x20:\x20','e-21','fd896.jpg','Sewa\x20☕','le.js','tungkan\x20da','FULLY\x20OPEN','harus\x20kamu','sendEpheme','sebentar\x20m','PigZx','zUeht','hulu```','Emoji-Twit','utu.be/BjT','ZPExn','ilangan\x20se','a\x20yang\x20And','bahtera\x20hi','know\x20sourc','\x20\x0a▶️\x20Durati','pGXNO','a\x20Anda\x20lak','e\x20the\x20tele','ganti\x20nama','ak\x20menikah','kutin','udian.”',',\x20supaya\x20s','Hampir\x20sem','Rqgav','\x20Chrome/92','batu\x20oleh\x20','ttp2','0_9_4)\x20App','ZEEONE\x20OFC','Instagram\x20','ertmenu\x20co','rsion/9.0.','erjebak\x20ol','if,\x20jauh\x20l','di\x20bijak\x20s','ike\x20Gecko)','Saya\x20tidak','\x20Chrome/85','999999999','ndai.','kerasukan,','groups','tuk\x20orang-','Siapa\x20nama','🐣\x20Title\x20:\x20','YqOKp','tua\x20saya,\x20','t\x20seharian','\x20Fisika.\x20L','ytmp4doc','rat.\x20Kau\x20t','teriak\x20\x22\x20a','u\x20bot\x20sila','i\x20%batre\x20k','rimu\x20jaket','ge\x20makerme','\x0a5.\x20Selesa','buk\x20belaja','\x20mantan\x20pa','fox/45.0','Juli','ini,\x20Tuhan','🏅\x20Source\x20s','*🌍\x20Type\x20:*','sasuke','t.com/','ot\x20contain','following','kepada\x20mer','kan\x20berpas','\x20want\x20to\x20d','HdYEc','upmenu','mu\x20adalah\x20','ZHcEp','ORvDg','e\x20the\x20soun','\x20Dan\x20sekar','🐺\x20Link\x20:\x20','en?type=tr','getDate','jimin','DISABLE\x20VE','n,\x20kalau\x20k','smug','FUveb','\x0a\x0a_For\x20the','sage\x20menu','\x1b[1;31m~\x1b[','e..._','97d8a.jpg','Belas\x20kasi','spam','Scan\x20Barco','rsion/10.1','itu\x20tai,\x20j','NOTE\x20！','ebuah\x20impi','i\x20apapun\x20y','lisa','ah\x20satu\x20an','japan','ZaXrQ','ptqsO','Donasi\x20💰','ge\x20upmenu','an\x20bahagia','☠️\x20Owner\x20','tahu\x20kunci','tan\x20terbes','HP-mu.\x0a2.\x20','n/x-www-fo','unya\x20dan\x20m','\x20masa\x20depa','~\x20@','includes','\x20orang\x20yan','ile/6941a5','\x20karena\x20ha','https://ti','fFVrc','https://ap','quotesmenu','alahkan\x20or','anna','🌀\x20*Link\x20:*','1\x20Safari/5','sZNmw','n\x20segalany','engan\x20ada\x20','y_form.cgi','.7,ms;q=0.','funmenu','c/zeeoneof','masih\x20di\x20i','esponse','length','empurna,\x20j','nUUfb','enis\x20yang\x20','ria,\x20beri\x20','_\x0a•>\x20Nomor','h\x20list\x20men','ang\x20yang\x20b','saja\x20dapat','up\x20saya','productMes','“Semakin\x20b','formattedS','buttonsRes','AAAAAAAANR','uga?','PLnIG','le\x20:\x20','noteks','\x20nih\x20video','Minggu','ah\x20ga\x20sola','IwCrs','UhrjP','agemenu\x20co','k\x20sebelah\x20','DANA\x20via\x20H','eninggal\x20s','faXYU','VALNf','atu\x20saja.','k\x20ada\x20manu','amu,\x20terus','ku\x20mungkin','results','dari\x20yang\x20','ah\x20mengert','\x20dapatkan\x20','FnjNN','perasaan\x20c','apa?','quBur','btoDg','Tiktok\x20Wit','lose',';q=0.8,app','kelihatan\x20','syifa','Meskipun,\x20','lpWEW','cavTp','woof','MqzHh','kiss','menjamet','\x20tetangga,','an\x20putra\x20a','JcjiP','xt/javascr','iam\x20cepiri','ge\x20telegra','paling\x20tep','Emoji-What','MJfYr','et-fu/prox','serializeM','Sebutkan\x20k','dah.\x20Biar.','api/ejecte','n\x20bilang\x20\x22','aYMGS','e-6','DMTEw','BY\x20ZEEONE\x20','emenu\x20comm','del','*🔗\x20Url\x20:*\x20','KoMSP','5.\x20Selesai','⋮☰\x20BACK','kos.life/a','rsYyD','HrqpF','uk\x20mengger','Lebih\x20baik','sukai\x20diam','idupan\x20kit','BRWCO','\x20ke\x20depan.','ixels-','\x20」----*\x0a\x09\x09','\x20nila\x20seti','✓\x20OWNER','TENza','ssoiV','💋\x20*Fullnam','i\x20pertama.','s\x20duluan\x20s','g\x20terbaik\x20','e\x20the\x20quot','yang\x20dibut','\x20diam?','rjYJz','-PLAY\x20MP4*','\x20impianmu\x20','lse&text=','\x20ss\x20drop\x20k','Satu\x20jam\x20y','Ada\x20tips\x20a','ga”','inan\x20untuk','\x20duration\x20','berlatih?”','4W5fYrjbea','ah\x20pengorb','k\x20bicara\x20d','aja\x20membia','SfBOF','INQUIRY','jid','ral','RDPuv','snap/post\x20','asil\x20Didap','t\x20diputusk','gOnYd','apa','sakan\x20saki','reka\x20menja','34461johnPm','NnUbE','mediaType','\x20yaitu\x20pas','jopdG','-beratnya\x20','Pilih\x20medi','&color=blu','a\x20ikut\x20ter','“Berlatih\x20','embayaran\x0a','i.\x20Makanya','_Tunggu\x20be','nah\x20buatmu','i\x20memegang','pa\x20aja\x20dit','.\x20Valery','di\x20sini','6288279575','a4639.jpg','idup\x20adala','b5e33a30ee','displayTex','pe\x20pasanga','in\x20banyak\x20','n\x20aku\x20yang','\x20merasa\x20ce','prank\x20chat','sa\x20ringan\x20','thumbnail','cuma\x20menci','nuxSJ','downloadme','pala.','an.”','ardianto','Emoji-JoyP','a!\x20Upgrade','kzajJ','t,\x20bagaima','t-appstate','semua\x20cint','a\x20Aplikasi','\x20ini\x20adala','.\x20Sedangka','Dik\x20jangan','e\x20the\x20imag','Button\x20mes','Linknya?','📍\x20Nama\x20:\x20@','“Saat\x20kamu','yVoXk','selectedDi','t\x20media\x20ak','same-origi','%2C%22no_f','e/apng,*/*','ku\x20masih\x20s','mangayutri','cors','begitulah\x20','xcsez','sapp.net','*??️\x20YOUTUBE','nya\x20takut\x20','mangat\x20ada','ayu','🔖\x20List\x20Sew','ita\x20adalah','ali\x20dan\x20ca','tweets','foot','ge\x20image\x20e','i,\x20mendidi','\x20berusaha.','i.twitter.','authorizat','clnhM','videos','huangzitao','s:\x20','danya.\x20Bal','▶️\x20*Ext*\x20:\x20','leCAi','u\x20ubah\x20jad','&raw=7','upport\x20but','xTH6oLd8','want\x20to\x20us','rah\x20al-kau','sIQFq','rol','eHeight=50'];_0x69ea=function(){return _0x16d3f3;};return _0x69ea();}const alphaapi='https://ap'+_0x1da9f3(0x794,0x785,0xc00,0xbde)+_0x1da9f3(0x237,0x286,0x258,-0x1ad)+_0x3f34aa(0x68b,0xcc,0x58a,0x91),alphakey=_0x1da9f3(0x51c,0x86a,0x2f4,0x9b5),reply=_0x28a444=>{const _0x545ffe={};function _0x419407(_0x274ec0,_0x3c19da,_0x500bce,_0x3cd8c2){return _0x3f34aa(_0x274ec0-0x1d5,_0x500bce,_0x3c19da- -0x111,_0x3cd8c2-0x124);}function _0x200db6(_0x14e59f,_0x17bfda,_0x50cdb3,_0x2cdd3a){return _0x1da9f3(_0x14e59f- -0x120,_0x2cdd3a,_0x50cdb3-0xb0,_0x2cdd3a-0x1dc);}_0x545ffe['wdFbg']=_0x200db6(0x894,0xd67,0x5a0,0x48b);const _0x430af4=_0x545ffe;alpha[_0x200db6(0xfd,0x51e,0xc2,0x4a)+'e'](from,_0x28a444,text,{'quoted':mek,'contextInfo':{'externalAdReply':{'title':''+ucapannya2,'body':''+baper,'previewType':_0x430af4[_0x419407(-0x2f8,0x256,0x262,0x3c8)],'thumbnailUrl':'','thumbnail':fs[_0x419407(0xa3,0x0,-0x4c0,-0x306)+'nc'](_0x419407(0x4cd,0x251,0x113,0x1c4)+thumbnail),'sourceUrl':''+apiku}}});};if(!isGroup&&!isCmd&&!command&&!mek[_0x3f34aa(0x725,0x6b7,0xa89,0xec4)][_0x1da9f3(0x7b3,0x2bc,0x785,0xcba)]&&!autorespon){simi=await fetchJson('https://ap'+_0x3f34aa(-0xc,0x7a7,0x305,0x391)+_0x3f34aa(0x654,-0x8b,0x3af,0x539)+'=ind&cf=fa'+_0x3f34aa(0x677,0x710,0x997,0xb17)+cmd),sami=simi[_0x1da9f3(0x7cc,0x78c,0x296,0x62a)];const _0x1914ab={};_0x1914ab['forwarding'+_0x3f34aa(0xa1f,0x588,0x5b3,0x428)]=0x1fc,_0x1914ab[_0x3f34aa(0x634,0xa92,0x54f,0x4cc)+'d']=!![],alpha[_0x3f34aa(0x179,0x135,0x69,0xe1)+'e'](from,'_'+sami+'_',text,{'thumbnail':fs[_0x1da9f3(0x2c5,-0x248,-0x19d,-0x14f)+'nc'](_0x3f34aa(0x4e9,0x1d,0xa1,-0x19c)+thumbnail),'sendEphemeral':!![],'quoted':mek,'contextInfo':_0x1914ab});}const sendButDoc=(_0x515d8f,_0x24808f,_0x546cee,_0x14a493,_0x465f56,_0x83062a)=>{const _0x210b85={};_0x210b85[_0x53fd77(0x325,-0xdb,0x871,0x467)]=_0x53fd77(0x47a,0xa8c,0x958,0x543)+_0x27c829(0x4b7,0x67e,0xac1,0x1b8)+_0x53fd77(0x91d,0x9a5,-0x20,0x4f6),_0x210b85['DUqUB']=_0x53fd77(0x9cf,0x6ae,0x209,0x543)+_0x53fd77(-0x75,0x5c6,-0x32b,0x7d)+_0x53fd77(0x6fe,0x5e6,0x170,0x324),_0x210b85['IqLmi']='https://yo'+'utu.be/POj'+_0x27c829(0x41c,0x456,0x56,0xd8),_0x210b85['ybzzw']=function(_0x250998,_0x55e21c){return _0x250998*_0x55e21c;},_0x210b85['ezaud']=_0x53fd77(0x27c,0x816,0x8b9,0x3a7)+_0x27c829(0xc0a,0x8a1,0xc1a,0x7cb)+'xmlformats'+'-officedoc'+_0x53fd77(0x84c,0xa0a,0x384,0x5eb)+_0x27c829(0xefd,0xb84,0x6ef,0xc51)+_0x53fd77(0x674,0x16,-0x45,0x206),_0x210b85[_0x27c829(0x9e6,0x99e,0x7da,0x5ac)]=_0x27c829(0xb77,0x746,0x783,0xaba)+_0x27c829(0xd9e,0x8a1,0x62b,0xdd8)+_0x53fd77(0x28c,0x5f7,-0x2b1,0x248)+'-officedoc'+_0x53fd77(-0x2fc,-0x303,0x3a8,0x1b1)+_0x53fd77(0x2ac,0x582,0x9c0,0x7c4)+'.presentat'+_0x27c829(0xa9c,0x6a7,0x499,0xad3),_0x210b85['KLDhM']=function(_0x284e4a,_0x2e0bb3){return _0x284e4a*_0x2e0bb3;},_0x210b85['gtejF']=_0x27c829(0x86d,0x7c1,0x739,0x3b3)+'u',_0x210b85[_0x53fd77(0x5ad,0x7e9,0x6b1,0xa32)]='owner',_0x210b85[_0x27c829(0x854,0x5d3,0x43e,0x81c)]=_0x27c829(0xcb,0x599,0x330,0x203),_0x210b85[_0x27c829(0xd73,0x8df,0x854,0x8b8)]=_0x27c829(0x893,0x820,0x999,0x53b)+_0x53fd77(-0x1a9,0x40a,0x15e,0x308),_0x210b85[_0x27c829(0x8ef,0x8da,0xb6f,0x51d)]=_0x53fd77(-0x4c,-0x12,0x24e,0x39b)+_0x27c829(0xa33,0x8c0,0x68c,0x45a)+'.net/d/f/A'+_0x53fd77(0xd1,0x3cd,0x41c,0x2f6)+_0x53fd77(-0x533,-0x4b5,-0x85,0x17)+_0x53fd77(0x5d3,0x79,0x155,0x225)+_0x27c829(0xaf5,0x5ea,0x51c,0x38a)+_0x53fd77(0x3f3,0xaa4,0x3e0,0x615),_0x210b85['HJSAa']=_0x53fd77(0x673,0x16d,0x906,0x6ae)+'D3MAaYx15D'+_0x27c829(0x48f,0x61c,0xa60,0x662)+_0x53fd77(0x770,0x528,0x2f9,0x515)+'tpE=',_0x210b85[_0x53fd77(0x6dc,0xabc,0xcc4,0x9f2)]='1000000000'+_0x27c829(0x381,0x8be,0xbdf,0x75a),_0x210b85[_0x27c829(0x1032,0xb01,0xd68,0x89f)]=_0x53fd77(0x84,0x88d,0xa6a,0x55e)+_0x53fd77(0x825,0x356,0x193,0x602)+_0x27c829(0xd16,0x81a,0x568,0x8ee)+_0x27c829(-0x24,0x4a2,0x3f5,0x7b8)+_0x27c829(0xa74,0x798,0xca8,0xce1),_0x210b85[_0x27c829(0x4dd,0x5d0,0x15d,0x520)]='bx/mFEB8SW'+'YMsYMtnlnb'+_0x53fd77(0x750,0xa08,0x722,0x72c)+_0x27c829(0x614,0x4b8,0x4d,-0x5e)+_0x27c829(0x917,0x6e4,0x810,0xa92);const _0x218e6f=_0x210b85,_0x41cf0e=[_0x218e6f[_0x53fd77(0x207,0x332,-0xaa,0x467)],_0x27c829(0xc1a,0x8e2,0xb78,0x4f7)+_0x53fd77(0x352,0xc9b,0x8ab,0x850)+_0x27c829(0xc53,0x7fb,0xccf,0x44c),_0x218e6f[_0x53fd77(0x24b,-0x2e6,0x26c,0x169)],_0x218e6f[_0x53fd77(0x12a,0x331,0x8b3,0x3f3)]],_0x43e817=_0x41cf0e[Math[_0x53fd77(0x880,0x3e9,-0x1e4,0x36f)](_0x218e6f['ybzzw'](Math['random'](),_0x41cf0e[_0x53fd77(0xa26,0xb86,0x6fe,0x8d1)]))],_0x396929=[_0x27c829(0xb5c,0x746,0x954,0xb5c)+'n/pdf',_0x218e6f[_0x53fd77(0x335,-0x3f0,-0xda,0x145)],_0x53fd77(0x23c,0x1b8,0xb8,0x3a7)+_0x27c829(0xca6,0x8a1,0xadf,0xa0b)+'xmlformats'+_0x53fd77(0x42a,0x755,-0xc1,0x2bf)+_0x53fd77(0x2bc,0x6c3,0x6ce,0x537)+_0x53fd77(0x1e,0x19f,0x97,-0x39)+_0x27c829(0xb5d,0x659,0x9f4,0x529)+'t',_0x218e6f[_0x53fd77(0x861,0x325,0x77c,0x5ff)]],_0x1d6cef=_0x396929[Math[_0x27c829(0x26f,0x70e,0x836,0x95f)](_0x218e6f[_0x53fd77(-0x25e,0x467,-0x3ac,0x17f)](Math['random'](),_0x396929[_0x27c829(0xb14,0xc70,0xc2a,0xec6)]))],_0x29f019={};_0x29f019[_0x27c829(0x96f,0xd07,0x895,0x1193)+'t']=_0x218e6f['gtejF'];const _0x1d444a={};_0x1d444a[_0x53fd77(0x39e,0x94,-0x108,0x2b2)]='Command',_0x1d444a[_0x27c829(0x9f4,0xa31,0xd81,0xb68)]=_0x29f019,_0x1d444a['type']=0x1;const _0x1e2118={};_0x1e2118[_0x53fd77(0x5b1,0x527,0x41f,0x968)+'t']='🙍‍♂️\x20Owner';const _0x48cab8={};_0x48cab8['buttonId']=_0x218e6f[_0x27c829(0x11ea,0xdd1,0xe32,0x1328)],_0x48cab8[_0x27c829(0x809,0xa31,0xaf6,0x5a5)]=_0x1e2118,_0x48cab8[_0x27c829(0x7d2,0x9d5,0xd75,0x63e)]=0x1;const _0x5262ce={};function _0x27c829(_0x17f962,_0x353ee1,_0x127573,_0x4d765f){return _0x1da9f3(_0x353ee1-0x18e,_0x127573,_0x127573-0xf5,_0x4d765f-0xfd);}function _0x53fd77(_0x5471f0,_0x21be5b,_0x643c89,_0x492037){return _0x1da9f3(_0x492037- -0x211,_0x643c89,_0x643c89-0xb5,_0x492037-0xdb);}_0x5262ce['buttonId']=_0x218e6f[_0x27c829(0x900,0x5d3,0x11d,0x753)],_0x5262ce[_0x27c829(0x806,0xa31,0xd64,0xc9b)]={},_0x5262ce[_0x53fd77(0x947,0x7bd,0x1b5,0x636)]=0x1,_0x5262ce[_0x27c829(0x806,0xa31,0xd64,0xc9b)][_0x27c829(0xcf3,0xd07,0xc93,0xf6d)+'t']=_0x218e6f[_0x27c829(0xa6b,0x8df,0xd1c,0x4d0)];const _0x4147fa=[_0x1d444a,_0x48cab8,_0x5262ce],_0x37af5a={};_0x37af5a[_0x53fd77(-0x96,0x73a,0x5dd,0x29c)]=_0x218e6f[_0x27c829(0xc16,0x8da,0xc68,0xdde)],_0x37af5a[_0x27c829(0x1066,0xd78,0x10b9,0xae2)]=_0x1d6cef,_0x37af5a['fileSha256']=_0x218e6f[_0x53fd77(0x54f,0x247,0x38,0x47d)],_0x37af5a['fileLength']=_0x218e6f[_0x53fd77(0xe49,0x53b,0x567,0x9f2)],_0x37af5a[_0x53fd77(0xa0d,0x9b4,0xc9,0x611)]=0x3e7,_0x37af5a[_0x27c829(0x868,0x50a,0x26f,0x3f3)]=_0x218e6f[_0x53fd77(0x53d,0x4de,0x2bc,0x762)],_0x37af5a[_0x27c829(0xf2a,0xbbf,0xef9,0x945)]=botname+_0x27c829(0xb1c,0x621,0x8f8,0xb05)+ownername,_0x37af5a[_0x53fd77(0xc23,0x4ff,0x6b3,0x82c)+_0x27c829(0xa18,0x866,0x37c,0x9b9)]=_0x218e6f[_0x27c829(0x2df,0x5d0,0x5b6,0x838)],_0x37af5a[_0x27c829(0x560,0x6f6,0x386,0x6db)+_0x53fd77(0x330,0x1fe,0x8f,-0x51)]=pp_bot2;const _0x3de4e9={};_0x3de4e9[_0x53fd77(0x142,0x5d5,0x6e,0x250)+'t']=_0x24808f,_0x3de4e9[_0x27c829(0x5a0,0x541,0x851,0x22a)]=_0x546cee,_0x3de4e9[_0x53fd77(0x32c,0x443,0x5a2,0x1d1)]=_0x4147fa,_0x3de4e9['headerType']=_0x27c829(0xb36,0xa81,0x62e,0xaed),_0x3de4e9['documentMe'+_0x27c829(-0x9c,0x498,0x97,0x29b)]=_0x37af5a;const _0x234486=_0x3de4e9,_0x4b08f4={};_0x4b08f4['mediaUrl']=_0x43e817,_0x4b08f4[_0x27c829(0xa94,0xcf3,0xadb,0xf9a)]=0x2,_0x4b08f4[_0x53fd77(0xa90,0x320,0x8ab,0x5b8)]=''+ucapannya2,_0x4b08f4[_0x27c829(0xe21,0xb39,0xeeb,0x6bf)]=''+tampilTanggal,_0x4b08f4['thumbnailU'+'rl']='',_0x4b08f4[_0x27c829(0xd20,0xd0e,0x100e,0x9c6)]=pp_userz;const _0x1eaea9={};_0x1eaea9['forwarding'+_0x53fd77(0x38a,0x5f5,0x855,0x556)]=0x3b9aca00,_0x1eaea9[_0x27c829(0x83b,0x891,0x524,0xa60)+'d']=!![],_0x1eaea9['sendEpheme'+_0x53fd77(0x6e0,0xd8a,0x662,0x949)]=!![],_0x1eaea9['mentionedJ'+'id']=[_0x14a493,_0x465f56,_0x83062a],_0x1eaea9['externalAd'+'Reply']=_0x4b08f4;const _0x53123c={};_0x53123c[_0x53fd77(0x265,-0x2b8,-0x7f,0x18e)+'o']=_0x1eaea9,_0x53123c[_0x53fd77(0x851,0x896,0x678,0x75c)]=fgif,_0x53123c[_0x27c829(0x97a,0xbe9,0xbf1,0xb75)+_0x27c829(0xcdc,0xce8,0x9d1,0xbc8)]=!![],alpha[_0x53fd77(-0xd5,0x3f0,0xee,0xc)+'e'](_0x515d8f,_0x234486,MessageType['buttonsMes'+_0x27c829(0xb9a,0xbb3,0xa60,0x1001)],_0x53123c);},sendButLocation=async(_0x451302,_0x283698,_0x1eae72,_0x5a1cd4,_0x4844d5=[],_0x1f583c={})=>{kma=_0x5a1cd4;const _0x308888=await alpha[_0x190b6f(0xd2a,0x7c4,0xace,0xb27)+'sage'](_0x451302,kma,MessageType['location'],{'thumbnail':kma});mhan=_0x308888[_0x3729c7(0xb5e,0xd4f,0x86e,0xf2c)]['ephemeralM'+_0x3729c7(0xf10,0xd64,0x9a8,0x96c)]?_0x308888[_0x190b6f(0xa27,0xc16,0x737,0x85b)][_0x3729c7(0xe91,0xbad,0xc61,0x9d8)+_0x3729c7(0xcda,0xd64,0xd52,0x1035)]:_0x308888;const _0x4da15f={};_0x4da15f[_0x3729c7(0x82a,0x98a,0xd26,0xc27)+'ssage']=mhan[_0x190b6f(0xb47,0x579,0xc44,0x85b)][_0x3729c7(0x842,0x98a,0x5fe,0x8af)+_0x190b6f(-0x38,0x28e,-0x134,0x1d9)],_0x4da15f[_0x190b6f(-0x87,0x6a3,-0x200,0x330)+'t']=_0x283698,_0x4da15f['footerText']=_0x1eae72,_0x4da15f[_0x3729c7(0xbaf,0x7a5,0x7c1,0x276)]=_0x4844d5,_0x4da15f[_0x190b6f(-0x16e,0x38f,0x3fc,0x110)]=0x6;const _0x11e6d9=_0x4da15f;function _0x190b6f(_0x4b5517,_0x130829,_0x418568,_0x458fd5){return _0x1da9f3(_0x458fd5- -0x131,_0x418568,_0x418568-0x1b,_0x458fd5-0xe8);}function _0x3729c7(_0x5dc901,_0x18dc14,_0x53e96f,_0x11b6f7){return _0x1da9f3(_0x18dc14-0x3c3,_0x11b6f7,_0x53e96f-0x88,_0x11b6f7-0x1ba);}alpha[_0x3729c7(0x39c,0x5e0,0xa3c,0x27d)+'e'](_0x451302,_0x11e6d9,MessageType['buttonsMes'+_0x3729c7(0xf7c,0xde8,0x113f,0xd66)],_0x1f583c);},sendButVideo=async(_0x4e7a41,_0xb43f1e,_0x1af45b,_0x51d4d3,_0x1782b0=[],_0x57a047={})=>{kma=_0x51d4d3;const _0x3c357c=await alpha[_0x11b95a(0x5c9,0x6f4,0xa29,0xa9e)+_0x11b95a(0x57b,0x6d5,0xc42,0x86b)](_0x4e7a41,kma,MessageType[_0x933f06(0xd00,0x1099,0x940,0xc55)]);function _0x11b95a(_0x217f59,_0x30cbf9,_0x253c39,_0x3c7ca8){return _0x3f34aa(_0x217f59-0x51,_0x30cbf9,_0x3c7ca8- -0x6,_0x3c7ca8-0xba);}mhan=_0x3c357c['message']['ephemeralM'+_0x933f06(0x10ba,0xa9b,0x95a,0xbec)]?_0x3c357c['message']['ephemeralM'+_0x933f06(0x10c6,0xbc3,0xdaf,0xbec)]:_0x3c357c;const _0x266218={};_0x266218[_0x11b95a(-0x21a,0x410,0x527,0x16f)+'ge']=mhan[_0x933f06(0xefc,0x1077,0x8fe,0xbd7)][_0x933f06(0xed,0x5aa,0x596,0x574)+'ge'],_0x266218['contentTex'+'t']=_0xb43f1e,_0x266218['footerText']=_0x1af45b,_0x266218['buttons']=_0x1782b0;function _0x933f06(_0x5a4ebe,_0x37f9d6,_0x27d67b,_0x2d4bcb){return _0x1da9f3(_0x2d4bcb-0x24b,_0x37f9d6,_0x27d67b-0x58,_0x2d4bcb-0x3b);}_0x266218['headerType']=0x5;const _0x3a0fd1=_0x266218;alpha[_0x933f06(0x99a,0x8a3,0x7cf,0x468)+'e'](_0x4e7a41,_0x3a0fd1,MessageType[_0x11b95a(0x958,-0xe8,0x635,0x42b)+_0x933f06(0xb39,0xede,0x119a,0xc70)],_0x57a047);},sendButImage=async(_0x217239,_0x274eed,_0x55183b,_0x230c58,_0x466f29=[],_0x5631aa={})=>{const _0x6ab1c7={};_0x6ab1c7[_0x290246(0x3ca,0x809,0x6b1,0x490)]=_0x41c9db(0x6be,0xc8c,0xa35,0x63e)+_0x41c9db(0xaca,0xef4,0xbec,0xfc9);const _0x50fa91=_0x6ab1c7;kma=_0x230c58;const _0x3fcf8d=await alpha[_0x41c9db(0xc13,0x11ae,0xea3,0xced)+_0x290246(0x9ce,0xd16,0x9e4,0x47f)](_0x217239,kma,MessageType[_0x290246(0x6f4,0x983,0x87e,0xafe)],{'thumbnail':Buffer['alloc'](0x37d*0x7+0x8ed+0x308*-0xb)});mhan=_0x3fcf8d[_0x290246(0x935,0x99f,0x890,0xc0b)][_0x50fa91[_0x41c9db(0x76a,0xb08,0x66c,0x7ca)]]?_0x3fcf8d[_0x290246(0x935,0x769,0x67a,0xb8d)][_0x290246(0x793,0x6ce,0xa1f,0xa7a)+'essage']:_0x3fcf8d;function _0x290246(_0x31cb84,_0x1f2a5a,_0x3905eb,_0x1c17ca){return _0x3f34aa(_0x31cb84-0xe3,_0x3905eb,_0x31cb84-0x15d,_0x1c17ca-0xe2);}const _0x310325={};_0x310325[_0x290246(0x6a7,0x279,0x8d8,0x63e)+'ge']=mhan[_0x41c9db(0x8b4,0xfbf,0xbd7,0x111c)][_0x41c9db(0x9fe,0xc73,0x949,0x50a)+'ge'];function _0x41c9db(_0x2d78a9,_0x5c7778,_0xc6f3f6,_0x397569){return _0x1da9f3(_0xc6f3f6-0x24b,_0x5c7778,_0xc6f3f6-0x2f,_0x397569-0x1);}_0x310325['contentTex'+'t']=_0x274eed,_0x310325[_0x290246(0x35c,0x224,0xd0,0x3ad)]=_0x55183b,_0x310325[_0x41c9db(0xf6,0x52d,0x62d,0x148)]=_0x466f29,_0x310325[_0x290246(0x1ea,0x536,0x1ad,0x111)]=0x4;const _0x5a6566=_0x310325;alpha[_0x290246(0x1c6,0x35b,-0x364,0x4fa)+'e'](_0x217239,_0x5a6566,MessageType[_0x290246(0x58e,0x3c3,0x46b,0x857)+'sage'],_0x5631aa);};if(autoregister){if(autoregister===![])return;const _0x26cec2={};_0x26cec2[_0x1da9f3(0xb79,0xc23,0xf13,0x1098)+'t']=_0x1da9f3(0x462,0x728,0x749,0x37b);const _0x4e6c7b={};_0x4e6c7b['buttonId']=_0x1da9f3(0x96a,0xb08,0xc35,0xb52),_0x4e6c7b[_0x3f34aa(0x339,0x442,0x6ef,0x405)]=_0x26cec2,_0x4e6c7b['type']=_0x1da9f3(0xa00,0xc68,0xe35,0x8cb);const _0x21501a={};_0x21501a[_0x1da9f3(0xb79,0x9db,0x6ff,0x71b)+'t']=_0x1da9f3(0xaac,0xd99,0x8bf,0x6c5)+_0x1da9f3(0x728,0xbd4,0x983,0x2d0);const _0x24f2c7={};_0x24f2c7['buttonId']='register\x20o'+'ff',_0x24f2c7[_0x1da9f3(0x8a3,0x5f6,0xc53,0xde7)]=_0x21501a,_0x24f2c7[_0x1da9f3(0x847,0xc0b,0x8a9,0xd1e)]=_0x1da9f3(0xa00,0x7a5,0x5ed,0xaed);let gaklopo=[_0x4e6c7b,_0x24f2c7];if(isCmd&&!mek[_0x1da9f3(0xc3d,0xe67,0x80c,0xc46)][_0x3f34aa(0x53d,0x61f,0x5ff,0x211)]&&!isOwner&&!isCreator&&!isRegister)return sendButMessage(from,_0x3f34aa(-0xff,-0x35a,0x1d7,0x144)+sender[_0x3f34aa(0xa48,0xc3c,0x7c7,0x37b)]('@')[-0xc7a+-0x819+0x1493]+('\x20kamu\x20belu'+_0x1da9f3(0x619,0x9fb,0x125,0x482)+_0x3f34aa(-0x2a6,-0x15a,0x7a,-0x2bd)+_0x1da9f3(0x955,0x6b6,0x8c4,0xe26)+_0x1da9f3(0xa3f,0xf22,0xb8f,0x896)+'verifikasi'+_0x1da9f3(0x85c,0xa3d,0xd40,0x550)+'\x20terlebih\x20'+_0x1da9f3(0x2a0,0x6f1,-0x4d,-0xaa)+_0x3f34aa(0x694,0x5a0,0xa5c,0xd9c)+_0x1da9f3(0x512,0x507,0x1f0,0xef)+_0x3f34aa(0xcd7,0xb4d,0xa32,0x5cd)+_0x3f34aa(0x457,0x504,0x44c,0x841)+_0x3f34aa(-0xc8,0x47c,0x118,-0x2d2)),'©\x20'+ownername,gaklopo,{'quoted':fgclink2,'contextInfo':{'mentionedJid':[sender]}});}const sendTroli=async _0x4af062=>{const _0x5688e2={};_0x5688e2[_0x13c955(-0x29d,0x4c4,0x198,0x37a)]=_0x41b1fd(0x928,0x526,0xde3,0xcad)+'00587',_0x5688e2[_0x41b1fd(0xa02,0xa63,0x627,0x645)]=_0x41b1fd(0xbbd,0x834,0xe9b,0xa71);const _0x52a2c7=_0x5688e2;let _0xefaabf=fs['readFileSy'+'nc'](_0x41b1fd(0x57b,0x108,0x440,0x1a2)+thumbnail);imeu=await alpha['prepareMes'+_0x13c955(0x5f0,0x5bc,0x913,0xad3)](_0x41b1fd(0x4a0,0x42,0x8a2,0x3c5)+_0x13c955(0x93d,0xc50,0x856,0x432),_0xefaabf,image);function _0x13c955(_0x4704ec,_0x2b3330,_0x3a86d8,_0x1cb9d3){return _0x1da9f3(_0x3a86d8- -0x112,_0x1cb9d3,_0x3a86d8-0x178,_0x1cb9d3-0xa);}imeg=imeu['message'][_0x41b1fd(0x763,0xc70,0x7db,0x4d7)+'ge'];const _0x278615={};_0x278615[_0x41b1fd(0x450,0x993,-0x10a,0x661)+_0x13c955(0x3a5,0x200,0x655,0x973)]=0x3,_0x278615[_0x41b1fd(0x768,0xae3,0x55a,0x702)+'d']=!![];function _0x41b1fd(_0x212cf5,_0x39633e,_0x17839f,_0x44e97b){return _0x1da9f3(_0x212cf5-0x65,_0x39633e,_0x17839f-0x156,_0x44e97b-0x3a);}const _0x40bdc6={};_0x40bdc6['quoted']=fgif,_0x40bdc6[_0x41b1fd(0x404,0x5c,0x2e9,0x3d4)+'o']={},res=await alpha['prepareMes'+'sageFromCo'+_0x13c955(0x937,0xaf,0x4e0,0x67b)](from,{'orderMessage':{'orderId':_0x52a2c7[_0x13c955(-0x322,-0x20b,0x198,0x5eb)],'thumbnail':fs[_0x41b1fd(0x32a,0x403,0x865,0x6b1)+'nc'](_0x13c955(0x2c8,-0xd3,0x404,-0x3a)+thumbnail),'itemCount':0x7e5,'status':_0x52a2c7[_0x13c955(0x478,0x4ea,0x88b,0x986)],'surface':_0x13c955(0x116,0x2f0,0x1d9,-0x16d),'orderTitle':_0x41b1fd(0xadb,0x6f5,0xce4,0xbda),'message':_0x4af062,'sellerJid':'6288743504'+_0x41b1fd(0x8cf,0xc18,0x9b5,0x761)+_0x41b1fd(0x403,0x85b,0x381,-0x52),'token':_0x13c955(0x8fd,0x95e,0x84a,0x985)+'4W5fYrjbea'+'kPiI3/Xxar'+'ATSeP+KLh+'+'0FGwkw==','contextInfo':_0x278615}},_0x40bdc6),alpha[_0x13c955(0xba,0x3ec,0x5d4,0x764)+_0x41b1fd(0xa8a,0xee0,0x58b,0x967)](res);},sendTroli2=async _0x49a6ab=>{const _0x33ffca={};_0x33ffca[_0x3a1dc5(0xd3d,0xe80,0xb2c,0x9a0)]=_0x1cd4ca(0xb17,0xe8b,0x722,0xa20)+'00587',_0x33ffca[_0x1cd4ca(0x765,0x6b6,0x9de,0x62f)]=_0x3a1dc5(0x9a7,0x10f5,0x1048,0xe7e),_0x33ffca[_0x1cd4ca(0xaff,0xc00,0x7ff,0x6ee)]='CATALOG',_0x33ffca[_0x3a1dc5(0xff4,0xd03,0xabe,0xe1e)]=_0x1cd4ca(0x4f3,0x9b0,0x7f0,0x397)+'7326@s.wha'+_0x1cd4ca(0x5f2,0x7ee,0x5bd,0xae4),_0x33ffca[_0x3a1dc5(0x939,0x606,0xa3e,0x954)]='AR5b5YFz2g'+_0x3a1dc5(0xaaa,0x9ef,0x1373,0xe79)+_0x3a1dc5(0x6f4,0x77b,0x6aa,0x576)+'ATSeP+KLh+'+_0x1cd4ca(0x82a,0x3ab,0x629,0x573),_0x33ffca[_0x3a1dc5(0xb38,0xe2c,0xfb8,0xeaf)]=_0x3a1dc5(0xa3d,0xee4,0xef4,0xda6);function _0x3a1dc5(_0x43b0f1,_0xaf36df,_0x59e203,_0x4dbde4){return _0x3f34aa(_0x43b0f1-0x1d5,_0x59e203,_0x4dbde4-0x4da,_0x4dbde4-0x31);}_0x33ffca[_0x1cd4ca(0xb92,0xe3b,0x6e0,0x867)]='IDR';const _0x59bfb9=_0x33ffca;function _0x1cd4ca(_0x5b2b44,_0x4693e4,_0x2bae30,_0x4bd4c4){return _0x1da9f3(_0x5b2b44-0x254,_0x2bae30,_0x2bae30-0x91,_0x4bd4c4-0x47);}let _0x58a92c=fs[_0x1cd4ca(0x519,0xa0b,0x5de,0x2b4)+'nc']('./image/'+thumbnail);imeu=await alpha['prepareMes'+'sage'](_0x1cd4ca(0x68f,0x70f,0x650,0x5b4)+'pp.net',_0x58a92c,image),imeg=imeu[_0x1cd4ca(0xbe0,0x969,0xad4,0xfa6)]['imageMessa'+'ge'];const _0x3b55bd={};_0x3b55bd[_0x3a1dc5(0x735,0x59f,0x970,0x711)+_0x3a1dc5(0x940,0xc2b,0x6b8,0xa8d)]=0x3,_0x3b55bd['isForwarde'+'d']=!![];const _0x5c69a7={};_0x5c69a7[_0x1cd4ca(0xbc1,0xd10,0xe33,0x66b)]=fgif,_0x5c69a7[_0x1cd4ca(0x5f3,0x321,0x992,0x78d)+'o']={},res=await alpha[_0x3a1dc5(0x1226,0x1246,0xc39,0xf7e)+_0x3a1dc5(0x893,0xac4,0x814,0x91c)+_0x1cd4ca(0x846,0x53d,0x2f3,0x9b0)](from,{'orderMessage':{'orderId':_0x59bfb9['Gyjld'],'thumbnail':fs[_0x3a1dc5(0x155,0x77f,0x854,0x5eb)+'nc'](_0x3a1dc5(0xd55,0x420,0xd88,0x83c)+thumbnail),'itemCount':0x7e5,'status':_0x59bfb9[_0x1cd4ca(0x765,0xbdc,0x7dc,0xa51)],'surface':_0x59bfb9[_0x1cd4ca(0xaff,0xd6f,0x822,0xc37)],'orderTitle':_0x3a1dc5(0xb95,0xd44,0x104b,0xd9c),'message':_0x49a6ab,'sellerJid':_0x59bfb9[_0x3a1dc5(0x1048,0x99e,0xbbe,0xe1e)],'token':_0x59bfb9[_0x3a1dc5(0xa70,0xdaa,0x603,0x954)],'totalAmount1000':_0x59bfb9[_0x3a1dc5(0x10f4,0x99d,0x1027,0xeaf)],'totalCurrencyCode':_0x59bfb9[_0x1cd4ca(0xb92,0x87b,0x787,0xad0)],'contextInfo':_0x3b55bd}},_0x5c69a7),alpha['relayWAMes'+'sage'](res);},sendKatalog2=async _0xfc46f3=>{const _0x1e6d17={};_0x1e6d17[_0x35c11c(0xb36,0xd9e,0xa0d,0x5e5)]=_0x35c11c(0x7e6,0xb2f,0x7ca,0x30b)+_0x26c416(0xd00,0xf7c,0x1192,0xc1a),_0x1e6d17['gOnYd']=_0x26c416(0xc2d,0xd43,0xa7e,0x114c),_0x1e6d17['aYMGS']='9199191919'+_0x35c11c(0x701,0xe74,0xbe5,0xcbf)+'1919';const _0x32bc57=_0x1e6d17;let _0x54b23c=fs[_0x35c11c(0x719,0x380,0x654,0x4ea)+'nc']('./image/'+thumbnail);imeu=await alpha['prepareMes'+_0x26c416(0xdbd,0x10c9,0x11da,0xa2b)](_0x32bc57[_0x26c416(0xa16,0x81c,0xf29,0x62e)],_0x54b23c,image,{'thumbnail':_0x54b23c}),imeg=imeu[_0x35c11c(0xc1d,0x834,0xd1b,0x9ef)][_0x26c416(0xa96,0xba3,0xa3d,0x89b)+'ge'];const _0x59049c={};function _0x26c416(_0x50a472,_0x3683fa,_0x83070c,_0x57a7c7){return _0x1da9f3(_0x50a472-0x398,_0x83070c,_0x83070c-0xda,_0x57a7c7-0x31);}_0x59049c[_0x26c416(0xb95,0xea6,0x10d1,0xf68)+'ge']=imeg,_0x59049c[_0x35c11c(0xc3d,0xc1f,0x857,0x7f8)]=_0x35c11c(0xd1a,0xce8,0xc52,0x100b)+'00587',_0x59049c['title']=_0x26c416(0x63e,0x718,0x965,0x386)+'OT',_0x59049c['descriptio'+'n']=_0xfc46f3,_0x59049c[_0x26c416(0xc5c,0x11a1,0xa59,0xe2f)+'de']=_0x32bc57[_0x35c11c(0xfb3,0xbc4,0xeee,0xfb3)],_0x59049c[_0x35c11c(0xe55,0x972,0xd03,0x90a)+_0x35c11c(0x1049,0x95e,0xb6e,0xd5f)]=_0x32bc57[_0x26c416(0xec0,0x9b2,0x131f,0xd45)],_0x59049c[_0x26c416(0xb95,0xebd,0x857,0xf24)+_0x26c416(0xad4,0x66b,0x5e7,0x894)]=0x1;const _0x2bae79={};_0x2bae79[_0x35c11c(0xa09,0xcb8,0x77a,0x9f6)+'Score']=0x3e7,_0x2bae79['isForwarde'+'d']=!![];const _0x5df74c={};function _0x35c11c(_0x438413,_0xbcfeb1,_0x424693,_0xa04aa5){return _0x1da9f3(_0x424693-0x38f,_0xbcfeb1,_0x424693-0x19f,_0xa04aa5-0x181);}_0x5df74c[_0x35c11c(0x6c3,0x51f,0x56b,0x9c8)]=_0x59049c,_0x5df74c['businessOw'+'nerJid']=_0x35c11c(0x953,0xa18,0x62e,0x254)+_0x35c11c(0x10c3,0xa11,0xbf9,0xc4c)+_0x26c416(0x736,0x487,0xc66,0x264),_0x5df74c[_0x26c416(0x737,0xa96,0x56c,0x8de)+'o']=_0x2bae79;const _0x2fc45e={};_0x2fc45e[_0x35c11c(0xcb0,0xcae,0xe7b,0xd9f)+_0x26c416(0xdbd,0x11c9,0xdf7,0x116a)]=_0x5df74c;const _0x52e25b={};_0x52e25b[_0x35c11c(0x816,0x1113,0xcfc,0x966)]=mek,res=await alpha[_0x26c416(0xff0,0xf97,0xe92,0xca9)+_0x26c416(0x98e,0x809,0xbd8,0x70f)+_0x26c416(0x98a,0x912,0xa10,0xd36)](from,_0x2fc45e,_0x52e25b),alpha[_0x35c11c(0x5c3,0xae3,0xa75,0xcfc)+_0x26c416(0xdbd,0x950,0x8be,0xefd)](res);},sendKatalog3=async _0x1ff8c4=>{const _0x3438cb={};_0x3438cb[_0x57fc95(0x4e8,0x871,0xa1,0x413)]=_0x57fc95(0x78c,0x9eb,0x3f0,0xb9b)+_0x107723(0xb7f,0x65b,0xa67,0xd04),_0x3438cb[_0x57fc95(0x77c,0x2ae,0xcae,0xb3c)]=_0x107723(0xbcf,0xb16,0xdcf,0x963),_0x3438cb[_0x107723(0xae7,0xa3a,0xbaa,0xa27)]=_0x57fc95(0x4c2,0x1c,0x8cd,0x273),_0x3438cb[_0x107723(0xf61,0xf16,0xfb8,0x1454)]=_0x107723(0x5d9,0x3da,0xb04,0x525)+_0x107723(0xba4,0x7f0,0x7b3,0xd01)+_0x57fc95(0x267,0x254,0x38,0x44a);const _0x38bd1e=_0x3438cb;let _0x4637a1=fs['readFileSy'+'nc'](_0x57fc95(0x3df,0x32c,0x344,0x1dd)+thumbnail);imeu=await alpha[_0x57fc95(0xb21,0xdd1,0x8e1,0xbb8)+_0x107723(0xd5f,0xa74,0xa6c,0xfe6)]('0@s.whatsa'+_0x107723(0xca2,0x95f,0x97f,0xf38),_0x4637a1,image,{'thumbnail':_0x4637a1}),imeg=imeu[_0x107723(0xcc6,0xc02,0x8e1,0xedd)]['imageMessa'+'ge'];const _0x95110={};_0x95110['productIma'+'ge']=imeg,_0x95110[_0x57fc95(0x391,-0x156,0x555,0x208)]=_0x38bd1e['quLxg'],_0x95110[_0x57fc95(0x692,0x7a0,0x791,0x3e3)]='ALL\x20MENU\x20B'+'OT',_0x95110[_0x57fc95(0x81d,0x8f3,0x2ce,0x5a8)+'n']=_0x1ff8c4,_0x95110[_0x57fc95(0x78d,0x56d,0xc6c,0xc8b)+'de']=_0x38bd1e[_0x57fc95(0x77c,0x265,0x2be,0x39f)],_0x95110[_0x107723(0xcae,0x1052,0xfd5,0xeba)+_0x107723(0xb19,0xfc7,0x6a5,0xce4)]=_0x38bd1e[_0x107723(0xae7,0x7b2,0xd21,0xfe8)],_0x95110[_0x57fc95(0x1eb,0x579,0x35b,0x5c)+_0x57fc95(0x2f6,0x80a,0x598,0x7e8)]='100000',_0x95110['productIma'+'geCount']=0x1;function _0x107723(_0x26f6ea,_0x355da2,_0x4509ad,_0x370fe0){return _0x3f34aa(_0x26f6ea-0x47,_0x370fe0,_0x26f6ea-0x4ee,_0x370fe0-0x1dc);}const _0x5aa8a6={};_0x5aa8a6[_0x107723(0x725,0x8df,0xaca,0x591)+_0x57fc95(0x630,0x3cd,0x8c1,0xac4)]=0x3e7,_0x5aa8a6[_0x107723(0xa3d,0x9c8,0x649,0xbd9)+'d']=!![];const _0x4e6fba={};_0x4e6fba['product']=_0x95110,_0x4e6fba[_0x57fc95(0x4ce,0x6f,0x2e7,0x1aa)+_0x107723(0x56f,0x91d,0x4ff,0xa0c)]=_0x38bd1e[_0x107723(0xf61,0xfb9,0xbc5,0xa1b)],_0x4e6fba[_0x107723(0x6d9,0x7bd,0x590,0x960)+'o']=_0x5aa8a6;const _0x316f78={};_0x316f78['productMes'+_0x107723(0xd5f,0xd69,0xb60,0xc9a)]=_0x4e6fba;const _0x22c368={};_0x22c368[_0x57fc95(0x836,0xcdf,0x512,0x36e)]=mek;function _0x57fc95(_0x5c1050,_0x30509a,_0x3715d3,_0x27189b){return _0x1da9f3(_0x5c1050- -0x137,_0x3715d3,_0x3715d3-0x16c,_0x27189b-0xd9);}res=await alpha[_0x107723(0xf92,0xe3d,0xa7d,0xc71)+_0x107723(0x930,0x7d8,0x48e,0x7ce)+_0x57fc95(0x4bb,0x4d4,0x28,0x806)](from,_0x316f78,_0x22c368),alpha['relayWAMes'+'sage'](res);},sendKatalog=async(_0xa777d7,_0xec2d83,_0x119d22)=>{const _0x86ae64={};_0x86ae64['ISuQP']=_0xbd0804(0x550,0x321,0x9b4,0x7cd)+_0x21a4cc(0x678,0x274,0x93b,0x693),_0x86ae64[_0x21a4cc(0x22e,0x37f,-0x5d4,-0x107)]=_0xbd0804(0x951,0xd41,0x79d,0xc55)+_0xbd0804(0xed9,0x84e,0xe2e,0xbd7),_0x86ae64[_0x21a4cc(0x487,-0x16a,0x103,0xb8)]=_0x21a4cc(0x892,0xa40,0x806,0x5c0),_0x86ae64[_0x21a4cc(0x5b4,0xb05,0xab9,0x793)]=_0xbd0804(0x10e5,0xd09,0xe74,0xd4b),_0x86ae64['cTCgo']=_0xbd0804(0x587,0x9f7,0xad3,0x631)+_0xbd0804(0x85e,0xb2b,0x1084,0xbfc)+_0x21a4cc(-0x13c,0x87,0x143,0xc9);const _0x24647b=_0x86ae64;let _0x3c3607=fs['readFileSy'+'nc'](_0x21a4cc(0x189,-0x143,-0x14,0x241)+thumbnail);imeu=await alpha['prepareMes'+_0x21a4cc(0xa85,0x349,0x707,0x750)](_0x24647b[_0x21a4cc(-0x349,0x390,-0x378,0x186)],_0x3c3607,image,{'thumbnail':_0x3c3607});function _0xbd0804(_0x2703d8,_0x1096a8,_0x554984,_0x2a92b0){return _0x1da9f3(_0x2a92b0-0x392,_0x554984,_0x554984-0xc6,_0x2a92b0-0x132);}imeg=imeu[_0x21a4cc(0x6be,0x2f8,0x474,0x6b7)][_0x21a4cc(0x86c,0x5ab,0x8e1,0x429)+'ge'];function _0x21a4cc(_0x40e54c,_0x50da03,_0x2ba8e4,_0x64b5e7){return _0x1da9f3(_0x64b5e7- -0x2d5,_0x2ba8e4,_0x2ba8e4-0xb4,_0x64b5e7-0xa8);}const _0x230ed6={};_0x230ed6[_0x21a4cc(-0x1b,0x881,0x329,0x528)+'ge']=imeg,_0x230ed6['productId']=_0x24647b[_0xbd0804(0x7b,0x7fe,0x394,0x560)],_0x230ed6['title']=_0xa777d7,_0x230ed6[_0x21a4cc(0xa3a,0x358,0xa54,0x67f)+'n']=_0x119d22,_0x230ed6[_0x21a4cc(0x1a7,0xafb,0x37f,0x5ef)+'de']=_0x24647b[_0xbd0804(0x91b,0xa47,0x777,0x71f)],_0x230ed6['priceAmoun'+_0xbd0804(0xde6,0x731,0xb0c,0xb71)]=_0xec2d83,_0x230ed6[_0x21a4cc(0x4e3,0x36a,0x189,0x4d)+'mount1000']=_0x24647b[_0x21a4cc(0xc33,0x3c4,0xc4d,0x793)],_0x230ed6[_0xbd0804(0xadc,0xdde,0x657,0xb8f)+_0x21a4cc(0x2b8,0x6f9,0x167,0x467)]=0x1;const _0x4d205e={};_0x4d205e[_0x21a4cc(0x194,0x4c5,-0x3d6,0x116)+_0xbd0804(0x8a2,0xd9d,0xa02,0xaf9)]=0x3,_0x4d205e[_0x21a4cc(0x7db,0x68c,0x72b,0x42e)+'d']=!![];const _0x46cded={};_0x46cded['product']=_0x230ed6,_0x46cded[_0xbd0804(0xe0b,0xb23,0xebb,0x997)+_0x21a4cc(0x4b5,-0x349,0x1d1,-0xa0)]=_0x24647b[_0xbd0804(0x715,0xbd2,0x2e8,0x6a9)],_0x46cded[_0xbd0804(0xb48,0xb5a,0x6c6,0x731)+'o']=_0x4d205e;const _0x418d8f={};_0x418d8f['productMes'+_0xbd0804(0x10f9,0x1276,0x9e8,0xdb7)]=_0x46cded;const _0x1ae993={};_0x1ae993[_0x21a4cc(0x63e,0x18f,0x30b,0x698)]=fgif,_0x1ae993[_0x21a4cc(-0x27,0x421,-0x25,0xca)+'o']={},res=await alpha[_0x21a4cc(0x62f,0x5e8,0xe7f,0x983)+_0xbd0804(0xa43,0x667,0xe50,0x988)+_0x21a4cc(0x72,0x255,0x3d,0x31d)](from,_0x418d8f,_0x1ae993),alpha[_0x21a4cc(0x699,0x301,0x3dc,0x411)+_0x21a4cc(0x75e,0x5ff,0xc3f,0x750)](res);},sendList=async _0x212d1c=>{const _0x3d792f={};_0x3d792f[_0x398a9b(0x534,0x74d,0x43e,0x3de)]=_0xf01278(0xf65,0x83d,0xda8,0xa57)+'E',_0x3d792f[_0xf01278(0x37b,0x936,0x99a,0x72c)]=_0x398a9b(0x980,0x1ba,0x573,0x1c7)+'ECT',_0x3d792f[_0xf01278(0x91b,0xe6e,0x89c,0xb1d)]=_0xf01278(0xe65,0x7db,0x68d,0xa6b)+_0xf01278(0x1031,0x9af,0x10b5,0xee5),_0x3d792f[_0x398a9b(0xa14,0xd01,0x812,0x838)]=_0x398a9b(0xb06,0x7bf,0x60b,0x59e)+'ot',_0x3d792f['yVoXk']=_0x398a9b(0xf4d,0xf32,0xaf0,0xdf6)+'nu',_0x3d792f[_0xf01278(0xd6f,0xbd7,0x847,0x887)]=_0xf01278(0x971,0x50b,0x43e,0x436)+_0xf01278(0x1327,0x10f2,0x105d,0xe44)+_0xf01278(0x943,0xd90,0xafb,0xe3f)+_0x398a9b(0x5d3,0x747,0x6b6,0x1d2)+_0x398a9b(0x294,0x102,0x411,0x5d6)+_0x398a9b(0xdb0,0x64b,0xb8a,0x6c4),_0x3d792f[_0x398a9b(-0xc6,0x151,0x3f3,0x7b5)]='Sub\x20Menu\x20k'+_0x398a9b(0xeae,0xe33,0x960,0xb2d),_0x3d792f[_0xf01278(0x786,0x173,0x4dc,0x509)]=_0xf01278(0x710,0x43,0xd7,0x436)+_0x398a9b(0x92e,0xecd,0xb2d,0xa71)+_0xf01278(0xb92,0xee5,0x11a5,0xe3f)+_0xf01278(0x4a4,0x782,0xa45,0x8fa)+_0x398a9b(0xd3d,0xca9,0x9b0,0x59c)+'and_',_0x3d792f['YNfMS']='Sub\x20Menu\x20k'+_0xf01278(0x547,0x6b6,0x6f,0x4dd),_0x3d792f[_0xf01278(0x80b,0x389,0x34d,0x861)]=_0x398a9b(0x3fe,0x412,0x11f,0x662)+'is\x20if\x20you\x20'+'want\x20to\x20us'+_0x398a9b(0x5a4,0x768,0x7b0,0xcd4)+_0xf01278(0xc4e,0xd1a,0xffd,0xcfc)+'mmand_',_0x3d792f['RAGDs']=_0xf01278(0x6b8,0xab6,0xae5,0x9c6)+_0xf01278(0x3f8,0xabc,0x40d,0x684),_0x3d792f[_0x398a9b(0xbd4,0xae8,0xa12,0x6a5)]=_0xf01278(0x1065,0xa70,0x11df,0xd62),_0x3d792f['Kcgkf']='_choose\x20th'+_0xf01278(0xcfa,0xdfb,0xfbd,0xe44)+'want\x20to\x20us'+'e\x20the\x20funm'+_0x398a9b(0x949,0x418,0x6de,0xaed)+'d_',_0x3d792f['pBjwl']=_0x398a9b(0x9cc,0x737,0x6af,0x154)+_0xf01278(0x7c7,0x942,0xc85,0x9b0),_0x3d792f[_0x398a9b(0x1ad,0x509,0x6da,0xb93)]='makermenu',_0x3d792f['VIfGN']=_0x398a9b(0x76,-0x292,0x11f,-0x28a)+'is\x20if\x20you\x20'+'want\x20to\x20us'+'e\x20the\x20make'+_0x398a9b(0x71f,0x488,0x781,0xb06)+_0x398a9b(0x63b,0x158,0x318,0x358),_0x3d792f[_0x398a9b(0xc5b,0xe31,0xb56,0xebc)]=_0xf01278(0xd46,0x754,0xada,0x9c6)+_0xf01278(0x1253,0x8ea,0xaa1,0xdad);function _0x398a9b(_0xa77e0d,_0x3f4e8e,_0x309ff2,_0x5b1273){return _0x1da9f3(_0x309ff2- -0x93,_0x3f4e8e,_0x309ff2-0x101,_0x5b1273-0x12f);}_0x3d792f[_0x398a9b(0x28d,0xe4,0x28c,0x5d6)]=_0xf01278(-0x92,0x740,0x54a,0x436)+_0xf01278(0x1137,0x11a6,0xea1,0xe44)+_0xf01278(0x8fd,0x131e,0x996,0xe3f)+_0xf01278(0x2b7,0xf9,0x677,0x57d)+_0xf01278(0xc72,0xdf4,0x5d0,0xa98)+_0xf01278(0x1ab,0x488,0x82a,0x62f),_0x3d792f[_0x398a9b(0x891,0xa4b,0x5f8,0x6fe)]='Sub\x20Menu\x20k'+_0x398a9b(0x69,0x349,0x2ca,0x6c),_0x3d792f[_0x398a9b(0x72b,0xae8,0x5f6,0xa05)]=_0x398a9b(0xe87,0x4ad,0x98b,0xba9),_0x3d792f[_0xf01278(0x853,0x78f,0x4f8,0x852)]=_0xf01278(0x550,0xa59,0xc49,0x9c6)+_0x398a9b(0xa88,0x91a,0x7e2,0x3ec),_0x3d792f[_0x398a9b(0x388,0x101,0x512,0x6da)]=_0xf01278(0x7ee,0x7d3,0x670,0x436)+'is\x20if\x20you\x20'+_0xf01278(0xdfb,0x1114,0xf2c,0xe3f)+_0xf01278(0x8d4,0x6df,0x8a4,0x729)+'agemenu\x20co'+'mmand_',_0x3d792f['ezmmL']='tagmenu',_0x3d792f[_0xf01278(0xad6,0x662,0x55a,0x98e)]=_0x398a9b(0x121,-0x1e0,0x11f,0xde)+_0x398a9b(0xc00,0x83a,0xb2d,0xdb9)+_0x398a9b(0x6f5,0xde4,0xb28,0x7bc)+_0x398a9b(0x5be,0x689,0x950,0xa0e)+_0x398a9b(0xa67,0x719,0x6de,0x8a1)+'d_',_0x3d792f[_0xf01278(0x688,0x604,0xa82,0x937)]=_0xf01278(0xdb9,0xb9c,0x914,0xd26),_0x3d792f[_0x398a9b(0x8d2,0xa43,0x623,0x3ae)]=_0x398a9b(0xc1,-0xe9,0x11f,0x675)+_0xf01278(0xae2,0xd35,0xe46,0xe44)+_0xf01278(0x12b8,0xf42,0xa0a,0xe3f)+_0xf01278(0xe65,0xb0d,0xc1b,0xc08)+'nu\x20command'+'_',_0x3d792f[_0x398a9b(0x8bd,0xa78,0x8a5,0xd34)]=_0xf01278(0x676,0xd8a,0x7d4,0x9c6)+_0x398a9b(0x46b,0x971,0x751,0x93b),_0x3d792f[_0x398a9b(-0xfd,0x1b6,0x151,0x286)]=_0xf01278(0x5fb,0xf9,0x5fb,0x44b)+'n',_0x3d792f[_0xf01278(0xdf7,0xad1,0xd9a,0x933)]=_0xf01278(0x48b,0x4e,-0x68,0x436)+'is\x20if\x20you\x20'+_0x398a9b(0xa57,0x683,0xb28,0x846)+'e\x20the\x20gach'+_0xf01278(0x810,0x1be,0x107,0x5f6)+_0x398a9b(0x612,0x4d7,0x5e6,0xa84),_0x3d792f['wAkFn']=_0x398a9b(0x676,0xb61,0x6af,0x5d7)+'e-12',_0x3d792f[_0x398a9b(0x6e8,-0x60,0x3ff,0x443)]='telegram\x20s'+_0xf01278(0x8bd,0xbe7,0x711,0x8a6),_0x3d792f[_0x398a9b(0x263,0x76d,0x66f,0x83f)]=_0xf01278(0xa86,0x6f2,0x9e1,0x9c6)+_0xf01278(-0xed,0x5a0,0x3a2,0x455),_0x3d792f[_0x398a9b(0x248,0x1a8,0x2fc,0x5d3)]='nsfw\x20menu',_0x3d792f[_0x398a9b(0x368,0xd1,0x3f9,-0x7c)]=_0xf01278(0x622,0xb83,0x6cb,0x9c6)+'e-14',_0x3d792f[_0x398a9b(0x787,0xcde,0xa6b,0xb5f)]='asupan\x20men'+'u',_0x3d792f[_0xf01278(0xfbb,0xadf,0x11fa,0xe49)]=_0xf01278(0x5bd,0xbf2,0xe81,0x9c6)+_0x398a9b(0x4ff,0x7d3,0x42d,0x12c),_0x3d792f[_0xf01278(0xc13,0xf0c,0x96e,0xd98)]=_0xf01278(0x931,0x485,0x57a,0x96e)+'ct',_0x3d792f[_0x398a9b(-0x1b4,0x574,0x349,0xaf)]='_choose\x20th'+'is\x20if\x20you\x20'+'want\x20to\x20us'+_0x398a9b(0x1047,0xecb,0xafe,0xfc7)+_0xf01278(0xc8b,0x8ba,0x6f8,0x80e)+_0x398a9b(0x739,0x379,0x6de,0xbc1)+'d_',_0x3d792f[_0xf01278(0x57e,0x68b,0x908,0x91c)]=_0x398a9b(0xabc,0xa9c,0x861,0xc63)+'n',_0x3d792f[_0x398a9b(0x74e,0x96b,0xaef,0x8ba)]=_0x398a9b(-0xdb,0x4b3,0x11f,0x38c)+_0xf01278(0xd84,0xfa9,0xba6,0xe44)+'want\x20to\x20us'+_0x398a9b(0x4fd,0xb05,0x97e,0x755)+_0x398a9b(0x585,0x52d,0x5e9,0x8f)+_0xf01278(0xb34,0x8c4,0x5aa,0x60e),_0x3d792f[_0xf01278(0xb0c,0x784,0xc1a,0x97b)]=_0xf01278(0xb8d,0x6a7,0x7a4,0x9c6)+_0xf01278(0xe8d,0x9ac,0x1087,0xebe),_0x3d792f[_0x398a9b(0x52f,0x7e2,0xa46,0x5ef)]=_0xf01278(0xba7,0x1052,0x882,0xd58),_0x3d792f[_0xf01278(0xe7a,0xc37,0x8d7,0x9f0)]='Sub\x20Menu\x20k'+_0xf01278(0x94d,0xa07,0x452,0x5e2),_0x3d792f[_0x398a9b(0x5a4,0x94f,0x5b5,0xacd)]=_0x398a9b(0x1e9,0x520,0x715,0x914),_0x3d792f[_0x398a9b(0x18,0x6df,0x1ec,0x5d4)]=_0x398a9b(-0x57,-0x26e,0x11f,-0xdf)+_0x398a9b(0x1060,0xb39,0xb2d,0x900)+'want\x20to\x20us'+_0x398a9b(0x387,0x52b,0x551,0x444)+_0xf01278(0xe91,0x9b2,0x108f,0xdb0)+_0x398a9b(-0x93,0x52,0x318,-0x1d5),_0x3d792f[_0x398a9b(0x3a9,0x44f,0x18c,0x3e2)]=_0xf01278(0x9c2,0xdb3,0xc2c,0x9c6)+_0xf01278(0x810,0x6c5,0xac1,0xb58),_0x3d792f[_0xf01278(0x593,0xc3e,0xdd6,0xa44)]=_0xf01278(0x972,0x512,0x6e6,0xa43)+'u',_0x3d792f[_0xf01278(0xf85,0xa34,0x1002,0xcb7)]='_choose\x20th'+'is\x20if\x20you\x20'+_0x398a9b(0x6e8,0xfbd,0xb28,0xa0b)+'e\x20the\x20text'+_0xf01278(0x9ab,0xa0e,0x5e7,0x4f5)+_0xf01278(0x756,0x6dd,0x471,0x60e),_0x3d792f[_0xf01278(0xd03,0xdf4,0xf6c,0xce6)]=_0xf01278(0xc46,0x726,0x880,0x9c6)+_0xf01278(0xfb,0x8d4,0x8c1,0x448);function _0xf01278(_0x483fc7,_0x5c8a1d,_0x19072f,_0x1bf835){return _0x3f34aa(_0x483fc7-0x21,_0x19072f,_0x1bf835-0x438,_0x1bf835-0x1ea);}_0x3d792f[_0x398a9b(0x76d,0x772,0x981,0x571)]=_0xf01278(0xec6,0x505,0x819,0x9c6)+'e-21',_0x3d792f[_0x398a9b(0x318,0xafc,0x7c7,0x712)]=_0xf01278(0x9d6,0xa71,0x9d2,0xaed),_0x3d792f[_0xf01278(0xf00,0x721,0x7ac,0xb9d)]=_0xf01278(0x4a1,0x507,0x6a2,0x9c6)+_0x398a9b(0x3d0,0x155,0x41e,0x8ce),_0x3d792f[_0x398a9b(0x376,0x391,0x211,0x2bf)]=_0xf01278(0xff9,0xafb,0x1274,0xe93),_0x3d792f[_0x398a9b(0x3bb,0x1b2,0x4f6,0x3bf)]=_0x398a9b(0x11f,0xd2,0x11f,0x32d)+_0x398a9b(0xd78,0x7b9,0xb2d,0xdb4)+_0x398a9b(0xb3c,0xd1b,0xb28,0x644)+_0xf01278(0xc48,0x115a,0x94a,0xc86)+_0x398a9b(0xac,0x686,0x215,0x2fa)+_0xf01278(0x940,0xd90,0x77f,0x8fd),_0x3d792f[_0x398a9b(0xaf8,0x8de,0x8cd,0x6d9)]=_0x398a9b(0x1ae,-0x1ac,0x14c,-0x1e5)+'e',_0x3d792f[_0x398a9b(0x2e7,0x3c0,0x630,0x202)]=_0xf01278(0xafb,0x1ec,0x8a8,0x60a)+_0x398a9b(0x5ea,0xad8,0x655,0x4a6);const _0x484db0=_0x3d792f,_0xdc9ec0={};_0xdc9ec0[_0xf01278(0xb9f,0xe84,0x6c2,0xa4d)]=_0xf01278(0x632,0x68b,0xd13,0xb7e)+'nu',_0xdc9ec0['descriptio'+'n']=_0xf01278(0x83b,0x2d7,0x7aa,0x436)+_0x398a9b(0xc7e,0xd47,0xb2d,0xb21)+'want\x20to\x20us'+_0xf01278(0x963,0x545,0x9d7,0x663)+_0xf01278(0x53,0x741,-0x78,0x4a6)+_0x398a9b(0xe87,0xd96,0xb8a,0x731),_0xdc9ec0[_0x398a9b(0x6a2,0xd5b,0xb75,0xc29)]='0';let _0x4def87=alpha['prepareMes'+'sageFromCo'+_0xf01278(0xa1b,0x45e,0x9ab,0x876)](from,{'listMessage':{'title':_0xf01278(0xbba,0xe27,0x799,0xc55)+'BOT','description':_0x398a9b(0x947,0xc93,0x8a9,0x93f)+sender[_0xf01278(0x749,0xc65,0x110c,0xbff)]('@')[-0x219d+-0x7*0x37f+-0x3a16*-0x1]+(_0xf01278(0x2c5,0x2e9,0x854,0x43e)+'rimu\x20menye'+_0x398a9b(-0x8d,0x587,0x16d,0x181)+_0x398a9b(0x5aa,0xf75,0xafb,0x5ae)+'h\x20list\x20men'+_0x398a9b(0x4e9,0x52c,0x9fb,0xd28)+_0xf01278(0xd5f,0x8c8,0xfd2,0xccf)+_0xf01278(0xaa2,0xcdd,0x74a,0xa12)+_0xf01278(0xf7a,0x1020,0x10f6,0xd3a)),'buttonText':_0x484db0[_0xf01278(0x35b,0x624,0xb4f,0x755)],'footerText':'©\x20'+creator,'listType':_0x484db0['BBdHP'],'sections':[{'title':_0x484db0[_0xf01278(0x834,0x718,0xc4b,0xb1d)],'rows':[{'title':_0x484db0[_0x398a9b(0xc9a,0x54b,0x812,0x927)],'description':_0xf01278(0x4b6,0x165,0x78a,0x436)+_0x398a9b(0xd90,0xf5a,0xb2d,0x103c)+'want\x20to\x20us'+'e\x20the\x20allm'+'enu\x20comman'+'d_','rowId':'0'}]},{'title':_0x398a9b(0x6d4,0x563,0x6af,0x8c2)+_0xf01278(0x619,0xe3e,0x729,0x9dc),'rows':[{'title':_0x484db0[_0xf01278(0xd13,0x11ce,0xa9e,0xe1a)],'description':_0x484db0['ddagL'],'rowId':'0'}]},{'title':_0x484db0[_0xf01278(0xba9,0x1e3,0x1c1,0x70a)],'rows':[{'title':_0x398a9b(0x940,0xa33,0x984,0xcd2),'description':_0x484db0['QNVVH'],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0xa12,0x7d4,0x54f,0x458)],'rows':[{'title':_0x398a9b(0x97e,0x715,0x6d1,0xa10)+'u','description':_0x484db0[_0xf01278(0x6be,0xb63,0x723,0x861)],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x665,0x89d,0x532,0x3cd)],'rows':[{'title':_0x484db0['ORvDg'],'description':_0x484db0[_0xf01278(0x1117,0x965,0xb5b,0xc56)],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x49a,-0x19b,0x23d,0x769)],'rows':[{'title':_0x484db0['Xgqai'],'description':_0x484db0[_0x398a9b(0x53e,0x76a,0x75e,0x25e)],'rowId':'0'}]},{'title':_0x484db0[_0xf01278(0x1276,0xe10,0x9d6,0xe6d)],'rows':[{'title':_0x398a9b(0x743,0x9ae,0x9bb,0xbad),'description':_0x484db0['zAaXj'],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0xac0,0x167,0x5f8,0x87c)],'rows':[{'title':_0x484db0['YxAcv'],'description':_0x398a9b(0x213,-0x1d6,0x11f,0x2e9)+_0x398a9b(0xb85,0xde6,0xb2d,0x633)+_0xf01278(0x908,0xbc7,0x131c,0xe3f)+_0x398a9b(0x224,0xa57,0x540,0x846)+_0xf01278(0xb53,0x6ea,0xe78,0xa98)+_0x398a9b(-0x1cf,0x5f6,0x318,0x771),'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x67b,0x8c4,0x53b,0x2df)],'rows':[{'title':_0x398a9b(0x619,0x2a2,0x706,0x6c8)+'u','description':_0x484db0['rdFkJ'],'rowId':'0'}]},{'title':_0xf01278(0xaae,0x6d6,0xe9e,0x9c6)+'e-9','rows':[{'title':_0x484db0[_0xf01278(0x799,0x10fa,0x8c2,0xccd)],'description':_0x484db0['nYncE'],'rowId':'0'}]},{'title':'Sub\x20Menu\x20k'+_0xf01278(0xdeb,0xcda,0x884,0xcca),'rows':[{'title':_0x484db0[_0xf01278(0x74d,0xd2e,0x969,0x937)],'description':_0x484db0[_0x398a9b(0x27d,0x46c,0x623,0x365)],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0xdea,0xcba,0x8a5,0x425)],'rows':[{'title':_0x484db0[_0x398a9b(0x5f2,-0x1c2,0x151,0x17c)],'description':_0x484db0['OONGH'],'rowId':'0'}]},{'title':_0x484db0[_0xf01278(0x58e,0x9f8,0x7c7,0x771)],'rows':[{'title':_0x484db0[_0x398a9b(0x2ca,0x541,0x3ff,0x3bd)],'description':_0x398a9b(0x21e,0x18d,0x11f,-0xab)+'is\x20if\x20you\x20'+_0xf01278(0x1177,0xc9b,0xeba,0xe3f)+_0xf01278(0x1147,0xd2a,0x10e1,0xcee)+_0xf01278(0x646,0x45a,0x436,0x74d)+'d_','rowId':'0'}]},{'title':_0x484db0[_0xf01278(0xc37,0x79b,0x6d9,0x986)],'rows':[{'title':_0x484db0[_0x398a9b(0x28b,0x694,0x2fc,0xe7)],'description':_0xf01278(-0x2d,0x890,-0x9c,0x436)+_0x398a9b(0xc3c,0xcf1,0xb2d,0xd28)+_0x398a9b(0x929,0xd65,0xb28,0xb0c)+'e\x20the\x20nsfw'+_0x398a9b(0xc6f,0xe6a,0x983,0x59e)+_0xf01278(0x90e,0xf70,0xe26,0xaf4),'rowId':'0'}]},{'title':_0x484db0[_0xf01278(0x20e,0x38c,0xa49,0x710)],'rows':[{'title':_0x484db0[_0xf01278(0xe17,0xf0a,0xa1c,0xd82)],'description':_0xf01278(0x184,-0x5d,0xba,0x436)+'is\x20if\x20you\x20'+_0xf01278(0x115b,0x124d,0xb26,0xe3f)+_0xf01278(0x5f3,0x529,0x6c7,0xa21)+'an\x20menu\x20co'+_0xf01278(0x680,0xa68,0xb52,0x60e),'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x6ac,0xa3c,0xb32,0x631)],'rows':[{'title':_0x484db0[_0xf01278(0x9a2,0x116d,0xa5e,0xd98)],'description':_0x484db0[_0x398a9b(0x4c9,0x72b,0x349,0x37b)],'rowId':'0'}]},{'title':_0x398a9b(0x29c,0x4a0,0x6af,0x79a)+_0xf01278(0x9b9,0x577,0x2a3,0x7a8),'rows':[{'title':_0x484db0[_0x398a9b(0x420,0x3f6,0x605,0x9f7)],'description':_0x484db0[_0x398a9b(0xa5b,0xa26,0xaef,0xbfc)],'rowId':'0'}]},{'title':_0x484db0['kFACr'],'rows':[{'title':_0x484db0['sZNmw'],'description':_0xf01278(0x518,0x76b,0x137,0x436)+_0x398a9b(0x7b0,0x838,0xb2d,0x9ad)+_0x398a9b(0xaa3,0xe03,0xb28,0xc26)+'e\x20the\x20quot'+_0x398a9b(0x7d4,0x371,0x559,0x3c6)+_0xf01278(0x6ba,0xb42,0xa72,0x8fd),'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x5f4,0xc06,0x6d9,0x97c)],'rows':[{'title':_0x484db0['PZreo'],'description':_0x484db0['UnhsP'],'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(-0x146,-0x135,0x18c,0x0)],'rows':[{'title':_0x484db0['ZtpQI'],'description':_0x484db0['lKBfY'],'rowId':'0'}]},{'title':_0x484db0[_0xf01278(0xbba,0xb38,0xcae,0xce6)],'rows':[_0xdc9ec0]},{'title':_0x484db0[_0xf01278(0xc8f,0xf3a,0xed0,0xc98)],'rows':[{'title':_0x484db0[_0xf01278(0xdff,0xd29,0x9f1,0xade)],'description':_0xf01278(-0xb,0x90,0x35f,0x436)+'is\x20if\x20you\x20'+_0xf01278(0xc4e,0xfd0,0xeba,0xe3f)+_0xf01278(0xbd0,0xefe,0xdb5,0xd2a)+_0x398a9b(0x23c,0x8c6,0x527,0x919)+_0xf01278(0xa4b,0x683,0xa76,0x62f),'rowId':'0'}]},{'title':_0x484db0[_0x398a9b(0x831,0x40a,0x886,0xae1)],'rows':[{'title':_0x484db0['KANuh'],'description':_0x484db0[_0x398a9b(0x303,0xe5,0x4f6,0x434)],'rowId':'0'}]},{'title':_0xf01278(0xf8a,0xab4,0xa10,0xd1c)+_0xf01278(0x68f,0x27d,0x106,0x4cd),'rows':[{'title':_0x484db0['srHep'],'description':_0x398a9b(0x6d6,0xa2e,0x8ca,0x7dd)+_0x398a9b(-0x99,0x36b,0x1f8,0x5b)+_0x398a9b(0x7b9,0x4b4,0x947,0x5c6)+_0xf01278(0x1071,0xe47,0x92d,0xcea)+'e\x20code\x20thi'+_0x398a9b(0xcdd,0x6ac,0x795,0x2fa),'rowId':_0x484db0['srHep']}]},{'title':_0xf01278(0xe9,0x91,0x4f6,0x4e3)+_0x398a9b(0x3e0,0xe61,0x909,0x53a),'rows':[{'title':_0x484db0['lCiXz'],'description':_0xf01278(0x949,0xa7c,0x10c5,0xbe1)+_0xf01278(0x60d,0xc5,0x798,0x50f)+_0xf01278(0x1184,0xa4f,0xc07,0xc5e)+_0x398a9b(0x897,0x9d4,0xb8c,0xc6b)+_0xf01278(0x53b,0x661,0x2d9,0x758)+_0x398a9b(0x952,0x15,0x462,0x284)+'\x20this\x20scri'+_0xf01278(0x12b1,0xdff,0xbbd,0xed4),'rowId':'0'}]}]}},{'quoted':fgclink2,'contextInfo':{'mentionedJid':[sender]}});const _0x45fc6a={};_0x45fc6a['waitForAck']=!![],alpha[_0xf01278(0xe57,0x5d2,0xb5e,0x96a)+_0xf01278(0xab7,0xbd5,0xbaa,0xca9)](_0x4def87,_0x45fc6a);},sendImageMaker=async(_0x29e915,_0x1fa0fc,_0x5c8095)=>{const _0x3b3bf5={'SHrhb':function(_0x306caf,_0x50dad2){return _0x306caf(_0x50dad2);},'CqMLZ':_0x4b83ba(0x20c,0x441,0x3e4,-0x1fe)+_0x30c5e2(-0x96,0x32f,-0x383,-0x87)+_0x30c5e2(0xa7c,0x63d,0x974,0x96d)+'pa\x20saat\x20la'+'gi','jJVRZ':_0x4b83ba(0x46a,-0xc6,0x985,0x479)+_0x4b83ba(0x621,0xa23,0x5f0,0x8ec),'GXvgd':_0x30c5e2(-0x411,-0x36b,-0x224,0x5c),'NAGSd':'🐨\x20Owner','noHre':_0x30c5e2(0xadd,0x788,0x6ee,0x618),'uuteU':_0x4b83ba(0x6d6,0x880,0x656,0xb10)},_0x4c98c2=await alpha[_0x30c5e2(0xeae,0x9b8,0x812,0x9d3)+_0x30c5e2(0x4d3,0x290,0x7dc,0x7a0)](from,_0x29e915,MessageType[_0x30c5e2(0x520,0x1e0,-0x44,0x4c6)],{'thumbnail':Buffer[_0x4b83ba(0x542,0x76b,0xe2,0x783)](-0xe31+0x10d+0xd24)});let _0x310c9c=_0x4c98c2['message'][_0x3b3bf5[_0x4b83ba(0x6ca,0x994,0x70e,0x737)]]?_0x4c98c2[_0x4b83ba(0x60c,0x9bd,0x814,0x201)]['ephemeralM'+'essage']:_0x4c98c2;const _0x121977={};_0x121977['buttonId']=_0x3b3bf5[_0x30c5e2(0xc5,0x9ea,0x16b,0x512)],_0x121977[_0x30c5e2(0x614,0xa03,0x9e2,0x61e)]={},_0x121977[_0x4b83ba(0x4c7,0x11e,0x78c,-0x42)]=0x1,_0x121977[_0x30c5e2(0x614,0xa03,0x9e2,0x61e)][_0x30c5e2(0x78c,0xa2a,0xb55,0x8f4)+'t']=_0x3b3bf5[_0x4b83ba(0x42f,0x2ec,0x17,0x3db)];const _0x169ff7={};_0x169ff7[_0x30c5e2(0x3a6,0x695,-0x165,0x23e)]=_0x3b3bf5[_0x30c5e2(0x348,-0x2ee,-0xfc,0x57)],_0x169ff7[_0x4b83ba(0x523,0x941,0x6e6,0x105)]={},_0x169ff7[_0x4b83ba(0x4c7,0x833,0x7f0,0x7d8)]=0x1,_0x169ff7[_0x4b83ba(0x523,0x941,0x6e6,0x105)][_0x4b83ba(0x7f9,0xadc,0x947,0x9db)+'t']=_0x3b3bf5['uuteU'];function _0x30c5e2(_0x11f2ef,_0x5c5562,_0x3c8954,_0x3a6069){return _0x3f34aa(_0x11f2ef-0x50,_0x3c8954,_0x3a6069- -0xd1,_0x3a6069-0x1cf);}const _0x171799=[_0x121977,_0x169ff7],_0x3cc88a={'contentText':_0x1fa0fc,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x4b83ba(-0x8d,-0xcc,-0x245,0x38d)+_0x4b83ba(0x3f2,0x4e7,0x6a8,0x713))+_0x5c8095[_0x30c5e2(0x6d8,0x4f4,0x95f,0x6f6)]('@')[-0x1*-0x2337+-0x2347+0x2*0x8],'buttons':_0x171799,'headerType':0x4,'imageMessage':_0x310c9c[_0x4b83ba(0x60c,0x79a,0xaa5,0x347)][_0x4b83ba(0x37e,0x638,-0x1b0,0x6b9)+'ge']},_0x577afc={};_0x577afc['mentionedJ'+'id']=[_0x5c8095];const _0x38dddc={};_0x38dddc[_0x4b83ba(0x5ed,0x5cd,0x9e4,0x51c)]=mek,_0x38dddc[_0x30c5e2(0x2d1,0x15e,-0x8a,0x11a)+'o']=_0x577afc;function _0x4b83ba(_0x530510,_0x363c22,_0x28f48c,_0x127660){return _0x1da9f3(_0x530510- -0x380,_0x363c22,_0x28f48c-0x1ed,_0x127660-0x18a);}alpha[_0x30c5e2(0x239,-0x2b3,0x4e,-0x68)+'e'](from,_0x3cc88a,MessageType['buttonsMes'+_0x30c5e2(0x36f,0x65a,0xb64,0x7a0)],_0x38dddc)[_0x4b83ba(0x513,0x799,0x800,0x51a)](_0x1f1a74=>{function _0xc7b271(_0x1cf9b7,_0x2c300f,_0x47559a,_0x58af59){return _0x30c5e2(_0x1cf9b7-0x13d,_0x2c300f-0x19a,_0x1cf9b7,_0x2c300f-0x357);}function _0xaac0ad(_0x26353f,_0x263c45,_0x1b55dd,_0x10616c){return _0x4b83ba(_0x26353f-0x126,_0x263c45,_0x1b55dd-0x16f,_0x10616c-0xe);}_0x3b3bf5[_0xc7b271(0x5d1,0x3e1,-0x96,0x58c)](reply,_0x3b3bf5[_0xc7b271(0xbd0,0x935,0x77d,0xbe2)]);});},send_qr_gopay=async(_0x3a5e5d,_0x4ae588)=>{ini_bayar=ucapannya2+(_0x46244c(0xc02,0x6be,0x74c,0x683)+_0x46244c(0x4ed,0x32e,0x515,0x221))+pushname+(_0x46244c(0x91b,0x6fe,0xa0c,0x677)+_0x18bff9(0x59,0x8d9,0x467,-0xe))+sender[_0x18bff9(0x8e6,0x199,0x683,0x198)]('@')[0x2*0x961+0x9c6+-0x1c88]+(_0x18bff9(0x5bd,0x60b,0x498,0x63b)+_0x18bff9(-0x2cc,0x497,0xdb,-0x477)+_0x18bff9(0x9de,0x8ca,0x51e,0x4f0)+_0x46244c(0x52b,0x879,0x3a0,-0xe0)+_0x46244c(0x4ca,0x6f3,0x9ed,0xd33)+'Pilih\x20Pay\x20'+_0x18bff9(0x52,0xa,0x79,-0x252)+_0x46244c(0x5b5,0x485,0x8e7,0x787)+_0x46244c(0x51d,0x551,0x75a,0xc64)+'\x20GOPAY.\x0a4.'+_0x46244c(0x92c,0x633,0x6ef,0x3e6)+_0x18bff9(-0x79,0x352,0x352,0x1b9)+_0x18bff9(0x467,0x2f7,0x2d,-0x1bc)+_0x46244c(0xaa3,0x6b7,0x7ee,0x4f4)+_0x46244c(0x65d,0x883,0x3d1,-0x98)+_0x46244c(0x93c,0x785,0x6f4,0x1b4))+ini_mark[_0x18bff9(0xa0c,0x520,0x683,0x520)]('@')[-0xb99+-0x19*0x18b+0x322c]+'_\x0a';const _0x1638a8={};_0x1638a8['mentionedJ'+'id']=[sender,ini_mark];function _0x18bff9(_0x1467fc,_0x52cb54,_0x588b94,_0x1206a6){return _0x3f34aa(_0x1467fc-0x6,_0x52cb54,_0x588b94- -0x144,_0x1206a6-0x186);}function _0x46244c(_0x19bb01,_0x40b5c5,_0x365749,_0x5362eb){return _0x1da9f3(_0x365749- -0xdb,_0x40b5c5,_0x365749-0x1dd,_0x5362eb-0x64);}await alpha[_0x46244c(0x17c,-0x29e,0x142,0x2af)+'e'](from,_0x3a5e5d,image,{'thumbnail':Buffer[_0x18bff9(0x4f6,0x906,0x5ca,0x71a)](-0x7a8+-0x1*0x6f2+0x216*0x7),'quoted':_0x4ae588,'caption':ini_bayar,'contextInfo':_0x1638a8});},send_qr_ovo=async(_0x37b634,_0x2fa156)=>{ini_bayar=ucapannya2+('\x0a\x0a\x0a•>\x20Nama'+_0x1032dd(0x976,0xb02,0x927,0x78a))+pushname+(_0x1032dd(0xce4,0x111a,0xe1e,0xec7)+_0x50d36a(0x9aa,0x59e,0x93c,0x9f7))+sender[_0x50d36a(0x291,0x7ba,0x9c4,0xb5a)]('@')[0x435*0x3+-0x1c*-0x160+-0x331f]+(_0x50d36a(0x8b8,0x660,0x655,0x58c)+_0x50d36a(0x4a8,0x4b6,0x460,0x6c9)+_0x1032dd(0x1051,0x1366,0xec4,0x127e)+'\x20OVO\x20via\x20H'+_0x1032dd(0x980,0x790,0x9f4,0x8c0)+_0x1032dd(0x6f5,0x7c3,0x567,0xa3)+'\x20Bayar\x0a3.\x20'+'Scan\x20Barco'+_0x50d36a(0x214,0x122,0xcb,0x297)+'OVO.\x0a4.\x20da'+_0x1032dd(0x775,0xe72,0x982,0x4f0)+_0x1032dd(0x500,0xd16,0x7e6,0x341)+_0x50d36a(0xd26,0x9ac,0x849,0x7cc)+_0x50d36a(0xb32,0x96f,0xaca,0xb86)+_0x50d36a(-0x11a,0x19,0x3e2,-0x394)+'@')+ini_mark['split']('@')[0xa36+0x1657+0x208d*-0x1]+'_\x0a';function _0x1032dd(_0x18f800,_0x497fcc,_0x3b833a,_0x5716ed){return _0x1da9f3(_0x3b833a-0x337,_0x18f800,_0x3b833a-0x158,_0x5716ed-0x12a);}function _0x50d36a(_0xa8a02f,_0x2fd485,_0x25ba2a,_0x35748d){return _0x1da9f3(_0x2fd485- -0x1c1,_0x35748d,_0x25ba2a-0x1ea,_0x35748d-0xb6);}const _0x1da8c5={};_0x1da8c5[_0x50d36a(-0x159,0x16e,0x11c,-0x1b)+'id']=[sender,ini_mark],await alpha[_0x50d36a(0x5,0x5c,-0xe,0x3d0)+'e'](from,_0x37b634,image,{'thumbnail':Buffer[_0x50d36a(0x7ba,0x701,0x852,0xc08)](0x252b+-0xd*-0x2f6+-0x4ba9),'quoted':_0x2fa156,'caption':ini_bayar,'contextInfo':_0x1da8c5});},send_qr_dana=async(_0x322ed5,_0xb6de5)=>{ini_bayar=ucapannya2+('\x0a\x0a\x0a•>\x20Nama'+_0x303636(0x9e4,0x63e,0x93f,0xb78))+pushname+(_0x303636(0xc03,0xb35,0xbd5,0x9e1)+'\x20:\x20_@')+sender['split']('@')[0x6d3*0x2+-0x23da+0xcb*0x1c]+(_0x551c2b(0x432,0x886,0x5ba,0x969)+_0x303636(-0x135,0x421,0xf7,0x92b)+_0x303636(0xd50,0x864,0x991,0x86e)+_0x303636(0xfc2,0xb4a,0xcfa,0xb50)+_0x303636(0x8e8,0x70b,0x7ee,0x493)+_0x551c2b(0x47a,0x439,0x5a,-0x438)+'\x20Bayar\x0a3.\x20'+_0x303636(0x68d,0xb05,0x7d4,0xdbb)+'de\x20diatas\x20'+_0x303636(0x51e,0x343,0x700,0x4cc)+'an\x20Masukka'+'n\x20Nominal\x20'+_0x303636(-0x248,0x2a1,0x386,-0x276)+_0x303636(0xbbf,0xae0,0x6cc,0x715)+_0x303636(0x831,0x3a8,0x360,0x4b7)+'\x20@')+ini_mark[_0x551c2b(0x80d,0xbed,0x7a5,0x4c3)]('@')[-0x1*0x147e+-0x14e3+0x2961]+'_\x0a';const _0x37ca96={};function _0x551c2b(_0x45116d,_0x440800,_0x125050,_0x444d01){return _0x3f34aa(_0x45116d-0xd6,_0x440800,_0x125050- -0x22,_0x444d01-0x1c1);}function _0x303636(_0x337902,_0x2bd511,_0x289c70,_0x1f8f31){return _0x1da9f3(_0x2bd511-0x4e,_0x1f8f31,_0x289c70-0x149,_0x1f8f31-0xe3);}_0x37ca96[_0x551c2b(0x274,0x173,0x159,-0x3c)+'id']=[sender,ini_mark],await alpha['sendMessag'+'e'](from,_0x322ed5,image,{'thumbnail':Buffer['alloc'](0x238d+0x1ae7+-0x3e74),'quoted':_0xb6de5,'caption':ini_bayar,'contextInfo':_0x37ca96});},{soundmenu,kerangmenu,groupmenu,quotesmenu,animemenu,coganmenu,helpmenu,convertmenu,funmenu,downloadmenu,makermenu,textpromenu,photooxymenu,telestiker,othermenu,ownermenu,set_stc_cmd,storagemenu,cecanmenu,tagmenu,upmenu,nsfwmenu,asupanmenu,image_effect,allmenu}=require(_0x1da9f3(0x4e8,0x51,0x854,0x38f)+_0x1da9f3(0x62b,0xa10,0x1c5,0xa69)),isBtMsg=type==_0x3f34aa(0xdec,0x7de,0x93b,0xc92)+_0x1da9f3(0x818,0x593,0x971,0xbad)+'ge'?mek[_0x1da9f3(0x98c,0xe3e,0xd07,0xd62)][_0x3f34aa(0xd12,0xa40,0x93b,0x5a5)+_0x3f34aa(0x389,0x568,0x664,0x9b8)+'ge'][_0x3f34aa(0x6bb,0xc46,0x9e3,0x608)+'splayText']:'',isStMsg=type=='listRespon'+'seMessage'?mek[_0x3f34aa(0xab0,0x430,0x7d8,0xc21)][_0x3f34aa(0xb7a,0x535,0x6b9,0x87a)+_0x3f34aa(0x646,0xa76,0x5c6,0x10c)][_0x1da9f3(0x7c9,0xc6c,0xce6,0x3f7)]:'';function _0x3f34aa(_0x2b7177,_0x5285de,_0x24cb6e,_0xdd2018){return _0xfa31(_0x24cb6e- -0x19c,_0x5285de);}const stc=mek[_0x1da9f3(0x98c,0xa34,0x92b,0xe09)]['stickerMes'+_0x1da9f3(0xa25,0x890,0x562,0x781)],msc=mek[_0x1da9f3(0x98c,0x723,0xdd3,0xba4)]['audioMessa'+'ge'];wew=fs[_0x3f34aa(-0x360,0x2cb,0x111,0x656)+'nc'](_0x1da9f3(0x255,0x1a7,0x229,0x77c)+thumbnail),alfa2=_0x3f34aa(0xa0a,0xc5b,0x9c1,0x70d)+_0x3f34aa(0x9b3,0x851,0x476,0x77f)+'sapp.net',alfa3=_0x1da9f3(0x22a,0x6a2,-0x2f8,-0x2cb)+_0x1da9f3(0x44b,0x12c,0x505,0x62c)+_0x1da9f3(0xba1,0xd49,0xc0e,0xe2c),sub_yt_zeeone_ofc=type==_0x3f34aa(0x82d,0x5b8,0x6b9,0xa49)+_0x3f34aa(0x8f8,0x24b,0x5c6,0x3c5)?mek['message']['listRespon'+'seMessage'][_0x1da9f3(0x7c9,0x93e,0x689,0x577)]:'',subscribezeeoneofc=type==_0x1da9f3(0xaef,0xb99,0xe3e,0x7db)+_0x1da9f3(0x818,0x4b7,0x8d1,0x968)+'ge'?mek[_0x1da9f3(0x98c,0xa7b,0x497,0xac9)]['buttonsRes'+_0x1da9f3(0x818,0xcee,0x647,0xc22)+'ge'][_0x1da9f3(0x837,0x50d,0x488,0x2f9)+_0x3f34aa(0x4f5,0x2d,0x3fc,0x37c)]:'';if(sub_yt_zeeone_ofc=='group\x20open'){console[_0x1da9f3(0x6d2,0x3aa,0x829,0x7dc)](color(time,_0x1da9f3(0x406,-0xc3,0x620,0x15d)),color(_0x3f34aa(0x2b2,0x342,0x189,-0x1e)+_0x3f34aa(-0x244,-0x1bb,0x19f,-0x2b2)+'pen'));if(!isBotGroupAdmins)return reply(_0x3f34aa(0x777,0x187,0x2ee,0x5ec)+_0x1da9f3(0x50c,0x185,0x214,0xa19)+_0x1da9f3(0x8cc,0x695,0xd4a,0x643)+_0x1da9f3(0x34c,0x560,0x6b1,-0x1f1)+'hulu```');await alpha['groupSetti'+_0x3f34aa(0xa30,0xcd4,0x87a,0xb8a)](from,GroupSettingChange[_0x3f34aa(0x613,0x68a,0x67a,0x99d)+'d'],![]),fakegroup(_0x3f34aa(0x873,0x72c,0x5d5,0x8e9)+_0x3f34aa(0x57e,0x867,0x8a5,0xcaa)+_0x1da9f3(0x78d,0x70e,0x6ad,0xb6a)+'``');}if(sub_yt_zeeone_ofc==_0x3f34aa(0x5d2,0x846,0x76f,0x85f)+'e'){console[_0x1da9f3(0x6d2,0x1ff,0xa64,0x45e)](color(time,_0x3f34aa(0x335,0x2f7,0x252,0x696)),color(_0x3f34aa(-0x15a,0x51b,0x189,-0xc1)+'ge\x20group\x20c'+_0x3f34aa(0x5c9,0xb72,0x95a,0x703)));if(!isBotGroupAdmins)return reply('```Jadikan'+_0x3f34aa(-0x174,0x696,0x358,0x76b)+_0x3f34aa(0x337,0x7bd,0x718,0x995)+_0x1da9f3(0x34c,0x348,0x231,0x287)+_0x3f34aa(0xda8,0xde7,0x8ab,0xa9a));await alpha['groupSetti'+_0x1da9f3(0xa2e,0x5ff,0xe9c,0xc7b)](from,GroupSettingChange[_0x3f34aa(0x211,0x596,0x67a,0xaa3)+'d'],!![]),fakegroup(_0x1da9f3(0x789,0xbb7,0x5f1,0x270)+_0x3f34aa(0x28e,0x618,0x67e,0x5f0)+'G\x20GROUP```');}if(sub_yt_zeeone_ofc==_0x1da9f3(0xc0f,0x6ef,0xc28,0xd87)){console[_0x1da9f3(0x6d2,0x5e6,0xbcb,0x2fd)](color(time,_0x1da9f3(0x406,0x1ee,0x1b1,0x102)),color('List\x20messa'+_0x1da9f3(0x30c,0x637,0x48a,0x564)+_0x1da9f3(0x73f,0xa29,0x8ef,0x36f)));var quotejamzk=await getBuffer('https://ww'+_0x1da9f3(0x3e1,-0x10a,0x5c8,0x1a6)+_0x3f34aa(0x27f,-0xdb,0x3e1,0x7b3)+_0x3f34aa(0x857,0xd37,0x96e,0x75d)+_0x3f34aa(0xd87,0xb2d,0x928,0x81d)+'?&imageout'+_0x3f34aa(0x5ca,0x4d3,0x236,0x609)+_0x3f34aa(0x347,0x774,0x2ab,0x39e)+_0x3f34aa(0x578,0x4dd,0x562,0x3b7)+_0x1da9f3(0x50b,0x341,0x267,0x969)+'&scaleWidt'+_0x3f34aa(-0x1bd,0x565,0x1ed,-0xb9)+_0x3f34aa(0xb42,0xa24,0xa0b,0x73f)+_0x3f34aa(0x7dd,0x571,0x4b4,0x830)+_0x3f34aa(0x53b,0x63b,0x1ce,0x136)+'extType=1&'+_0x1da9f3(0x7d7,0x8c4,0x948,0x612)+'ttern=Warn'+'ing!&text='+_0x1da9f3(0x575,0x4cf,0x2df,0x51e)+'ib');const _0x308892={};_0x308892['displayTex'+'t']=_0x1da9f3(0xb31,0xd2b,0x7d8,0xbb9);const _0x4004c1={};_0x4004c1[_0x3f34aa(0x552,0x771,0x30f,0xe0)]=_0x3f34aa(-0x151,-0x414,0x8c,0x1bc),_0x4004c1[_0x1da9f3(0x8a3,0xd05,0x8aa,0xb3d)]=_0x308892,_0x4004c1['type']=0x1;const _0x136027={};_0x136027[_0x3f34aa(0xd31,0x6cb,0x9c5,0xd1a)+'t']=_0x1da9f3(0xb3e,0x98e,0xbb7,0xb11);const _0x31d6a6={};_0x31d6a6[_0x3f34aa(-0x245,0x1ab,0x30f,0x290)]=_0x3f34aa(0x662,0x314,0x12d,-0xd6),_0x31d6a6[_0x1da9f3(0x8a3,0xa71,0x587,0xace)]=_0x136027,_0x31d6a6[_0x3f34aa(0x5d6,0xa91,0x693,0x69f)]=0x1,sendButLocation(from,soundmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x1fe,0x338,0x1b6,0x4e7)+ini_mark[_0x3f34aa(0x8ea,0x556,0x7c7,0xcd3)]('@')[-0x73*0x11+0x13a*0xb+0x5db*-0x1],quotejamzk,[_0x4004c1,_0x31d6a6],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x5bb,0x229,0x6b5,0x15c)){console[_0x3f34aa(0xa16,0xf1,0x51e,0x120)](color(time,_0x3f34aa(-0xfc,0x609,0x252,0x433)),color(_0x1da9f3(0x33d,0x371,0x72a,0x6d9)+'ge\x20quotesm'+_0x1da9f3(0x73f,0xbd4,0x532,0xb4c)));var quotejamzk=await getBuffer(_0x3f34aa(0x2ed,0x5d,0x22c,0x537)+'legra.ph/f'+_0x1da9f3(0x6a4,0xb95,0x3a4,0x26e)+_0x1da9f3(0x7ed,0x2b7,0xb22,0x504)+_0x1da9f3(0x2ac,0x6a9,0x38f,0x3b2));const _0x2ee953={};_0x2ee953[_0x1da9f3(0xb79,0xf8b,0x979,0xb80)+'t']=_0x1da9f3(0xb31,0x60d,0xc46,0x9eb);const _0x1372f3={};_0x1372f3[_0x3f34aa(0x1ab,0x30e,0x30f,0x647)]=_0x1da9f3(0x240,0x3b4,0x38f,0x69a),_0x1372f3[_0x1da9f3(0x8a3,0x3a5,0x7f1,0x3dc)]=_0x2ee953,_0x1372f3[_0x3f34aa(0x882,0x4d5,0x693,0x662)]=0x1;const _0x85db22={};_0x85db22[_0x1da9f3(0xb79,0x697,0x704,0xac7)+'t']=_0x3f34aa(0x933,0x83f,0x98a,0xa70);const _0x5825ca={};_0x5825ca[_0x1da9f3(0x4c3,0x6ba,0x2d4,0x47a)]='owner',_0x5825ca['buttonText']=_0x85db22,_0x5825ca[_0x3f34aa(0x72e,0xb56,0x693,0x629)]=0x1,sendButLocation(from,soundmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x707,0x581,0x1b6,0x1e9)+ini_mark['split']('@')[0x34*-0x21+0x102*0xe+-0x768],quotejamzk,[_0x1372f3,_0x5825ca],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='quotesmenu'){console[_0x3f34aa(0xa5f,0x42d,0x51e,0xa25)](color(time,_0x1da9f3(0x406,0x767,0x5b0,0x88e)),color(_0x3f34aa(0x687,0x396,0x189,-0x285)+_0x3f34aa(-0x32e,0x600,0x158,-0xbc)+_0x3f34aa(0x994,0x9aa,0x58b,0x331)));var quoteamzk=await getBuffer(_0x1da9f3(0x3e0,-0xfe,0x3e3,0x66a)+_0x3f34aa(0x207,0x54b,0x204,0x3b4)+_0x3f34aa(0x3c6,0x771,0x52c,0x800)+_0x3f34aa(0x7d1,0xa76,0x650,0xa3e)+_0x1da9f3(0x833,0xb25,0x954,0xc21));const _0x255d5f={};_0x255d5f[_0x3f34aa(0xad3,0x4ae,0x9c5,0x7d8)+'t']=_0x3f34aa(0x4a3,0x5af,0x97d,0xd19);const _0x3f5909={};_0x3f5909[_0x1da9f3(0x4c3,0x5e8,-0x3,0x878)]=_0x1da9f3(0x240,0x4c4,0x318,-0x2b2),_0x3f5909[_0x3f34aa(0xaaf,0x793,0x6ef,0x3e5)]=_0x255d5f,_0x3f5909[_0x1da9f3(0x847,0x4da,0x58e,0x4c5)]=0x1;const _0x4e3ec5={};_0x4e3ec5[_0x1da9f3(0xb79,0x10cf,0x755,0xf43)+'t']=_0x3f34aa(0x81e,0x5bb,0x98a,0x485);const _0x3bb9b7={};_0x3bb9b7[_0x3f34aa(0x24d,0x77f,0x30f,-0x1a5)]='owner',_0x3bb9b7[_0x3f34aa(0x850,0x86a,0x6ef,0xc41)]=_0x4e3ec5,_0x3bb9b7[_0x1da9f3(0x847,0xd92,0x765,0xbcd)]=0x1,sendButLocation(from,quotesmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x3f34aa(0x274,0x96b,0x7c7,0x927)]('@')[-0x2*-0xcbb+-0xc4e+0x8*-0x1a5],quoteamzk,[_0x3f5909,_0x3bb9b7],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x689,0x59f,0x863,0x4a2)){console[_0x1da9f3(0x6d2,0xa25,0x787,0x576)](color(time,_0x3f34aa(0x6b3,0x285,0x252,0x579)),color('List\x20messa'+'ge\x20quotesm'+_0x1da9f3(0x73f,0x656,0xb22,0xa36)));var jakkja=await getBuffer(_0x1da9f3(0x3e0,0x511,-0xbf,0x723)+'legra.ph/f'+'ile/db6f0f'+'fb7e261a68'+_0x3f34aa(0x4d4,0x6fc,0x359,0x560));const _0x3216ee={};_0x3216ee[_0x1da9f3(0xb79,0xa73,0xde1,0xee0)+'t']=_0x1da9f3(0xb31,0xdcc,0x94b,0x662);const _0x376387={};_0x376387[_0x3f34aa(0x43b,0x420,0x30f,0x39)]='Command',_0x376387[_0x3f34aa(0x412,0x85e,0x6ef,0x245)]=_0x3216ee,_0x376387[_0x1da9f3(0x847,0xd2a,0x6a7,0x78f)]=0x1;const _0x51dfb5={};_0x51dfb5[_0x1da9f3(0xb79,0x9bf,0x6b7,0x657)+'t']='✓\x20OWNER';const _0x5bad5e={};_0x5bad5e[_0x3f34aa(0x699,-0x1c6,0x30f,-0x1bb)]='owner',_0x5bad5e[_0x3f34aa(0x32d,0xb53,0x6ef,0x307)]=_0x51dfb5,_0x5bad5e['type']=0x1,sendButLocation(from,groupmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark['split']('@')[0xab0+-0x1*-0x1feb+0xd*-0x347],jakkja,[_0x376387,_0x5bad5e],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x7a8,0x67a,0x91c,0x2ce)){console[_0x1da9f3(0x6d2,0x520,0x763,0x4bf)](color(time,_0x1da9f3(0x406,-0xcc,0x790,0x4b0)),color(_0x1da9f3(0x33d,-0x1d3,-0x4c,0x4c8)+'ge\x20animeme'+'nu'));var animkzka=await getBuffer(_0x1da9f3(0x3e0,0x717,0x2b6,0x26d)+'legra.ph/f'+_0x3f34aa(0x113,-0x107,0xe0,0x565)+_0x3f34aa(0x387,0x941,0x5c0,0x65c)+_0x3f34aa(0x68b,0x84f,0x540,0x2a2));const _0x447c35={};_0x447c35[_0x3f34aa(0xd6b,0xb7a,0x9c5,0xc06)+'t']=_0x1da9f3(0xb31,0xd44,0xccc,0x7c4);const _0x2ab35c={};_0x2ab35c[_0x3f34aa(-0xa1,0x3dc,0x30f,0x22c)]=_0x1da9f3(0x240,-0x2c6,-0xe4,0x258),_0x2ab35c[_0x3f34aa(0x954,0xa4a,0x6ef,0xb11)]=_0x447c35,_0x2ab35c['type']=0x1;const _0x1e35d3={};_0x1e35d3[_0x1da9f3(0xb79,0xf3a,0x666,0xb0c)+'t']=_0x3f34aa(0x741,0x88f,0x98a,0x8c7);const _0x3f9172={};_0x3f9172['buttonId']=_0x1da9f3(0x2e1,0x177,0x30e,0x703),_0x3f9172[_0x1da9f3(0x8a3,0xb5e,0x43a,0x7b4)]=_0x1e35d3,_0x3f9172[_0x1da9f3(0x847,0x3bb,0x80b,0x98e)]=0x1,sendButLocation(from,animemenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x3a1,0xab,0x1b6,0x455)+ini_mark['split']('@')[-0x4*-0x987+0x13*-0x80+-0x1*0x1c9c],animkzka,[_0x2ab35c,_0x3f9172],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x764,0xa8a,0x50b,0x7bf)+'u'){console['log'](color(time,_0x1da9f3(0x406,0x1ea,0x8a3,0x656)),color(_0x1da9f3(0x33d,0x1d,0x895,0x69c)+_0x3f34aa(0x4f1,0x98d,0x5d8,0x5d6)+'menu'));var converkdk=await getBuffer(_0x1da9f3(0x3e0,0x1ca,-0x52,0x3b3)+'legra.ph/f'+_0x1da9f3(0x234,0x117,0x36d,0xfa)+'65e7d9ab70'+_0x1da9f3(0x3f5,0x5f6,0x858,0x528));const _0x2af324={};_0x2af324['displayTex'+'t']=_0x1da9f3(0xb31,0xf9b,0x678,0x6d7);const _0x13ffba={};_0x13ffba[_0x1da9f3(0x4c3,0x275,0x19c,0x762)]=_0x1da9f3(0x240,0x2a5,0xce,-0x210),_0x13ffba['buttonText']=_0x2af324,_0x13ffba[_0x3f34aa(0x44c,0x918,0x693,0x990)]=0x1;const _0x179252={};_0x179252[_0x1da9f3(0xb79,0xabc,0x1055,0xbc8)+'t']='✓\x20OWNER';const _0x15db5e={};_0x15db5e['buttonId']='owner',_0x15db5e[_0x1da9f3(0x8a3,0x477,0xc03,0xbd8)]=_0x179252,_0x15db5e[_0x1da9f3(0x847,0x4be,0x34a,0xc5c)]=0x1,sendButLocation(from,convertmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x2f5,0x2d1,0x1b6,0x2b4)+ini_mark[_0x3f34aa(0x599,0x424,0x7c7,0xcf5)]('@')[-0x11f0+-0x1c0+0x13b0],converkdk,[_0x13ffba,_0x15db5e],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x2b4,0x1a4,0x118,0x334)){console[_0x1da9f3(0x6d2,0x82d,0xbdb,0x1b9)](color(time,_0x1da9f3(0x406,0x3aa,0x152,0x281)),color(_0x1da9f3(0x33d,0x69b,0x5f8,0x409)+_0x3f34aa(-0x1c8,-0x4f5,0x12,0x110)+_0x1da9f3(0x503,0x2c0,0x182,0x8a0)));var nsfwkak=await getBuffer(_0x3f34aa(0x22d,-0xbb,0x22c,0x64c)+_0x1da9f3(0x3b8,0x302,0x882,0x5ab)+'ile/487eeb'+_0x1da9f3(0x4a0,0x869,0x98b,0xd4)+_0x3f34aa(0x587,-0x165,0x4b,0x249));const _0x439bb7={};_0x439bb7['displayTex'+'t']=_0x3f34aa(0xd1d,0xae6,0x97d,0xcdb);const _0x371429={};_0x371429[_0x1da9f3(0x4c3,0x948,0x2ff,0x82b)]='Command',_0x371429[_0x1da9f3(0x8a3,0x7c3,0x59b,0x363)]=_0x439bb7,_0x371429[_0x3f34aa(0x5ed,0x89b,0x693,0x600)]=0x1;const _0x16bdbf={};_0x16bdbf[_0x3f34aa(0x786,0x69e,0x9c5,0xc05)+'t']=_0x1da9f3(0xb3e,0xf43,0xa78,0xee1);const _0xf5c656={};_0xf5c656[_0x3f34aa(-0x1e3,0x3dc,0x30f,0x322)]=_0x1da9f3(0x2e1,0x684,0x2b0,0x1ea),_0xf5c656[_0x3f34aa(0x5fc,0x2fa,0x6ef,0x47d)]=_0x16bdbf,_0xf5c656[_0x1da9f3(0x847,0xc29,0x84d,0x835)]=0x1,sendButLocation(from,nsfwmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,0xdd,0x70d,-0x12d)+ini_mark['split']('@')[0xb*-0x2b+0x1001*0x1+-0xe28],nsfwkak,[_0x371429,_0xf5c656],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='asupan\x20men'+'u'){console[_0x1da9f3(0x6d2,0x473,0x58b,0x4c4)](color(time,_0x3f34aa(0x5d5,0x463,0x252,0x9c)),color(_0x1da9f3(0x33d,0x89,0x598,-0xd1)+'ge\x20asupan\x20'+_0x1da9f3(0x503,0x9a8,0x868,0x4cf)));var asupanak=await getBuffer(_0x1da9f3(0x3e0,0xee,0x8bb,0x3c2)+_0x3f34aa(-0x144,0x17,0x204,-0x30)+_0x1da9f3(0x446,0x1b8,0x881,0x8b0)+_0x3f34aa(0x947,0x991,0x644,0xa8a)+_0x1da9f3(0x7aa,0x5cd,0x837,0x321));const _0x378b0e={};_0x378b0e[_0x1da9f3(0xb79,0x8b4,0x1069,0xb0f)+'t']=_0x3f34aa(0xec8,0x86f,0x97d,0xcb6);const _0xd51be2={};_0xd51be2[_0x1da9f3(0x4c3,0x1cc,0x766,0x117)]='Command',_0xd51be2['buttonText']=_0x378b0e,_0xd51be2[_0x1da9f3(0x847,0x903,0x6b9,0xb41)]=0x1;const _0x546bfb={};_0x546bfb['displayTex'+'t']=_0x1da9f3(0xb3e,0xb83,0xc96,0xf10);const _0x4e02e7={};_0x4e02e7[_0x1da9f3(0x4c3,0x2ca,0x381,0x46e)]=_0x1da9f3(0x2e1,0xe8,0x28,0x709),_0x4e02e7[_0x1da9f3(0x8a3,0xb8e,0xb04,0x4b7)]=_0x546bfb,_0x4e02e7[_0x3f34aa(0xb1f,0x780,0x693,0x7cc)]=0x1,sendButLocation(from,asupanmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,-0xc2,0x1f6,0x6c3)+ini_mark[_0x3f34aa(0x362,0x484,0x7c7,0x6d4)]('@')[0x2064+-0x1*-0xdf5+-0x2e59],asupanak,[_0xd51be2,_0x4e02e7],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x69e,0x5c9,0x7d9,0x558)+'ot'){console[_0x1da9f3(0x6d2,0x346,0x2a6,0x313)](color(time,'magenta'),color('Jika\x20menu\x20'+_0x3f34aa(0xb4d,0x725,0x6e2,0x85a)+'ul,\x20kurang'+_0x3f34aa(0x442,0x127,0x3a7,0x32e)+'mbnail'));var allmenuk=await getBuffer(_0x1da9f3(0x3e0,-0x131,0x528,0x66a)+_0x3f34aa(0x668,-0x95,0x204,0x5d1)+_0x1da9f3(0x423,0x89,0x25d,0x272)+_0x1da9f3(0x4ce,0x8cc,0x77a,0x4d8)+_0x1da9f3(0x675,0x863,0x38b,0x63b));const _0x156248={};_0x156248[_0x3f34aa(0xa7b,0x53c,0x9c5,0x605)+'t']=_0x3f34aa(0xc0f,0xac5,0x97d,0xb6f);const _0x201503={};_0x201503['buttonId']=_0x3f34aa(0x469,-0xb4,0x8c,-0x406),_0x201503[_0x1da9f3(0x8a3,0xb4e,0xcae,0xb90)]=_0x156248,_0x201503[_0x3f34aa(0x939,0x5a8,0x693,0x60b)]=0x1;const _0xa70cbd={};_0xa70cbd['displayTex'+'t']=_0x3f34aa(0x56e,0x4b9,0x98a,0x84e);const _0x478418={};_0x478418[_0x1da9f3(0x4c3,0x711,0x3fe,0x3be)]=_0x3f34aa(-0x3e6,-0x1a4,0x12d,0x57b),_0x478418[_0x3f34aa(0x906,0x32f,0x6ef,0xb94)]=_0xa70cbd,_0x478418['type']=0x1,sendButLocation(from,allmenu(kyun,os,prefix,wita,wit,ucapannya2,timuu,status,wa_version,mcc,mnc,os_version,device_manufacturer,device_model,alfa,alfa1,thisDay,ini_tanggal,totalchat,hit_today,ini_gcchat,latensii),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,-0x9f,0x31b,0x755)+ini_mark['split']('@')[0x8*0x4ab+-0x3a0+-0x4*0x86e],allmenuk,[_0x201503,_0x478418],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0xb83,0xb7e,0xd1d,0xb41)+'nu'){console[_0x3f34aa(0x5d6,0x298,0x51e,0x77)](color(time,_0x1da9f3(0x406,0x14c,0x8f3,0x7f2)),color(_0x1da9f3(0x33d,-0x4d,0x649,0x61a)+_0x3f34aa(0x3b5,0x4bc,0x4f,0x302)+_0x1da9f3(0x46d,0x284,0x742,0x596)));var downloadlsl=await getBuffer('https://te'+_0x3f34aa(-0x110,0x258,0x204,-0x19b)+'ile/97b236'+_0x3f34aa(0x918,0x28e,0x483,0x7e4)+_0x3f34aa(0x1be,0x601,0x66f,0x4ad));const _0x56837a={};_0x56837a[_0x3f34aa(0x46e,0x74d,0x9c5,0xc80)+'t']=_0x3f34aa(0x42c,0xb67,0x97d,0x9f8);const _0x73e2e8={};_0x73e2e8[_0x1da9f3(0x4c3,0x9c2,0x750,0x864)]='Command',_0x73e2e8['buttonText']=_0x56837a,_0x73e2e8[_0x1da9f3(0x847,0xc27,0xca7,0xb21)]=0x1;const _0x122652={};_0x122652[_0x1da9f3(0xb79,0xf02,0x72d,0xd7b)+'t']=_0x1da9f3(0xb3e,0xe5d,0xed0,0xd93);const _0x5bb6fc={};_0x5bb6fc[_0x1da9f3(0x4c3,0x6e2,0x81d,0x6d2)]=_0x3f34aa(0x12f,0x3db,0x12d,-0xb0),_0x5bb6fc['buttonText']=_0x122652,_0x5bb6fc[_0x1da9f3(0x847,0xa61,0xa39,0x5fc)]=0x1,sendButLocation(from,downloadmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x1da9f3(0x97b,0x9e9,0x8c4,0x54a)]('@')[-0x1175+0x1390+0x4d*-0x7],downloadlsl,[_0x73e2e8,_0x5bb6fc],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0xade,0xf8d,0xef8,0x1024)){console[_0x1da9f3(0x6d2,0x708,0xae5,0x8bc)](color(time,_0x3f34aa(0x797,0x22,0x252,0x56b)),color('List\x20messa'+_0x3f34aa(0x72d,0x86d,0x4f4,0x75c)));var funmenujs=await getBuffer(_0x3f34aa(0x19f,0x531,0x22c,-0x25f)+'legra.ph/f'+'ile/b14ea2'+'6a8f7fc99d'+_0x3f34aa(0x7b3,0x850,0x89c,0x3b0));const _0x392a3a={};_0x392a3a['displayTex'+'t']=_0x1da9f3(0xb31,0x662,0xd46,0xbe2);const _0x5911fd={};_0x5911fd['buttonId']='Command',_0x5911fd[_0x3f34aa(0x39a,0xba2,0x6ef,0x53d)]=_0x392a3a,_0x5911fd[_0x1da9f3(0x847,0xd36,0xb58,0x6f9)]=0x1;const _0x34515e={};_0x34515e[_0x3f34aa(0xcd9,0xb14,0x9c5,0xac9)+'t']=_0x3f34aa(0x805,0xa93,0x98a,0xbc7);const _0x2cb769={};_0x2cb769[_0x1da9f3(0x4c3,0x8c8,0x77b,0x14)]=_0x1da9f3(0x2e1,0x5bb,0x2b1,0x315),_0x2cb769[_0x1da9f3(0x8a3,0x864,0x689,0xd65)]=_0x34515e,_0x2cb769['type']=0x1,sendButLocation(from,funmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,0x718,0x5a9,-0xa8)+ini_mark[_0x3f34aa(0x42e,0x2cb,0x7c7,0x3f1)]('@')[-0x20*0xa7+0x57d*-0x6+0xc2*0x47],funmenujs,[_0x5911fd,_0x2cb769],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='makermenu'){console[_0x1da9f3(0x6d2,0x430,0x5d5,0x2e2)](color(time,_0x3f34aa(0x69d,0x280,0x252,0x5d9)),color(_0x3f34aa(0x3d8,-0x1af,0x189,-0x238)+_0x3f34aa(0x890,0xb70,0x8dd,0x3a3)+'nu'));var makermenksk=await getBuffer(_0x1da9f3(0x3e0,0x41f,0x17,0x777)+'legra.ph/f'+_0x3f34aa(0x809,0x348,0x676,0x276)+'af9c61265d'+_0x1da9f3(0xb76,0x872,0x724,0x815));const _0x5beb3a={};_0x5beb3a[_0x1da9f3(0xb79,0xed4,0x9b5,0x10d2)+'t']='⋮☰\x20BACK';const _0x1131d4={};_0x1131d4['buttonId']=_0x3f34aa(0x4c3,0x108,0x8c,-0x4b2),_0x1131d4[_0x1da9f3(0x8a3,0x6d4,0x8fc,0xb20)]=_0x5beb3a,_0x1131d4[_0x3f34aa(0x31c,0x164,0x693,0x39d)]=0x1;const _0x28c43a={};_0x28c43a[_0x1da9f3(0xb79,0x702,0x67f,0x974)+'t']=_0x3f34aa(0x8d6,0x5ef,0x98a,0xc2a);const _0xf5cd26={};_0xf5cd26['buttonId']=_0x3f34aa(-0x320,0x5d0,0x12d,0x380),_0xf5cd26['buttonText']=_0x28c43a,_0xf5cd26['type']=0x1,sendButLocation(from,makermenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(-0x302,-0x17e,0x1b6,0x7a)+ini_mark[_0x3f34aa(0xa8e,0xc63,0x7c7,0x9b3)]('@')[0x3ed+0x1454+-0x1841],makermenksk,[_0x1131d4,_0xf5cd26],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}function _0x1da9f3(_0x116671,_0x12d35b,_0x2d33f1,_0x4fe979){return _0xfa31(_0x116671-0x18,_0x12d35b);}if(sub_yt_zeeone_ofc=='textpromen'+'u'){console[_0x1da9f3(0x6d2,0x40c,0xb04,0x1a5)](color(time,_0x1da9f3(0x406,0x950,0x18d,0x311)),color('List\x20messa'+'ge\x20textpro'+'menu'));var textjskzb=await getBuffer(_0x3f34aa(0x4a,0x630,0x22c,0x60)+_0x1da9f3(0x3b8,0x3c,0x2d9,0x807)+'ile/148e7a'+'f719b8edb2'+_0x1da9f3(0x6dc,0xbae,0xbbe,0xb5e));const _0x260637={};_0x260637[_0x3f34aa(0x7ef,0x974,0x9c5,0x943)+'t']=_0x3f34aa(0x4c1,0x4c5,0x97d,0xdc4);const _0x18e9fc={};_0x18e9fc[_0x3f34aa(0x1de,0x70e,0x30f,0x71b)]='Command',_0x18e9fc[_0x1da9f3(0x8a3,0x74f,0x418,0x47d)]=_0x260637,_0x18e9fc[_0x3f34aa(0x36c,0x19c,0x693,0x509)]=0x1;const _0x10a7f4={};_0x10a7f4[_0x1da9f3(0xb79,0xbf3,0x754,0xafb)+'t']=_0x1da9f3(0xb3e,0xdcb,0xe0d,0xa50);const _0x215cca={};_0x215cca['buttonId']=_0x3f34aa(0x2fc,0x2e,0x12d,-0x3c4),_0x215cca[_0x1da9f3(0x8a3,0x794,0xc5e,0x997)]=_0x10a7f4,_0x215cca[_0x1da9f3(0x847,0x3ba,0x9c2,0x92b)]=0x1,sendButLocation(from,textpromenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,-0x10c,0x339,0x84a)+ini_mark['split']('@')[0x219*0x3+0x25f*-0x7+0xa4e],textjskzb,[_0x18e9fc,_0x215cca],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='photooxyme'+'nu'){console[_0x1da9f3(0x6d2,0x3a4,0x8d1,0x203)](color(time,_0x1da9f3(0x406,0x37a,0x20f,-0xac)),color(_0x1da9f3(0x33d,0x80,0x320,0x53e)+_0x1da9f3(0x29e,0x6d,0x140,0x518)+_0x3f34aa(0x897,0xc7e,0x7dd,0x62b)));var pdkdk=await getBuffer(_0x3f34aa(0x681,0x1aa,0x22c,0x25c)+_0x1da9f3(0x3b8,0x3f6,0x4e9,0xc7)+_0x3f34aa(0x4fc,0x7c4,0x64c,0xb74)+'5c293c4fd5'+_0x3f34aa(0x35b,0x614,0x814,0x5aa));const _0x5825f8={};_0x5825f8[_0x1da9f3(0xb79,0x9c7,0xc48,0xe8b)+'t']=_0x3f34aa(0x80b,0xb09,0x97d,0x95f);const _0x526408={};_0x526408[_0x1da9f3(0x4c3,0x4b0,0x7f1,0x649)]=_0x3f34aa(-0x31a,-0x139,0x8c,0x2c2),_0x526408['buttonText']=_0x5825f8,_0x526408[_0x1da9f3(0x847,0xa20,0xba7,0x3e2)]=0x1;const _0x2c6b4c={};_0x2c6b4c['displayTex'+'t']='✓\x20OWNER';const _0x2e87ca={};_0x2e87ca[_0x3f34aa(0x834,0x15b,0x30f,0x78a)]='owner',_0x2e87ca['buttonText']=_0x2c6b4c,_0x2e87ca['type']=0x1,sendButLocation(from,photooxymenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x463,0x299,0x1b6,-0xff)+ini_mark[_0x1da9f3(0x97b,0x87e,0x580,0x574)]('@')[-0x1*-0x15fd+0xf1a+0x76b*-0x5],pdkdk,[_0x526408,_0x2e87ca],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x55c,0xac0,0x89a,0xa88)){console['log'](color(time,_0x1da9f3(0x406,0x685,-0x18,0x1e4)),color(_0x1da9f3(0x33d,0x3f3,0x541,0x131)+_0x1da9f3(0x46c,0x903,0x59b,0x14e)+'nu'));var othendjke=await getBuffer(_0x3f34aa(0x277,0x4ca,0x22c,-0x188)+_0x3f34aa(-0x52,0x55,0x204,0x725)+_0x3f34aa(0x377,0x458,0x79e,0x50f)+'d89eeaefc6'+_0x3f34aa(-0x1dc,0x121,0x85,0x15));const _0x2d5e94={};_0x2d5e94[_0x1da9f3(0xb79,0xcd2,0xcc9,0x65a)+'t']='⋮☰\x20BACK';const _0x5e52ee={};_0x5e52ee[_0x1da9f3(0x4c3,0x7a9,0x13c,0x38c)]=_0x1da9f3(0x240,-0x1e3,-0x1e4,0x187),_0x5e52ee[_0x1da9f3(0x8a3,0xd73,0x433,0x417)]=_0x2d5e94,_0x5e52ee['type']=0x1;const _0x138e21={};_0x138e21[_0x1da9f3(0xb79,0xff3,0xa80,0xf83)+'t']='✓\x20OWNER';const _0x28e240={};_0x28e240[_0x3f34aa(0x7d8,0x29c,0x30f,0x6ff)]=_0x1da9f3(0x2e1,0x7f6,0x551,0x33d),_0x28e240['buttonText']=_0x138e21,_0x28e240['type']=0x1,sendButLocation(from,othermenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,0x81,0x134,-0x18f)+ini_mark[_0x1da9f3(0x97b,0xc37,0x63e,0xe8f)]('@')[0x29*0x2a+0x1*-0x2383+0x1*0x1cc9],othendjke,[_0x5e52ee,_0x28e240],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0xa1e,0x9c5,0x79e,0xb2b)){console['log'](color(time,_0x1da9f3(0x406,0x829,0x1cc,0x31e)),color(_0x1da9f3(0x33d,-0x1da,0x5ca,0x173)+_0x1da9f3(0x8c6,0xa40,0x7e6,0xd88)+'nu'));var ownermenjdu=await getBuffer(_0x3f34aa(0x245,-0x1e9,0x22c,0x229)+_0x1da9f3(0x3b8,0x16e,0x373,0x47c)+_0x1da9f3(0x785,0x348,0x97e,0x720)+_0x1da9f3(0x527,0x12e,0x2fa,0x2f0)+'99c5f.jpg');const _0x9a4a22={};_0x9a4a22['displayTex'+'t']=_0x3f34aa(0xc5e,0x461,0x97d,0xb26);const _0x3792e3={};_0x3792e3[_0x1da9f3(0x4c3,0x8fa,0x91,0x255)]=_0x1da9f3(0x240,-0x99,0x638,0x32f),_0x3792e3[_0x1da9f3(0x8a3,0xb52,0x968,0xb9e)]=_0x9a4a22,_0x3792e3['type']=0x1;const _0x5693e2={};_0x5693e2[_0x1da9f3(0xb79,0x8af,0x99e,0xebc)+'t']='✓\x20OWNER';const _0x313f8b={};_0x313f8b[_0x1da9f3(0x4c3,0x4b6,-0x2b,0x6fa)]=_0x3f34aa(0x7c,0x280,0x12d,0x51),_0x313f8b[_0x1da9f3(0x8a3,0xdd9,0xa5f,0x5cd)]=_0x5693e2,_0x313f8b[_0x3f34aa(0xa1a,0x9e9,0x693,0x2a1)]=0x1,sendButLocation(from,ownermenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,0x5ac,0x5b0,0x53b)+ini_mark[_0x1da9f3(0x97b,0xd1a,0xac1,0x696)]('@')[-0x9bc+-0x46c*0x3+0x2e*0x80],ownermenjdu,[_0x3792e3,_0x313f8b],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x3b5,0x59f,0x5e5,0x74d)+'u'){console[_0x1da9f3(0x6d2,0xb76,0x635,0x484)](color(time,_0x3f34aa(0x54b,0x67b,0x252,0x753)),color(_0x3f34aa(0x52c,0x6ae,0x189,0x324)+'ge\x20storage'+'menu'));var storagemsn=await getBuffer(_0x1da9f3(0x3e0,0x83,0x7b8,0x6f)+'legra.ph/f'+_0x3f34aa(0x8e5,0x3c6,0x812,0xac6)+_0x3f34aa(0x520,-0x3ed,0xd9,0x5d6)+_0x3f34aa(0x5ef,0x47e,0x8a1,0xc7b));const _0x46b35c={};_0x46b35c[_0x1da9f3(0xb79,0x7c8,0x1076,0x976)+'t']=_0x3f34aa(0x4ea,0xb37,0x97d,0xdd7);const _0x2e2b01={};_0x2e2b01['buttonId']=_0x3f34aa(0x92,-0x79,0x8c,0x3b3),_0x2e2b01[_0x3f34aa(0x7e3,0x1d8,0x6ef,0x768)]=_0x46b35c,_0x2e2b01[_0x1da9f3(0x847,0x4a8,0xd7f,0x8f9)]=0x1;const _0x7619d2={};_0x7619d2[_0x3f34aa(0xdde,0xe29,0x9c5,0x6e4)+'t']=_0x1da9f3(0xb3e,0x9ed,0x100e,0x69d);const _0x3eac6c={};_0x3eac6c['buttonId']=_0x3f34aa(0x25c,0x58e,0x12d,0x22b),_0x3eac6c[_0x3f34aa(0x91e,0x3db,0x6ef,0x6e3)]=_0x7619d2,_0x3eac6c[_0x3f34aa(0x36b,0x41e,0x693,0x7d4)]=0x1,sendButLocation(from,storagemenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,-0x5a,-0xfd,0x623)+ini_mark['split']('@')[0x13c3*0x1+0x244+-0x1607],storagemsn,[_0x2e2b01,_0x3eac6c],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='tagmenu'){console[_0x1da9f3(0x6d2,0x7d1,0xb50,0x449)](color(time,'magenta'),color(_0x1da9f3(0x33d,0x36f,-0xd1,0x3ed)+'ge\x20tagmenu'));var tagmwnjn=await getBuffer(_0x1da9f3(0x3e0,0x62e,-0x5e,0x4d2)+_0x3f34aa(0x10f,-0x181,0x204,0x65a)+_0x3f34aa(-0x3e5,0x72,0x8f,0x153)+_0x3f34aa(0x664,0x854,0x779,0x320)+_0x3f34aa(0x63b,0xa66,0xa65,0xf56));const _0x4d8282={};_0x4d8282[_0x3f34aa(0x77e,0xd71,0x9c5,0xc73)+'t']='⋮☰\x20BACK';const _0x4ef079={};_0x4ef079['buttonId']=_0x3f34aa(-0xe9,-0x278,0x8c,-0x162),_0x4ef079[_0x3f34aa(0x77b,0x636,0x6ef,0x683)]=_0x4d8282,_0x4ef079[_0x1da9f3(0x847,0xc65,0x464,0xd76)]=0x1;const _0x864de0={};_0x864de0['displayTex'+'t']='✓\x20OWNER';const _0x56433b={};_0x56433b[_0x3f34aa(0x630,0x5a5,0x30f,-0x57)]='owner',_0x56433b[_0x3f34aa(0x264,0x5cb,0x6ef,0x762)]=_0x864de0,_0x56433b[_0x1da9f3(0x847,0x63c,0xc37,0x7b7)]=0x1,sendButLocation(from,tagmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0xf,-0x74,0x1b6,-0x2c3)+ini_mark['split']('@')[-0x1ef7*-0x1+-0x2*-0x45a+-0x27ab],tagmwnjn,[_0x4ef079,_0x56433b],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='upmenu'){console[_0x3f34aa(0x28e,0x563,0x51e,0x749)](color(time,_0x1da9f3(0x406,0xd3,0x920,0xf6)),color(_0x1da9f3(0x33d,0xb2,0x6c3,0x7f7)+_0x1da9f3(0xac3,0xd0a,0xac6,0xc87)));var upmenuj=await getBuffer(_0x1da9f3(0x3e0,0x445,0x879,0x13d)+_0x1da9f3(0x3b8,0x144,0x4b4,0xbb)+_0x3f34aa(0x44a,0x4e9,0x91b,0xa82)+_0x1da9f3(0x348,0x8a,0x5f4,0x48a)+'b9509.jpg');const _0x335bd7={};_0x335bd7[_0x1da9f3(0xb79,0x87e,0xe57,0x6a9)+'t']=_0x1da9f3(0xb31,0xc4a,0xdea,0x94a);const _0x423604={};_0x423604[_0x3f34aa(-0x1c,0x81d,0x30f,-0x218)]=_0x3f34aa(0x576,0x179,0x8c,0x2b2),_0x423604['buttonText']=_0x335bd7,_0x423604[_0x1da9f3(0x847,0x70c,0x882,0xb45)]=0x1;const _0x3914e7={};_0x3914e7[_0x1da9f3(0xb79,0xe11,0x1076,0xf1f)+'t']=_0x3f34aa(0xd29,0x958,0x98a,0xb19);const _0xef4018={};_0xef4018[_0x1da9f3(0x4c3,0x6d2,0x460,0xa1c)]=_0x1da9f3(0x2e1,0x52e,0x4e2,0x13c),_0xef4018['buttonText']=_0x3914e7,_0xef4018['type']=0x1,sendButLocation(from,upmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x1da9f3(0x97b,0x7d2,0xc92,0xde4)]('@')[-0x1*0x114c+0x1973+-0x827],upmenuj,[_0x423604,_0xef4018],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x87,-0x283,0x13,0x401)+'n'){console[_0x1da9f3(0x6d2,0xa54,0xb79,0x7fa)](color(time,'magenta'),color(_0x1da9f3(0x33d,0x2b5,0xd0,-0x183)+_0x3f34aa(0x9ee,0x39c,0x4bb,0x948)+_0x1da9f3(0x927,0x4ee,0xbd4,0xdb5)));var cecanksk=await getBuffer('https://te'+_0x3f34aa(0x98,0xa0,0x204,0x26f)+_0x3f34aa(0xaec,0x4a9,0x80a,0x9b2)+'496a8748f2'+_0x1da9f3(0x6ac,0x77a,0x1af,0x52a));const _0x20d387={};_0x20d387[_0x3f34aa(0xd75,0xa70,0x9c5,0xb8d)+'t']=_0x3f34aa(0x96a,0x9c8,0x97d,0xe14);const _0x5b5992={};_0x5b5992['buttonId']=_0x3f34aa(-0x301,-0x398,0x8c,0x1c6),_0x5b5992[_0x1da9f3(0x8a3,0xda1,0xd8e,0x59c)]=_0x20d387,_0x5b5992[_0x3f34aa(0x870,0x30a,0x693,0xbed)]=0x1;const _0x25168a={};_0x25168a[_0x1da9f3(0xb79,0xc99,0xc17,0x64d)+'t']='✓\x20OWNER';const _0x429330={};_0x429330[_0x1da9f3(0x4c3,0x7ed,0x2f4,0x264)]=_0x1da9f3(0x2e1,-0x1e6,-0x175,0x4e6),_0x429330[_0x1da9f3(0x8a3,0x9c6,0x474,0x5b4)]=_0x25168a,_0x429330[_0x1da9f3(0x847,0xbc2,0xa2e,0x4ca)]=0x1,sendButLocation(from,cecanmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x1da9f3(0x36a,0x2f9,0x735,-0x121)+ini_mark[_0x1da9f3(0x97b,0x4c7,0x88f,0x710)]('@')[-0x236e+-0xab5+0x1f*0x17d],cecanksk,[_0x5b5992,_0x429330],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x8f4,0xa4c,0x84a,0x86f)+'n'){console[_0x1da9f3(0x6d2,0x281,0xa9a,0x61f)](_0x1da9f3(0xab2,0xe12,0xd34,0x9e1)+_0x3f34aa(0x736,0x91a,0x58f,0xaa0),'[\x1b[1;32m\x20C'+_0x3f34aa(0x3dd,-0xc1,0x461,0x22a)+']',wib,color(_0x3f34aa(0x36e,0x468,0x2e3,-0x193)+_0x3f34aa(0x920,0x83a,0x4bb,0x4c2)+_0x1da9f3(0x8c8,0xd2e,0xd5b,0xd8b)));var coganksk=await getBuffer('https://te'+_0x1da9f3(0x3b8,0x797,0x31f,0x15f)+_0x3f34aa(0x57e,0xe22,0xa9e,0x794)+_0x1da9f3(0x22b,0x715,0x592,0x207)+_0x3f34aa(0x314,-0x1c8,0x68,0x41e));const _0x357444={};_0x357444['displayTex'+'t']=_0x1da9f3(0xb31,0xe77,0xf03,0x6d2);const _0x74208e={};_0x74208e[_0x1da9f3(0x4c3,0x551,0x3cb,-0x10)]=_0x1da9f3(0x240,0x74d,0x42f,0x60d),_0x74208e[_0x3f34aa(0x687,0x37e,0x6ef,0x1f1)]=_0x357444,_0x74208e[_0x1da9f3(0x847,0x6a7,0x3e3,0xb77)]=0x1;const _0x5d1ca2={};_0x5d1ca2[_0x1da9f3(0xb79,0x891,0xabc,0xe15)+'t']=_0x1da9f3(0xb3e,0xedf,0xfa6,0xbbe);const _0xfbf16f={};_0xfbf16f[_0x1da9f3(0x4c3,0x58c,0x640,0x46f)]=_0x3f34aa(-0x3ff,0x2b8,0x12d,-0x3d4),_0xfbf16f['buttonText']=_0x5d1ca2,_0xfbf16f[_0x3f34aa(0x36e,0x443,0x693,0x5ee)]=0x1,sendButLocation(from,coganmenu(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x3f34aa(0x98f,0xb9d,0x7c7,0x77c)]('@')[0x599+-0xf*-0x27f+-0x626*0x7],coganksk,[_0x74208e,_0xfbf16f],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x3f3,0x46b,0x30e,0x859)+_0x1da9f3(0x622,0x31d,0x278,0x585)){console[_0x1da9f3(0x6d2,0x92d,0xbc5,0xa8f)](color(time,_0x1da9f3(0x406,0x104,0x6bc,0x155)),color('List\x20messa'+_0x3f34aa(0xc63,0x80b,0x96a,0x50a)+'m\x20sticker'));var telesjtje=await getBuffer(_0x3f34aa(-0x235,-0x63,0x22c,0x518)+_0x3f34aa(0x2d5,-0x34e,0x204,-0x1b0)+_0x1da9f3(0x6df,0x246,0x207,0xb58)+_0x1da9f3(0x985,0xd82,0x772,0xd99)+_0x3f34aa(0x7fb,0x36d,0x739,0xc0f));const _0x1838e7={};_0x1838e7[_0x3f34aa(0x8e6,0xd19,0x9c5,0x60e)+'t']=_0x3f34aa(0x48f,0xa88,0x97d,0xa5b);const _0x12d888={};_0x12d888[_0x3f34aa(0x85a,-0xee,0x30f,0x325)]=_0x1da9f3(0x240,0x64b,0x249,0x707),_0x12d888[_0x3f34aa(0xa41,0x4e4,0x6ef,0x522)]=_0x1838e7,_0x12d888[_0x1da9f3(0x847,0xa4b,0x777,0x429)]=0x1;const _0x3bee2e={};_0x3bee2e[_0x3f34aa(0xc10,0x83f,0x9c5,0xc65)+'t']=_0x3f34aa(0x5d5,0x7d7,0x98a,0xded);const _0x2948d2={};_0x2948d2[_0x3f34aa(0x2ec,0x7ff,0x30f,-0xb6)]='owner',_0x2948d2[_0x1da9f3(0x8a3,0xa2a,0x818,0xabf)]=_0x3bee2e,_0x2948d2['type']=0x1,sendButLocation(from,telestiker(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x370,0x6af,0x1b6,0x514)+ini_mark[_0x3f34aa(0xa57,0x461,0x7c7,0xc43)]('@')[0x54e*-0x7+-0x6b5*-0x5+-0x133*-0x3],telesjtje,[_0x12d888,_0x2948d2],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x6ea,0xab0,0x238,0x997)+'ct'){console[_0x3f34aa(0x5c8,0x116,0x51e,0x552)](color(time,_0x3f34aa(0x66f,0x43b,0x252,0x6e3)),color(_0x1da9f3(0x33d,0x6b4,0x7c4,0x513)+_0x3f34aa(0x6ca,0x8b1,0x9f7,0xc77)+_0x3f34aa(0x8cb,0x4af,0x792,0xac6)));var imagekskwo=await getBuffer('https://te'+_0x3f34aa(-0x33f,-0x34d,0x204,-0x47)+_0x3f34aa(0x672,0x19e,0x2e1,0x6df)+'2d0c2195ab'+_0x3f34aa(0x4e5,0x422,0x900,0xa2a));const _0x10ad0a={};_0x10ad0a[_0x1da9f3(0xb79,0x807,0xc26,0xac1)+'t']='⋮☰\x20BACK';const _0x407ce6={};_0x407ce6[_0x3f34aa(0x33b,-0x1a9,0x30f,-0xce)]=_0x3f34aa(-0x18a,0x445,0x8c,-0x414),_0x407ce6[_0x1da9f3(0x8a3,0x800,0xb2b,0x4af)]=_0x10ad0a,_0x407ce6['type']=0x1;const _0x35cce1={};_0x35cce1[_0x1da9f3(0xb79,0xf98,0x821,0x7da)+'t']=_0x1da9f3(0xb3e,0xdfc,0xbb1,0xb86);const _0x4dbead={};_0x4dbead[_0x1da9f3(0x4c3,0x6b2,0x5cc,0x90)]=_0x3f34aa(0x3ae,0x388,0x12d,-0x1ca),_0x4dbead[_0x1da9f3(0x8a3,0x994,0x683,0xd93)]=_0x35cce1,_0x4dbead[_0x1da9f3(0x847,0x9ad,0x9fd,0xc7a)]=0x1,sendButLocation(from,image_effect(prefix),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x3f34aa(0x36c,0x749,0x7c7,0xcb3)]('@')[-0x1b52+-0x1819+0x336b*0x1],imagekskwo,[_0x407ce6,_0x4dbead],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc==_0x1da9f3(0x1df,-0x22e,0xe5,0x44d)+'e'){console['log'](color(time,_0x3f34aa(0x773,0x763,0x252,0x92)),color(_0x3f34aa(0x195,-0x261,0x189,0x613)+'ge\x20source\x20'+'code'));var sourbwbbz=await getBuffer('https://te'+_0x1da9f3(0x3b8,0x330,-0x62,-0x154)+'ile/e2c139'+_0x3f34aa(0xcca,0xed9,0x9c4,0xd1d)+_0x1da9f3(0xc5d,0xee9,0x9df,0x114c));const _0x387215={};_0x387215['displayTex'+'t']=_0x1da9f3(0x60d,0x418,0x930,0x592);const _0x1b3dff={};_0x1b3dff[_0x1da9f3(0x4c3,0x765,0x719,-0x30)]=_0x3f34aa(0x3e5,0x19b,0x8c,0x3b1),_0x1b3dff[_0x3f34aa(0xa72,0xb50,0x6ef,0x898)]=_0x387215,_0x1b3dff[_0x1da9f3(0x847,0x78f,0x472,0x625)]=0x1;const _0xc215cb={};_0xc215cb[_0x3f34aa(0xe97,0x641,0x9c5,0xf00)+'t']=_0x3f34aa(0x7d0,0xb4c,0x98a,0x4fa);const _0xb0e780={};_0xb0e780[_0x1da9f3(0x4c3,0x6eb,0x620,0x27a)]=_0x1da9f3(0x2e1,0x424,-0x24,0x538),_0xb0e780[_0x1da9f3(0x8a3,0xb65,0x39f,0x3b0)]=_0xc215cb,_0xb0e780[_0x1da9f3(0x847,0xc6b,0x755,0x6e3)]=0x1,sendButLocation(from,'🏅\x20Source\x20S'+'cript\x20Bot\x20'+_0x3f34aa(0x9ce,0x3ac,0x7a2,0x3fb)+_0x3f34aa(0x2de,0x9b1,0x6a3,0xbef)+_0x1da9f3(0x3cc,0x1a4,0xd9,-0xc4)+_0x1da9f3(0x745,0x2b0,0xa2a,0x452),''+tampilTanggal+enter+tampilWaktu+enter+enter+'Regard\x20@'+ini_mark[_0x1da9f3(0x97b,0xcf5,0xdab,0x826)]('@')[0x89*0x2f+0x2b*0x8+-0x1a7f],sourbwbbz,[_0x1b3dff,_0xb0e780],{'contextInfo':{'mentionedJid':[ini_mark]}}),await limitAdd(sender,limit);}if(sub_yt_zeeone_ofc=='Big\x20Thanks'+'\x20to'){const VIkAXK=_0x1da9f3(0x780,0x5d5,0xa8a,0xb2f)['split']('|');let FCFzUD=0x114c+-0x13*0x26+-0x22*0x6d;while(!![]){switch(VIkAXK[FCFzUD++]){case'0':var imgUrl=imgList[Math['floor'](Math[_0x1da9f3(0x957,0xd18,0x709,0xe54)]()*imgList[_0x1da9f3(0xae2,0x998,0x100f,0x929)])];continue;case'1':const _0x15f1ea={};_0x15f1ea['title']=''+ucapannya2,_0x15f1ea[_0x1da9f3(0x9ab,0xbdf,0x57d,0x74d)]='Subscribe\x20'+_0x1da9f3(0x791,0xa0a,0x8e2,0xc67),_0x15f1ea[_0x3f34aa(0x6f9,0x5f3,0x628,0x6e4)+'e']='PHOTO',_0x15f1ea[_0x3f34aa(0x40e,0x503,0x34b,0x532)+'rl']=''+imgUrl,_0x15f1ea['thumbnail']='',_0x15f1ea[_0x3f34aa(0x359,0x473,0x505,0x816)]=_0x1da9f3(0x754,0xbca,0x9c5,0x962)+_0x3f34aa(0xe6,-0x159,0x129,-0xb1)+'channel/UC'+_0x3f34aa(0x64,0x1af,0x50c,0x2e2)+_0x3f34aa(0x6e2,-0x112,0x36d,0x7b3)+'lA';const _0x3dd32a={};_0x3dd32a[_0x1da9f3(0x5e8,0x67b,0x161,0x13c)]=_0x3f34aa(0x1d2,0xa3c,0x71c,0x81c),_0x3dd32a[_0x1da9f3(0x3eb,0x539,0x30f,0x3b7)+'Score']=0x3b9aca00,_0x3dd32a['isForwarde'+'d']=!![],_0x3dd32a['sendEpheme'+_0x1da9f3(0xb5a,0xad8,0xa96,0x99d)]=!![],_0x3dd32a['externalAd'+'Reply']=_0x15f1ea;const _0x26d8db={};_0x26d8db['mentionedJ'+'id']=[ini_mark],alpha[_0x1da9f3(0x21d,-0x26a,0x6d,-0x7a)+'e'](from,tqto,text,{'thumbnail':fs[_0x1da9f3(0x2c5,0x116,0x710,0x12d)+'nc']('image/'+thumbnail),'caption':_0x1da9f3(0x8d0,0xc1b,0xd50,0x79a),'contextInfo':_0x3dd32a,'quoted':ftoko,'contextInfo':_0x26d8db});continue;case'2':console['log'](color(time,_0x1da9f3(0x406,0x389,0x69d,0x6fd)),color(_0x3f34aa(0x1c3,0x551,0x189,0x211)+'ge\x20Big\x20Tha'+_0x3f34aa(0x773,0x74b,0x39d,0x3f0)));continue;case'3':tqto=_0x3f34aa(0x4ad,0x2a7,0x44e,0x359)+_0x1da9f3(0x646,0x5d3,0xba0,0x104)+_0x1da9f3(0x6d3,0x1e0,0x4b7,0x9e5)+_0x3f34aa(0x69e,0x7c8,0x4a2,0x23b)+_0x1da9f3(0x4e0,-0x3d,0x2ed,-0x17)+_0x3f34aa(0x3f4,0x105,0x3c4,0x64e)+'\x20\x0a\x0aPowered'+_0x3f34aa(0x6ef,0x117,0x5be,0x7de)+ini_mark['split']('@')[0x14+-0x1da8+0x2*0xeca];continue;case'4':var imgList=[_0x1da9f3(0x4b6,0x40c,0x374,0x5b6)+_0x3f34aa(0x9da,0x40b,0x69a,0x624)+_0x1da9f3(0x5bc,0x819,0x95c,0x5e3)+'cv1.jpg',_0x3f34aa(-0x2f7,0x2d8,0x5,-0x2d5)+'top4top.io'+'/p_2032mwb'+_0x3f34aa(0x415,0xf,0x178,-0x108),_0x1da9f3(0x80d,0xa77,0x418,0x495)+_0x1da9f3(0x84e,0x526,0xd15,0x3e8)+'/p_2032cf1'+_0x1da9f3(0x95e,0x7fd,0x48e,0x72c),'https://f.'+_0x1da9f3(0x84e,0x33d,0x512,0x925)+_0x1da9f3(0x983,0x6cd,0x80a,0x651)+_0x3f34aa(-0x6b,0x358,0xb5,0x1b6),_0x3f34aa(0x661,0x366,0x3d0,0x3f9)+_0x3f34aa(0x3e4,0xb1b,0x69a,0x4a5)+_0x3f34aa(0xd6,0x97,0x466,0x5ee)+_0x3f34aa(-0x297,0x1b5,0xc3,-0x1b8),_0x1da9f3(0x41f,0x55a,0x41f,0x7fb)+'top4top.io'+_0x1da9f3(0x7e8,0xa25,0x659,0x93a)+_0x3f34aa(0x20,0x379,0x2be,0x7f6),'https://j.'+_0x3f34aa(0xb7d,0x7bc,0x69a,0x3eb)+_0x1da9f3(0xc09,0xc8c,0x853,0x1046)+_0x3f34aa(0xd54,0x619,0x880,0x4d9)];continue;}break;}}if(subscribezeeoneofc==_0x1da9f3(0x240,-0xd7,0x79a,0x115)){console['log'](color(time,'magenta'),color(_0x1da9f3(0xb92,0x71c,0x9f1,0x76c)+_0x1da9f3(0xab1,0x727,0xc9c,0xd0e)));const _0x3ae2b5={};_0x3ae2b5[_0x1da9f3(0x7c9,0xa6c,0xc3a,0xc61)]=_0x3f34aa(0xd0,0x4fc,0x4ea,0x4a8)+'ot',_0x3ae2b5['descriptio'+'n']=_0x1da9f3(0x1b2,-0x192,0x5d7,0x18a)+'is\x20if\x20you\x20'+'want\x20to\x20us'+_0x1da9f3(0x8af,0xb00,0x9f7,0x4a2)+'enu\x20comman'+'d_',_0x3ae2b5[_0x1da9f3(0xc08,0x10e1,0x1028,0xdc9)]='0';const _0x473f67={};_0x473f67['title']='🐣\x20All\x20menu'+'\x20bot',_0x473f67['rows']=[_0x3ae2b5];const _0xe548be={};_0xe548be[_0x1da9f3(0x7c9,0x9cf,0x351,0xcb1)]=_0x3f34aa(0x5c4,0x89f,0x9cf,0xc5a)+'nu',_0xe548be[_0x1da9f3(0x954,0x7ee,0xabf,0x4cd)+'n']=_0x1da9f3(0x1b2,0x13c,0x5b1,0x189)+_0x3f34aa(0xd8e,0xf48,0xa0c,0x67f)+'want\x20to\x20us'+_0x3f34aa(0x623,0x386,0x595,0x74)+_0x1da9f3(0x4a4,0x88b,0x256,0x698)+'ommand_',_0xe548be['rowId']='0';const _0x15eb5a={};_0x15eb5a[_0x3f34aa(0x62a,0x20b,0x615,0x3b3)]='Sub\x20Menu\x20k'+_0x1da9f3(0x758,0x224,0x4ad,0x65c),_0x15eb5a[_0x1da9f3(0x5a9,0x539,0x11f,0x959)]=[_0xe548be];const _0x55c680={};_0x55c680[_0x1da9f3(0x7c9,0xad4,0x515,0x426)]=_0x3f34aa(0xcd0,0x79e,0x863,0xa37),_0x55c680[_0x3f34aa(0x5a0,0xa72,0x7a0,0x797)+'n']=_0x1da9f3(0x1b2,0x1fb,0x4e2,-0xf5)+_0x3f34aa(0xbc5,0x836,0xa0c,0x946)+_0x1da9f3(0xbbb,0xf08,0x865,0xa05)+_0x3f34aa(0x525,0x7eb,0x4c2,0x10)+'pmenu\x20comm'+'and_',_0x55c680[_0x3f34aa(0x726,0xcf8,0xa54,0xcc4)]='0';const _0x3daea9={};_0x3daea9['title']=_0x3f34aa(0x69c,0x87d,0x58e,0xa84)+_0x3f34aa(0xd93,0xcd7,0x83f,0x884),_0x3daea9[_0x3f34aa(0x48a,0x659,0x3f5,0x647)]=[_0x55c680];const _0x1be811={};_0x1be811['title']='convertmen'+'u',_0x1be811[_0x1da9f3(0x954,0xc70,0x4ea,0xa37)+'n']=_0x3f34aa(-0x2b0,-0x3cd,-0x2,-0x2ed)+'is\x20if\x20you\x20'+_0x3f34aa(0xda1,0x625,0xa07,0xd2b)+_0x3f34aa(0x9cd,0x44b,0x68f,0x8d7)+_0x3f34aa(0x72c,0x68d,0x8c4,0xb7c)+_0x1da9f3(0x38a,0x2c3,0x33b,0x4ed),_0x1be811[_0x1da9f3(0xc08,0x114a,0xa85,0xb96)]='0';const _0x31b35e={};_0x31b35e['title']='Sub\x20Menu\x20k'+_0x3f34aa(-0x1e4,-0x3d3,0xa5,0x580),_0x31b35e[_0x3f34aa(0x618,0x16,0x3f5,0x3ba)]=[_0x1be811];const _0x2c7e29={};_0x2c7e29['title']=_0x1da9f3(0xade,0xaf0,0xe97,0xefc),_0x2c7e29[_0x1da9f3(0x954,0xc2c,0xe56,0xc9b)+'n']=_0x3f34aa(-0x24a,0x2d2,-0x2,0x25b)+_0x3f34aa(0xc0d,0xc57,0xa0c,0x97d)+'want\x20to\x20us'+_0x3f34aa(0x2ff,0x35a,0x54c,0x785)+_0x1da9f3(0x771,0x31d,0x849,0xbdf)+'d_',_0x2c7e29['rowId']='0';const _0x414d48={};_0x414d48['title']='Sub\x20Menu\x20k'+_0x3f34aa(0x692,0x664,0x24c,0x17e),_0x414d48['rows']=[_0x2c7e29];const _0x1aa4d2={};_0x1aa4d2[_0x3f34aa(0x26f,0x921,0x615,0xae8)]=_0x3f34aa(0x321,0x4f3,0x379,0x386),_0x1aa4d2[_0x3f34aa(0xb58,0xccf,0x7a0,0xbd2)+'n']=_0x3f34aa(-0x1ee,-0x386,-0x2,-0x3d4)+_0x3f34aa(0x728,0x76d,0xa0c,0x859)+_0x1da9f3(0xbbb,0xab9,0xaf5,0xd4d)+_0x1da9f3(0x311,0x7f4,-0x3d,0x40b)+_0x3f34aa(0x86f,0xa0f,0x660,0x82b)+'and_',_0x1aa4d2[_0x3f34aa(0x85a,0xf5a,0xa54,0xd58)]='0';const _0x1a78e4={};_0x1a78e4[_0x3f34aa(0x277,0x265,0x615,0x140)]=_0x3f34aa(0x601,0xabe,0x58e,0x7db)+'e-5',_0x1a78e4[_0x3f34aa(0x6ca,0x3cc,0x3f5,0x7a1)]=[_0x1aa4d2];const _0x505ea6={};_0x505ea6[_0x1da9f3(0x7c9,0x5da,0x503,0x29f)]=_0x3f34aa(0xb70,0xd61,0x89a,0xa87),_0x505ea6['descriptio'+'n']=_0x1da9f3(0x1b2,0x33b,0x38f,-0x35a)+_0x3f34aa(0x76d,0x5c6,0xa0c,0x786)+_0x1da9f3(0xbbb,0xd8b,0xc1a,0x7e3)+'e\x20the\x20othe'+_0x3f34aa(0x7cf,0x73a,0x660,0xafa)+_0x1da9f3(0x3ab,0x8b1,0xf2,0x117),_0x505ea6['rowId']='0';const _0x2697a0={};_0x2697a0[_0x1da9f3(0x7c9,0x2e4,0xa52,0xc93)]='Sub\x20Menu\x20k'+_0x1da9f3(0xb29,0x7c5,0x65e,0xd70),_0x2697a0[_0x1da9f3(0x5a9,0x6bf,0x9dc,0x977)]=[_0x505ea6];const _0xf6dcab={};_0xf6dcab[_0x3f34aa(0x90d,0x387,0x615,0x1bd)]=_0x1da9f3(0xa1e,0xe07,0x931,0xc21),_0xf6dcab[_0x3f34aa(0x506,0x74e,0x7a0,0x6e2)+'n']=_0x1da9f3(0x1b2,-0x1ad,0x221,0x676)+_0x3f34aa(0x4e6,0xa85,0xa0c,0xe84)+_0x3f34aa(0x9a5,0x4b0,0xa07,0xe62)+'e\x20the\x20owne'+_0x3f34aa(0x648,0x685,0x660,0x96d)+_0x1da9f3(0x3ab,0x5e,0x431,0x6bf),_0xf6dcab[_0x3f34aa(0x83e,0x636,0xa54,0xa3c)]='0';const _0x1f3f2f={};_0x1f3f2f[_0x3f34aa(0x6c0,0x6e6,0x615,0x79d)]=_0x3f34aa(0x810,0xe8,0x58e,0x855)+_0x1da9f3(0x35d,0x3a,-0x1ba,-0x9b),_0x1f3f2f[_0x3f34aa(0x85c,0x7c0,0x3f5,-0x144)]=[_0xf6dcab];const _0x318ea2={};_0x318ea2[_0x1da9f3(0x7c9,0x68b,0x2d3,0xa31)]=_0x3f34aa(0x55b,0xa66,0x5e5,0x82f)+'u',_0x318ea2[_0x1da9f3(0x954,0x7bd,0xe83,0x7ff)+'n']=_0x1da9f3(0x1b2,0x61,-0x239,0x397)+'is\x20if\x20you\x20'+'want\x20to\x20us'+_0x3f34aa(-0x216,0x411,0x2f1,0x806)+_0x3f34aa(0x72f,0x823,0x946,0xea1)+'mmand_',_0x318ea2['rowId']='0';const _0x517472={};_0x517472[_0x3f34aa(0x3cf,0x5c5,0x615,0x3f1)]=_0x3f34aa(0x54,0x318,0x58e,0x2ce)+_0x3f34aa(0x42a,0xc11,0x6c1,0x16b),_0x517472[_0x1da9f3(0x5a9,0x5ba,0x43a,0x60b)]=[_0x318ea2];const _0x22c7b8={};_0x22c7b8['title']=_0x3f34aa(0x110,0x732,0x2d7,0x59b),_0x22c7b8[_0x3f34aa(0xcc5,0xa0a,0x7a0,0xbce)+'n']='_choose\x20th'+_0x1da9f3(0xbc0,0x9f8,0x72e,0xa98)+_0x3f34aa(0x572,0x712,0xa07,0xe04)+'e\x20the\x20tagm'+'enu\x20comman'+'d_',_0x22c7b8[_0x1da9f3(0xc08,0xaa4,0x6d4,0xaad)]='0';const _0x538791={};_0x538791[_0x3f34aa(0xc9,0x91c,0x615,0xaf3)]=_0x1da9f3(0x742,0x59d,0x326,0x41e)+_0x1da9f3(0xc3f,0xfa6,0xa9a,0x9a3),_0x538791['rows']=[_0x22c7b8];const _0x3a8647={};_0x3a8647[_0x1da9f3(0x7c9,0xab1,0x495,0xa76)]='upmenu',_0x3a8647[_0x3f34aa(0xbaa,0x427,0x7a0,0xba0)+'n']=_0x3f34aa(-0x110,-0x40b,-0x2,-0x3ae)+'is\x20if\x20you\x20'+_0x3f34aa(0xd04,0x9b0,0xa07,0xe91)+_0x1da9f3(0x984,0x629,0x93a,0xa6e)+_0x3f34aa(0x461,0x8fa,0x7e1,0x94b)+'_',_0x3a8647['rowId']='0';const _0x1fdd26={};_0x1fdd26['title']='Sub\x20Menu\x20k'+_0x1da9f3(0xa46,0xeab,0x7e8,0xdaa),_0x1fdd26[_0x1da9f3(0x5a9,0x1e4,0x236,0x405)]=[_0x3a8647];const _0xf33312={};_0xf33312[_0x1da9f3(0x7c9,0xad5,0x3a6,0xb0c)]='gacha\x20ceca'+'n',_0xf33312[_0x3f34aa(0x7bc,0x88a,0x7a0,0xc0e)+'n']=_0x1da9f3(0x1b2,0x4be,0x45b,0x530)+_0x1da9f3(0xbc0,0x8a4,0xd8d,0x10b2)+_0x3f34aa(0x784,0x752,0xa07,0xc5f)+_0x1da9f3(0xa11,0x6ca,0xedf,0xb63)+'acecan\x20com'+_0x3f34aa(0x19a,0x78c,0x4c5,0x60f),_0xf33312[_0x1da9f3(0xc08,0x87b,0x817,0xe7c)]='0';const _0x229732={};_0x229732[_0x1da9f3(0x7c9,0xb5f,0xa40,0x4ea)]='Sub\x20Menu\x20k'+'e-11',_0x229732['rows']=[_0xf33312];const _0x4122f5={};_0x4122f5[_0x3f34aa(0x786,0x9e5,0x615,0x32a)]='telegram\x20s'+_0x1da9f3(0x622,0x430,0x397,0xb7b),_0x4122f5[_0x1da9f3(0x954,0xcbd,0xafd,0x834)+'n']=_0x3f34aa(-0x14b,0x3a7,-0x2,-0x4a2)+_0x1da9f3(0xbc0,0x10d3,0x803,0xa7d)+'want\x20to\x20us'+_0x3f34aa(0x80f,0x500,0x8b6,0xc09)+'stc\x20comman'+'d_',_0x4122f5[_0x3f34aa(0x7de,0xf1f,0xa54,0x7be)]='0';const _0xbb1d45={};_0xbb1d45[_0x1da9f3(0x7c9,0x28c,0x9f7,0xc08)]=_0x1da9f3(0x742,0x652,0x347,0x8d5)+'e-12',_0xbb1d45[_0x1da9f3(0x5a9,0x118,0x967,0x736)]=[_0x4122f5];const _0x22b20a={};_0x22b20a[_0x3f34aa(0x4ac,0x19a,0x615,0x3a4)]='nsfw\x20menu',_0x22b20a[_0x1da9f3(0x954,0x52a,0x49b,0x61b)+'n']=_0x1da9f3(0x1b2,0x642,-0x1f2,-0x2da)+_0x3f34aa(0xb36,0x7ae,0xa0c,0x6d5)+_0x3f34aa(0xd02,0xe24,0xa07,0x75f)+_0x1da9f3(0x8dc,0x760,0x955,0x44e)+'menu\x20comma'+_0x1da9f3(0x870,0x5b8,0x7f0,0x803),_0x22b20a[_0x3f34aa(0xcff,0x9a1,0xa54,0x7fb)]='0';const _0x248cd5={};_0x248cd5['title']=_0x3f34aa(0x9fe,0xe7,0x58e,0x324)+'e-13',_0x248cd5[_0x1da9f3(0x5a9,0x4de,0x430,0x7e8)]=[_0x22b20a];const _0x5da9bd={};_0x5da9bd[_0x1da9f3(0x7c9,0x480,0xc49,0x502)]=_0x3f34aa(0x6ee,0x3d2,0x209,0x3aa)+'u',_0x5da9bd['descriptio'+'n']=_0x1da9f3(0x1b2,0x3d6,-0x31c,0x4)+_0x1da9f3(0xbc0,0x100a,0xdc4,0x949)+'want\x20to\x20us'+_0x3f34aa(0x1d0,0x483,0x5e9,0x151)+_0x1da9f3(0x7bc,0x45f,0x408,0xa3a)+_0x1da9f3(0x38a,0x681,0x3f4,0x71f),_0x5da9bd['rowId']='0';const _0x22d10e={};_0x22d10e['title']=_0x1da9f3(0x742,0x331,0x7cb,0x406)+_0x3f34aa(0x52d,0x486,0x18c,-0x2df),_0x22d10e['rows']=[_0x5da9bd];const _0x3cf67c={};_0x3cf67c[_0x3f34aa(0x1ec,0x17e,0x615,0x25f)]=_0x3f34aa(0x6f7,0x5a7,0x536,0x84f)+'ct',_0x3cf67c[_0x3f34aa(0x7c4,0xbb7,0x7a0,0xa39)+'n']=_0x1da9f3(0x1b2,-0x31b,0xd5,0x330)+_0x1da9f3(0xbc0,0xe7b,0xde1,0xa62)+'want\x20to\x20us'+_0x1da9f3(0xb91,0x732,0xd8e,0x7c2)+_0x3f34aa(0x27a,0x70e,0x3d6,0x83d)+_0x1da9f3(0x771,0x601,0xc9d,0xccb)+'d_',_0x3cf67c[_0x3f34aa(0x8e1,0x5fc,0xa54,0x8d8)]='0';const _0x5d34b8={};_0x5d34b8[_0x3f34aa(0x341,0x7d5,0x615,0x74d)]=_0x1da9f3(0x742,0xc98,0x704,0xa54)+_0x3f34aa(0x286,0x5bf,0x30c,0x12d),_0x5d34b8[_0x3f34aa(0x49a,0x7f1,0x3f5,-0x5f)]=[_0x3cf67c];const _0x154ed9={};_0x154ed9[_0x1da9f3(0x7c9,0x5b5,0xb45,0x6e1)]=_0x3f34aa(0x5a3,0xab7,0x740,0x86f)+'n',_0x154ed9[_0x1da9f3(0x954,0x7e7,0xe90,0xb52)+'n']=_0x1da9f3(0x1b2,0x108,0x3a3,-0x18f)+_0x1da9f3(0xbc0,0xaeb,0x730,0x7c0)+_0x3f34aa(0x73a,0xe3a,0xa07,0xdcb)+_0x1da9f3(0xa11,0x4e6,0xe76,0xebe)+_0x1da9f3(0x67c,0x786,0x40d,0x340)+'mmand_',_0x154ed9[_0x1da9f3(0xc08,0xed4,0x1012,0xea9)]='0';const _0x33d945={};_0x33d945[_0x1da9f3(0x7c9,0x55f,0x829,0x735)]=_0x3f34aa(0x762,0x46f,0x58e,0x4f8)+_0x3f34aa(0x45c,0x7b3,0x370,0x895),_0x33d945['rows']=[_0x154ed9];const _0x112997={};_0x112997[_0x3f34aa(0xb29,0x69b,0x615,0xb39)]='quotesmenu',_0x112997[_0x3f34aa(0x6bf,0x303,0x7a0,0xca5)+'n']=_0x3f34aa(-0x436,-0xa8,-0x2,0x334)+_0x3f34aa(0xb4c,0x6a7,0xa0c,0xcb2)+_0x1da9f3(0xbbb,0xf9c,0xaa5,0xd5e)+_0x3f34aa(0x8c8,0xd25,0x991,0x8b0)+_0x3f34aa(0x29f,-0x50,0x438,0x9c)+'mand_',_0x112997[_0x3f34aa(0x66f,0xe24,0xa54,0xb5b)]='0';const _0x2e38bc={};_0x2e38bc['title']=_0x1da9f3(0x742,0xb5a,0xbdf,0x593)+'e-17',_0x2e38bc['rows']=[_0x112997];const _0x2290d0={};_0x2290d0['title']='animemenu',_0x2290d0[_0x1da9f3(0x954,0x4a9,0x84e,0xd09)+'n']='_choose\x20th'+_0x1da9f3(0xbc0,0xdc6,0xbe1,0xc35)+_0x1da9f3(0xbbb,0x7eb,0xa89,0xc82)+_0x3f34aa(0x51c,-0x56,0x430,0x36e)+_0x1da9f3(0xb2c,0xeb0,0xec1,0xa1b)+'and_',_0x2290d0[_0x3f34aa(0xcce,0xe7a,0xa54,0x8a7)]='0';const _0x42c8fd={};_0x42c8fd['title']=_0x3f34aa(0xa2f,0x214,0x58e,0x2c4)+'e-18',_0x42c8fd[_0x3f34aa(0x7b5,0x559,0x3f5,0xb7)]=[_0x2290d0];const _0x302fe={};_0x302fe[_0x3f34aa(0x86b,0x6e8,0x615,0xaed)]=_0x3f34aa(0x91c,0xaf5,0x60b,0x4a7)+'u',_0x302fe[_0x1da9f3(0x954,0xa63,0x768,0xcd8)+'n']='_choose\x20th'+'is\x20if\x20you\x20'+_0x1da9f3(0xbbb,0xaea,0xf5f,0x89f)+'e\x20the\x20text'+_0x3f34aa(-0x335,-0x2cb,0xbd,-0x185)+_0x3f34aa(0x6d0,-0x1ba,0x1d6,0x370),_0x302fe[_0x1da9f3(0xc08,0xba3,0xda3,0x107c)]='0';const _0x3b7794={};_0x3b7794[_0x3f34aa(0x4fd,0x11c,0x615,0x65e)]=_0x1da9f3(0x742,0x5ea,0x360,0x1ff)+'e-19',_0x3b7794['rows']=[_0x302fe];const _0x785a96={};_0x785a96[_0x3f34aa(0x7e6,0x5f9,0x615,0x3ad)]=_0x3f34aa(0x403,0x525,0x746,0x423)+'nu',_0x785a96[_0x3f34aa(0x8fe,0x5fa,0x7a0,0x3a4)+'n']='_choose\x20th'+_0x1da9f3(0xbc0,0xda6,0xc43,0x868)+_0x1da9f3(0xbbb,0xbb9,0xef2,0xb0f)+'e\x20the\x20phot'+_0x1da9f3(0x222,-0x1ce,0x54f,-0x4c)+'ommand_',_0x785a96[_0x3f34aa(0x7e4,0xba3,0xa54,0xd8c)]='0';const _0x5eba2a={};_0x5eba2a[_0x3f34aa(0x894,0x626,0x615,0x2cf)]=_0x1da9f3(0x742,0x67a,0x35e,0x686)+'e-20',_0x5eba2a[_0x1da9f3(0x5a9,0x381,0x6fe,0x3a9)]=[_0x785a96];const _0x569e53={};_0x569e53[_0x3f34aa(0x2e0,0xa56,0x615,0xa33)]='soundmenu',_0x569e53[_0x3f34aa(0xb9b,0xae4,0x7a0,0x8e5)+'n']='_choose\x20th'+_0x3f34aa(0xa3c,0xc30,0xa0c,0x831)+_0x1da9f3(0xbbb,0xa3b,0x860,0x8b1)+'e\x20the\x20soun'+_0x3f34aa(0x762,0x6d7,0x406,0x7e8)+_0x1da9f3(0x3ab,0x34f,0xb1,0x719),_0x569e53[_0x1da9f3(0xc08,0x76f,0xb21,0xb11)]='0';const _0x518128={};_0x518128['title']='Sub\x20Menu\x20k'+_0x3f34aa(0xbf9,0x5b1,0x8a0,0x66a),_0x518128[_0x1da9f3(0x5a9,0x84e,0x28d,0x7bd)]=[_0x569e53];const _0x531f95={};_0x531f95[_0x1da9f3(0x7c9,0xb10,0xa23,0xcd3)]=_0x1da9f3(0xc0f,0x97a,0x9be,0xa85),_0x531f95[_0x3f34aa(0x7b0,0x526,0x7a0,0x6de)+'n']=_0x3f34aa(-0x4e1,0x14d,-0x2,0x345)+_0x1da9f3(0xbc0,0x992,0xef3,0x1045)+_0x3f34aa(0xaac,0xec3,0xa07,0xc71)+_0x1da9f3(0xa02,0x5e9,0x5ef,0x723)+'ngmenu\x20com'+_0x3f34aa(0x59c,0x476,0x4c5,0x342),_0x531f95['rowId']='0';const _0x48ac6c={};_0x48ac6c[_0x1da9f3(0x7c9,0x7f9,0x6c3,0x477)]=_0x1da9f3(0x742,0xa46,0x697,0x38a)+'e-22',_0x48ac6c[_0x1da9f3(0x5a9,0xa2e,0x623,0x24c)]=[_0x531f95];const _0x16b353={};_0x16b353[_0x1da9f3(0x7c9,0x463,0xc76,0x9b4)]='source\x20cod'+'e',_0x16b353[_0x3f34aa(0xb95,0x761,0x7a0,0x27d)+'n']=_0x3f34aa(0xad8,0x580,0x7a9,0xb12)+_0x3f34aa(-0x140,0x5dd,0xd7,0x504)+_0x3f34aa(0x602,0x41d,0x826,0x455)+_0x3f34aa(0xd37,0x45b,0x8b2,0x658)+'e\x20code\x20thi'+'s\x20bot_',_0x16b353['rowId']='source\x20cod'+'e';const _0x532590={};_0x532590[_0x1da9f3(0x7c9,0xab7,0x941,0x838)]=_0x3f34aa(0x7be,0xa62,0x8e4,0xb6a)+_0x3f34aa(-0x4b8,-0x1bb,0x95,0x140),_0x532590[_0x1da9f3(0x5a9,0x5cc,0x90e,0x102)]=[_0x16b353];const _0x30e36f={};_0x30e36f[_0x3f34aa(0x5ef,0xb21,0x615,0x6ca)]='Big\x20Thanks'+_0x1da9f3(0x6e8,0xb64,0x390,0xa67),_0x30e36f['descriptio'+'n']=_0x3f34aa(0x420,0xa08,0x7a9,0x9b4)+_0x3f34aa(-0x460,0x186,0xd7,0x413)+_0x3f34aa(0x2dd,0x715,0x826,0x6f6)+_0x1da9f3(0xc1f,0xe90,0xcf4,0xed6)+'e\x20who\x20cont'+_0x1da9f3(0x4f5,0xa41,0x5c8,0x25f)+_0x1da9f3(0x554,0x8a3,0x5f3,0x8df)+'pt_',_0x30e36f[_0x3f34aa(0x77d,0x745,0xa54,0xd61)]='0';const _0x14ee05={};_0x14ee05[_0x1da9f3(0x7c9,0x8c3,0x6ce,0xa2e)]=_0x3f34aa(0x491,-0x307,0xab,0x313)+_0x3f34aa(0x5d8,0x76f,0x7e8,0x381),_0x14ee05[_0x3f34aa(0x5e0,0x719,0x3f5,0x17e)]=[_0x30e36f];let pip=alpha[_0x1da9f3(0xc58,0xeb3,0xfd4,0xbb2)+_0x1da9f3(0x5f6,0x611,0x1d7,0x642)+_0x3f34aa(-0x57,0x934,0x43e,0x855)](from,{'listMessage':{'title':_0x3f34aa(0xd10,0xd07,0x81d,0xc0e)+_0x3f34aa(0x68d,0x6ab,0x51c,0x665),'description':_0x1da9f3(0x93c,0x657,0x89d,0xc02)+sender[_0x3f34aa(0x77c,0x693,0x7c7,0x94a)]('@')[0xe99+0x1b95+-0x2a2e]+('\x20Semoga\x20ha'+_0x3f34aa(0x744,0x7fd,0x36c,0x1d8)+'nangkan\x20😇,'+'\x20ini\x20adala'+_0x1da9f3(0xae8,0xfbd,0x5a0,0xd73)+_0x3f34aa(0x6e7,0xbc2,0x8da,0x384)+'hkan\x20dipil'+_0x1da9f3(0x78e,0x55b,0x836,0x6d8)+_0x1da9f3(0xab6,0x75e,0xc85,0x92e)),'buttonText':'SELECT\x20HER'+'E','footerText':''+creator,'listType':_0x1da9f3(0x606,0x734,0x711,0x472)+'ECT','sections':[_0x473f67,_0x15eb5a,_0x3daea9,_0x31b35e,_0x414d48,_0x1a78e4,_0x2697a0,_0x1f3f2f,_0x517472,_0x538791,_0x1fdd26,_0x229732,_0xbb1d45,_0x248cd5,_0x22d10e,_0x5d34b8,_0x33d945,_0x2e38bc,_0x42c8fd,_0x3b7794,_0x5eba2a,_0x518128,_0x48ac6c,_0x532590,_0x14ee05]}},{'quoted':fgclink2,'contextInfo':{'mentionedJid':[sender]}});const _0x1d86a1={};_0x1d86a1[_0x3f34aa(0x334,0x6e0,0x3f6,0x883)]=!![],alpha[_0x3f34aa(0x725,0x62b,0x532,0xa17)+_0x1da9f3(0xa25,0x6da,0x578,0x7d7)](pip,_0x1d86a1);}const ini_bc_gc_bang=_0x16ae65=>{const _0x1b0b7e={'qThZr':function(_0x64fe8e,_0x3058f8){return _0x64fe8e(_0x3058f8);},'ptlcE':_0x288ebf(0x948,0x42b,0x597,0x5e6)+_0x25a702(-0x167,-0x238,-0x157,-0x2d7)+_0x288ebf(0xfae,0xf16,0xf6c,0x1183)+_0x288ebf(0xaf4,0x954,0xd7a,0x86b)+'gi','gsSTq':function(_0x354f73,_0x53e463){return _0x354f73===_0x53e463;},'kILMO':_0x288ebf(0xbf8,0xdc8,0x7c8,0xcb7),'wAGOV':_0x25a702(-0xe,0x17d,-0x551,-0x514),'GQhnc':_0x288ebf(0x7a1,0x2f2,0x3a5,0x572),'iEcrE':_0x25a702(0x2ae,-0x165,0x59b,-0x1cb),'Hbvrp':_0x25a702(0x841,0x882,0x8f8,0x8ec)+'a','fGpwE':_0x288ebf(0x572,0x4cf,0x37a,0x54f)+'21'};function _0x288ebf(_0x14fa9f,_0x557226,_0x352767,_0x4169dc){return _0x1da9f3(_0x14fa9f-0x3bc,_0x557226,_0x352767-0xfc,_0x4169dc-0x11f);}function _0x25a702(_0x134906,_0x226a37,_0x3228b0,_0x4ef1f1){return _0x3f34aa(_0x134906-0x91,_0x226a37,_0x134906- -0x1b1,_0x4ef1f1-0x2a);}var _0x11a731=alpha[_0x25a702(0x2e8,0x51c,0x42f,0x20f)]['all']()[_0x288ebf(0x972,0x5cc,0x689,0x4d4)](_0x1b221d=>_0x1b221d['jid'][_0x25a702(0x4c4,0x3d6,0x171,0x43f)](_0x25a702(0x5ac,0x84a,0x4ac,0xa49)));let _0xec8627=_0x16ae65;for(let _0x4d41a2 of _0x11a731){if(_0x1b0b7e['gsSTq'](_0x1b0b7e[_0x25a702(0x23a,-0xbb,0x2c3,0x5fd)],_0x288ebf(0xbf8,0x6b5,0x88e,0xfa6))){const _0xac8903={};_0xac8903[_0x25a702(0x15e,-0x351,-0x1c5,-0x15d)]=_0x1b0b7e['wAGOV'],_0xac8903[_0x288ebf(0xc5f,0x70b,0x935,0x936)]={},_0xac8903['type']=0x1,_0xac8903[_0x288ebf(0xc5f,0x70b,0x935,0x936)][_0x25a702(0x814,0xa28,0xa67,0x948)+'t']=_0x1b0b7e['GQhnc'];const _0x66ba48={};_0x66ba48[_0x25a702(0x15e,0x15,0x18f,-0x2a1)]=_0x1b0b7e[_0x288ebf(0x8b7,0xaae,0x885,0x403)],_0x66ba48['buttonText']={},_0x66ba48[_0x25a702(0x4e2,0x8cb,0x932,0x82d)]=0x1,_0x66ba48['buttonText'][_0x288ebf(0xf35,0x1465,0x10a0,0xec3)+'t']=_0x1b0b7e[_0x25a702(0x2,0x453,-0x393,0x2a1)];var _0x2e22d7=[_0xac8903,_0x66ba48];const _0x2221f8={};_0x2221f8[_0x25a702(0xfc,0x20d,-0x43d,-0x223)+'t']=_0x288ebf(0xa08,0x8a8,0x5c6,0x52b)+'*'+enter+enter+_0xec8627,_0x2221f8[_0x25a702(0x4e,-0x1fd,-0x409,0x10d)]=''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator,_0x2221f8[_0x288ebf(0x79e,0x754,0x498,0x5cc)]=_0x2e22d7,_0x2221f8[_0x25a702(-0x124,-0x145,0xd7,-0x348)]=0x1;var _0x4a7adf=_0x2221f8;const _0x376103={};_0x376103[_0x25a702(0x283,0x3fd,0x3ab,0x26e)]='hi',_0x376103[_0x288ebf(0x7a7,0x2c5,0x27e,0x7ff)+_0x288ebf(0xb23,0x7cd,0x7b5,0xe17)]=0x3b9aca00,_0x376103[_0x288ebf(0xabf,0xc30,0xecd,0xe96)+'d']=!![],_0x376103[_0x25a702(0x6f6,0xb3f,0x8e7,0x51e)+'ral']=!![];const _0x2d2939={};_0x2d2939[_0x288ebf(0x840,0x359,0x33c,0x763)]=_0x1b0b7e[_0x25a702(-0xe2,-0x83,-0x28e,-0x1ad)],_0x2d2939['contextInf'+'o']=_0x376103,_0x2d2939['quoted']=ftroli,_0x2d2939['sendEpheme'+_0x25a702(0x7f5,0x504,0x61a,0x64c)]=!![],alpha['sendMessag'+'e'](_0x4d41a2[_0x25a702(0x7f4,0x68b,0x67f,0x887)],_0x4a7adf,MessageType[_0x288ebf(0x9a1,0xad2,0xce0,0x98a)+_0x25a702(0x6c0,0x7aa,0x563,0x49b)],_0x2d2939);}else _0x1b0b7e['qThZr'](_0x636b6d,_0x1b0b7e['ptlcE']);}},ini_bc_pc_bang=_0x33e596=>{const _0x7ad3c1={};_0x7ad3c1[_0x5aa036(0xbc7,0x1116,0xd4e,0xad5)]=_0x2ab745(0x780,0x7ba,0xc23,0xbab);function _0x5aa036(_0x5d84f2,_0x32c7b2,_0xc90292,_0x32ab0a){return _0x3f34aa(_0x5d84f2-0x65,_0x32c7b2,_0x5d84f2-0x355,_0x32ab0a-0x115);}_0x7ad3c1['mBgAN']=_0x2ab745(0xbdd,0xdf5,0xd9f,0x754),_0x7ad3c1[_0x5aa036(0x8aa,0x663,0x86d,0x53f)]=_0x2ab745(0x551,0x5dd,0x61,0x694)+'21';function _0x2ab745(_0x55ea6d,_0x406b5b,_0x162cac,_0x2e92a6){return _0x1da9f3(_0x55ea6d-0x39b,_0x406b5b,_0x162cac-0x1,_0x2e92a6-0x15c);}const _0x26e190=_0x7ad3c1;var _0x3b4def=alpha['chats'][_0x2ab745(0xd3a,0xbc2,0xe64,0x102e)]();let _0x2adbc1=_0x33e596;for(let _0x19da1d of _0x3b4def){const _0x1785ff={};_0x1785ff['displayTex'+'t']=_0x26e190[_0x2ab745(0xdc1,0x95e,0xf90,0xceb)];const _0x1c1300={};_0x1c1300[_0x5aa036(0x664,0x2a8,0x462,0x77a)]=_0x2ab745(0x6f2,0x905,0xc39,0x453),_0x1c1300[_0x5aa036(0xa44,0x9ac,0xf12,0xe3e)]=_0x1785ff,_0x1c1300[_0x2ab745(0xbe2,0x10f0,0x1075,0x7dc)]=0x1;const _0x39bbde={};_0x39bbde[_0x5aa036(0xd1a,0x890,0xac7,0xf29)+'t']=_0x26e190['mBgAN'];const _0xcaaf68={};_0xcaaf68['buttonId']=_0x5aa036(0x7b4,0x8b2,0x4de,0xcd6),_0xcaaf68[_0x5aa036(0xa44,0xc8c,0xac1,0x905)]=_0x39bbde,_0xcaaf68['type']=0x1;var _0x4f3763=[_0x1c1300,_0xcaaf68];const _0xf8ca4={};_0xf8ca4[_0x2ab745(0x7fc,0x2b5,0x9b3,0x3b7)+'t']=_0x5aa036(0x7ed,0xc39,0xb8f,0x53c)+'*'+enter+enter+_0x2adbc1,_0xf8ca4[_0x5aa036(0x554,0x363,0x691,0x98a)]=''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator,_0xf8ca4[_0x5aa036(0x583,0x7b4,0x892,0x835)]=_0x4f3763,_0xf8ca4[_0x2ab745(0x5dc,0x641,0x55e,0x3eb)]=0x1;var _0x392ae5=_0xf8ca4;const _0x40624b={};_0x40624b[_0x5aa036(0x789,0x63a,0xcbe,0x819)]='hi',_0x40624b[_0x5aa036(0x58c,0x78f,0x4ac,0x237)+_0x5aa036(0x908,0x6a0,0xc93,0x432)]=0x3b9aca00,_0x40624b['isForwarde'+'d']=!![],_0x40624b['sendEpheme'+_0x2ab745(0xef5,0xd80,0xe05,0xc8a)]=!![];const _0x32b2e3={};_0x32b2e3[_0x5aa036(0x625,0x3e7,0xaa7,0x473)]=_0x26e190['DbDSr'],_0x32b2e3['contextInf'+'o']=_0x40624b,_0x32b2e3[_0x2ab745(0xd08,0xa3b,0xe03,0xe1f)]=ftroli,_0x32b2e3[_0x5aa036(0xbfc,0xef9,0xbba,0x74a)+_0x2ab745(0xef5,0xb89,0x12c3,0x13f4)]=!![],alpha[_0x5aa036(0x3be,0x421,0x872,0x474)+'e'](_0x19da1d[_0x5aa036(0xcfa,0x1104,0xa88,0xbbd)],_0x392ae5,MessageType[_0x5aa036(0x786,0x4fc,0xac1,0x8e2)+_0x5aa036(0xbc6,0xf21,0xd99,0xb59)],_0x32b2e3);}};switch(command){case'q':if(!mek[_0x1da9f3(0xc3d,0xd46,0x81d,0xfa3)][_0x1da9f3(0x7b3,0x611,0x962,0x336)]&&!isOwner&&!isCreator)return reply(lang['onlyOwner']());if(!m[_0x1da9f3(0x96d,0xda2,0xea5,0x6fa)])return reply(_0x3f34aa(0x76d,0x808,0x2ff,-0x23c)+_0x3f34aa(0x351,0x99f,0x6fe,0x35d));let qse=alpha[_0x3f34aa(0xdf6,0x82c,0x96f,0xd3c)](await m['getQuotedO'+'bj']());if(!qse[_0x3f34aa(0x261,0x9f6,0x7b9,0xa06)])return reply('the\x20messag'+_0x1da9f3(0x2d4,0x420,0x648,-0x25)+_0x1da9f3(0x3c8,0x8ba,0x8d4,0x76e)+_0x3f34aa(0xc4e,0x440,0x8e8,0x568)+_0x1da9f3(0x979,0x61f,0xb57,0xa77));await qse['quoted']['copyNForwa'+'rd'](m[_0x3f34aa(0xcf0,0xb41,0xa70,0x878)],!![]);break;case _0x3f34aa(0x90b,0x6ef,0x979,0xb66):case'delete':case'd':try{if(!isQuotedMsg)return reply(_0x1da9f3(0xc07,0x8ec,0x78d,0xa57)+'n\x20Bot!');const _0x51956a={};_0x51956a['id']=mek[_0x3f34aa(0xcf6,0x924,0x7d8,0xa16)][_0x1da9f3(0x922,0xa9f,0x692,0xb93)+_0x3f34aa(-0x1f2,-0x130,0x112,0x286)][_0x1da9f3(0x39f,0x64f,0x6f1,0x781)+'o'][_0x1da9f3(0x506,0x5e9,0xa46,0x24)],_0x51956a[_0x1da9f3(0x635,0x745,0x12e,0x84f)]=from,_0x51956a[_0x1da9f3(0x7b3,0xa68,0xbd1,0xc74)]=!![],pp=_0x51956a;if(!isGroupAdmins&&!mek[_0x1da9f3(0xc3d,0xa88,0x7c3,0xe65)][_0x3f34aa(0x51b,0x7ba,0x5ff,0x8b5)]&&!isOwner&&!isCreator)return reply(mess[_0x1da9f3(0xc12,0x109c,0xa55,0xaa8)]['admin']);alpha['deleteMess'+_0x3f34aa(0x65a,0x751,0x39b,0x11b)](from,pp);}catch(_0x5247b9){reply('reply\x20pesa'+_0x1da9f3(0x384,0x21,-0x18d,0x3b0)),reply(_0x5247b9),console[_0x3f34aa(0x461,0x738,0x51e,0x634)](_0x5247b9);}break;case _0x1da9f3(0x66b,0x2e5,0xb40,0x1f1):const _0xf9f692={};_0xf9f692[_0x1da9f3(0xb79,0x8dd,0x10c6,0x889)+'t']=_0x3f34aa(0x4ad,0x2f8,0x4e1,0x47c)+'t';const _0x14f01c={};_0x14f01c[_0x3f34aa(0x57,0x483,0x30f,0x483)]=_0x1da9f3(0x886,0x67f,0xdc9,0xc46),_0x14f01c[_0x3f34aa(0x30e,0x1e1,0x6ef,0x231)]=_0xf9f692,_0x14f01c[_0x1da9f3(0x847,0x8a6,0xc1b,0xcb9)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xc1e,0x63a,0xa86)](prefix),'©\x20'+ownername,[_0x14f01c]);try{if(!isGroup)return reply(_0x1da9f3(0xc42,0xc1c,0xd05,0x1009)+_0x3f34aa(0x238,0x519,0x226,0x5f5)+'ruop');if(!isQuotedMsg)return reply(_0x1da9f3(0xc07,0xfb5,0xc05,0x8b5)+_0x3f34aa(0xa5f,0x954,0x614,0x234));infom=await alpha[_0x1da9f3(0x307,0x3a5,0x5e0,0x5d1)+'o'](from,mek['message'][_0x1da9f3(0x922,0xad1,0x710,0x3ff)+_0x3f34aa(0x180,0x76,0x112,0x65)][_0x3f34aa(-0x178,-0xd2,0x1eb,0x405)+'o'][_0x1da9f3(0x506,0x80f,0x6aa,0x3bb)]),hemm=infom[_0x1da9f3(0x303,0x224,0x312,-0x109)],hemms=infom[_0x1da9f3(0x53d,0x147,0x5b7,0x7ba)],readon=hemms[_0x3f34aa(0x738,0xa28,0x657,0x136)](_0x5ecffb=>_0x5ecffb[_0x1da9f3(0xb59,0xd32,0xcc1,0xb7f)]),readdin=hemm[_0x1da9f3(0x80b,0xa4c,0x420,0x8fe)](_0x210b2a=>_0x210b2a['jid']),stamp=hemm['map'](_0x53e788=>_0x53e788['t']);function toTime(_0x3a0b38,_0x230de9){const _0x5159b3={'XucWS':_0x4c8a30(0x80c,0x547,0x7b5,0xa5b),'RIMdZ':function(_0x148033,_0x1720a0){return _0x148033>=_0x1720a0;},'DSiJO':function(_0x29e58b,_0x190945,_0x3c2024,_0x373425){return _0x29e58b(_0x190945,_0x3c2024,_0x373425);},'EIMAH':function(_0x157fc0,_0x8bae18){return _0x157fc0==_0x8bae18;},'zKAmL':_0x4deab6(-0x2ae,-0x3c0,-0x8a,0x318),'sqFKa':function(_0x2c1f74,_0x21085c){return _0x2c1f74===_0x21085c;},'RmzAQ':_0x4c8a30(0x78a,0x102,0x5aa,0x6a0),'hVPrQ':function(_0x4408fa,_0x306bcb){return _0x4408fa*_0x306bcb;}};function _0x4deab6(_0x463cd9,_0x20b1b1,_0x34d8cf,_0x2e533f){return _0x3f34aa(_0x463cd9-0x198,_0x20b1b1,_0x34d8cf- -0x1a3,_0x2e533f-0x82);}_0x230de9=_0x5159b3['EIMAH'](typeof _0x230de9,_0x5159b3[_0x4c8a30(0xd5a,0xf10,0xb0f,0x1017)])?![]:!![];let _0x1bab3d=new Date(_0x3a0b38);if(_0x230de9){if(_0x5159b3[_0x4c8a30(0xe7a,0xe3f,0x9d9,0xc49)](_0x5159b3['RmzAQ'],_0x4deab6(0x16e,-0x3a,0x4df,0x10e))){const _0x2c3809={'zlvSy':function(_0x309f8f,_0x1de890){return _0x309f8f(_0x1de890);},'cZGko':_0x5159b3[_0x4deab6(0x277,0x1fb,0x2e3,0x778)]};if(_0x5159b3[_0x4c8a30(0x19a,0x9db,0x6e5,0xadc)](_0x1dd5ec(_0x223771),-0x23051+-0x152e7*0x2+-0x5fcf*-0x11))return _0x257648(_0x2d281d,_0x2453b9,_0x4c8a30(0x12f8,0x1339,0xf4f,0xb7d)+_0x4deab6(0x4b6,0x8b6,0x7f2,0xbcf)+'*'+_0x4b9f41+_0xc90a3d+('🐣\x20*Title*\x20'+':\x20')+_0x592de4+_0x4bd477+('📊\x20*Quality'+'\x20:*\x20720kbp'+'s')+_0x2bf1cf+('🦁\x20*Ext*\x20:\x20'+'MP3')+_0x1b44c1+(_0x4c8a30(0xad3,0x7a6,0xa28,0x600)+_0x4deab6(0x236,0x95c,0x6fc,0x64c))+_0x24e698+_0x1d4492+(_0x4deab6(-0x24f,0x335,0xfc,-0x22d)+'\x20')+_0x32f6a9[_0x4c8a30(0xc1d,0xad0,0xa17,0x9df)]+_0xfe160b+_0x27889a+('_Untuk\x20dur'+_0x4c8a30(0xa45,0xc9f,0x9d5,0x759)+_0x4deab6(0x479,0x58e,0x1e0,0x540)+_0x4c8a30(0xa93,0x7bc,0x682,0x530)+_0x4c8a30(0x5f3,0x417,0x802,0x703)+_0x4deab6(-0x363,0x357,-0x145,-0x394)));_0x5159b3[_0x4deab6(-0x38e,-0x46f,0x72,-0x104)](_0x5963b4,_0x5d69a4,_0x3ecd04,'')['catch'](_0x1bf963=>{_0x2c3809['zlvSy'](_0x3300ca,_0x2c3809['cZGko']);});}else _0x1bab3d=new Date(_0x5159b3[_0x4deab6(0x439,0xec,-0x70,0x127)](_0x3a0b38,0x245d+-0x7*-0x533+0x7*-0x9d6));}var _0x347efe=_0x1bab3d[_0x4c8a30(0x83c,0x669,0x59f,0x2e5)](),_0xc78b1c=_0x1bab3d[_0x4c8a30(0xc52,0x952,0x836,0x3b1)](),_0x2b5a78=_0x1bab3d[_0x4deab6(0x2f5,-0x582,-0xf4,-0x31a)]();function _0x4c8a30(_0x5acaca,_0x1378fd,_0x23eaf3,_0x21ced5){return _0x3f34aa(_0x5acaca-0x123,_0x1378fd,_0x23eaf3-0x561,_0x21ced5-0x1eb);}var _0x10436e=_0x347efe+':'+_0xc78b1c+':'+_0x2b5a78;return _0x10436e;}function toBulan(_0x143edb,_0x2b1f98){const _0xadf1f5={'DDLVf':function(_0x4e2289,_0x2a4ed8){return _0x4e2289(_0x2a4ed8);},'OVLqG':function(_0x36ef50,_0x5925da){return _0x36ef50==_0x5925da;},'WMXMv':'undefined','IOloz':_0x915514(0x819,0xcd2,0xe76,0x921),'MJfYr':function(_0x3da1ea,_0x90c29c){return _0x3da1ea*_0x90c29c;},'JNjuD':_0x10bef5(0xc86,0xe76,0xaa2,0xeb2),'obGAZ':_0x10bef5(0xaf4,0xdbe,0xfa9,0xbf3),'RKQIc':_0x10bef5(0x803,0x951,0xd19,0xc58),'yiCNs':_0x915514(0xb96,0x62f,0xf1d,0xa21),'EBEMw':_0x10bef5(0xc14,0xfc1,0x1244,0xd7d),'jOpzV':_0x10bef5(0x7e4,0x23a,0x66a,0x5d1),'OKPsC':_0x10bef5(0x5f9,0x30,0x5dc,0x541),'eUuqL':_0x915514(0xc40,0xbc2,0x135f,0xe7b),'wOIxM':_0x10bef5(0xe16,0xea8,0x5e5,0xa1c),'Lvzno':_0x10bef5(0x59e,0xbf2,0x3a5,0x8fb),'mIzPu':_0x915514(0x1b4,0x9,0x106,0x557),'xnuyR':_0x10bef5(0x183,0x6ca,0x50c,0x5cf),'qbGmm':function(_0x57fa26,_0x36ed83){return _0x57fa26<_0x36ed83;},'etRvc':function(_0x2c386a,_0x33f30f){return _0x2c386a+_0x33f30f;}};_0x2b1f98=_0xadf1f5[_0x915514(0x88b,0x6bb,0xa09,0x6e9)](typeof _0x2b1f98,_0xadf1f5[_0x915514(0x836,0x330,0x74e,0x558)])?![]:!![];let _0xc9d735=new Date(_0x143edb);_0x2b1f98&&(_0xadf1f5['IOloz']!==_0x915514(0x538,0x7ec,0x969,0x921)?_0xadf1f5['DDLVf'](_0x320352,_0x5326f0['error'][_0x915514(0xe88,0x846,0xb40,0xa04)]):_0xc9d735=new Date(_0xadf1f5[_0x915514(0x10e5,0xa46,0x98d,0xea6)](_0x143edb,0x1085*-0x1+-0x2*0x8b+0x1583)));var _0x788240=[_0xadf1f5[_0x10bef5(0xb88,0xd59,0xf48,0xf40)],_0xadf1f5[_0x10bef5(0x322,0x65e,0x19f,0x62b)],_0xadf1f5['RKQIc'],_0xadf1f5['yiCNs'],_0x915514(0x13e9,0xad4,0x1489,0xf67),'Juni',_0xadf1f5[_0x10bef5(0x800,0x254,0x8a7,0x720)],_0xadf1f5[_0x10bef5(0xb67,0x5d3,0xa3c,0x822)],_0x915514(0x805,0xc66,0xc18,0xd46),_0xadf1f5[_0x10bef5(0x1053,0x9b8,0xea8,0xb8e)],_0x10bef5(0x8c0,0xd4c,0x686,0x84e),_0x10bef5(0x7ce,0x8ce,0x7a6,0xa9e)];function _0x10bef5(_0x573be7,_0x5b3c59,_0x4ca7ae,_0x4e74d5){return _0x1da9f3(_0x4e74d5-0x2e7,_0x5b3c59,_0x4ca7ae-0xc9,_0x4e74d5-0x1bf);}var _0x1b12e4=[_0xadf1f5[_0x915514(0x75d,0xc85,0xdad,0xca5)],_0x10bef5(0xae0,0x12fc,0xfb3,0xf0d),_0x10bef5(0x413,0x853,0x9f8,0x95a),_0xadf1f5[_0x915514(0x860,0x110f,0xdff,0xce8)],_0xadf1f5['Lvzno'],_0xadf1f5[_0x10bef5(0xb47,0x775,0x3a9,0x71c)],_0xadf1f5[_0x10bef5(0x714,0x255,0x9c0,0x622)]],_0x50fdd0=_0xc9d735[_0x915514(0x112b,0xd48,0x9b9,0xe2f)]();bulanee=_0xc9d735[_0x10bef5(0xb73,0x529,0xc56,0x919)]();function _0x915514(_0x748828,_0x182498,_0x58974a,_0x2d32b1){return _0x1da9f3(_0x2d32b1-0x385,_0x748828,_0x58974a-0x19,_0x2d32b1-0x36);}var _0x2397d0=_0xc9d735[_0x915514(0x128c,0xcd5,0xc6b,0xfd1)](),_0x2397d0=_0x1b12e4[_0x2397d0],_0xbcb5e9=_0xc9d735[_0x10bef5(0xd51,0x7ad,0xfe4,0xb25)](),_0x4a1aa0=_0xadf1f5[_0x915514(0xa37,0x911,0xc3c,0xb77)](_0xbcb5e9,0x14cb+0x1*-0x1969+-0x443*-0x2)?_0xadf1f5[_0x10bef5(0x88a,0xa3e,0xe57,0xa5e)](_0xbcb5e9,-0xa3*0x25+0x1f80+-0x85):_0xbcb5e9,_0x3673fd=_0x2397d0+',\x20'+_0x50fdd0+_0x915514(0x7d1,0xbf1,0xcdb,0xb8a)+_0x788240[bulanee]+_0x10bef5(0xc11,0xbd8,0xaa3,0xaec)+_0x4a1aa0;return _0x3673fd;}teksx=_0x1da9f3(0x81e,0x4e0,0x3e1,0xa73)+enter;for(let i of hemm){teksx+=_0x3f34aa(0x935,0xd63,0x9e0,0x6f0)+i[_0x1da9f3(0xb59,0xa9e,0xb1e,0xea7)]['split']('@')[-0x1*-0x166f+-0x441*0x2+-0x5*0x2c9]+enter+_0x1da9f3(0x7c4,0x5ba,0x728,0xb25)+toTime(i['t'],!![])+enter+(_0x3f34aa(0x1f3,0x16c,0x33d,0x3a0)+':\x20')+toBulan(i['t'],!![])+enter+'•'+enter;}teksx1=_0x1da9f3(0x4d9,0x19c,0x55f,0x7da)+'D*'+enter+enter;for(let i of hemms){teksx1+=_0x3f34aa(0x644,0x5bc,0x9e0,0xb3f)+i[_0x3f34aa(0xc23,0x771,0x9a5,0xbe1)]['split']('@')[-0x13b4+0x14d+0x1267]+enter,teksx1+='📍\x20Waktu\x20:\x20'+toTime(i['t'],!![])+enter+('📍\x20Tanggal\x20'+':\x20')+toBulan(i['t'],!![])+enter+'•'+enter;}const _0x3a0cc4={};_0x3a0cc4[_0x3f34aa(0x42b,0x34,0x237,0x9c)+_0x3f34aa(0xa18,0x52e,0x5b3,0xaa)]=0x3e7,_0x3a0cc4[_0x1da9f3(0x703,0x6df,0x775,0xb3d)+'d']=!![],_0x3a0cc4[_0x1da9f3(0x32f,-0x1d6,0x2df,0x259)+'id']=readdin,alpha['sendMessag'+'e'](from,teksx,text,{'sendEphemeral':!![],'quoted':mek,'thumbnail':fs[_0x3f34aa(0x1e,-0x90,0x111,0x217)+'nc'](_0x3f34aa(0x4b4,-0x23,0x362,0x32c)+thumbnail),'contextInfo':_0x3a0cc4});const _0x30433d={};_0x30433d[_0x1da9f3(0x3eb,0x4b3,0x2e7,0x743)+_0x1da9f3(0x767,0x762,0x422,0x7b4)]=0x3e7,_0x30433d[_0x1da9f3(0x703,0xaf9,0x7fa,0x445)+'d']=!![],_0x30433d[_0x3f34aa(0x689,0x69b,0x17b,-0x93)+'id']=readon,alpha['sendMessag'+'e'](from,teksx1,text,{'sendEphemeral':!![],'quoted':mek,'thumbnail':fs[_0x1da9f3(0x2c5,0x657,-0x54,0x1ab)+'nc'](_0x3f34aa(0x14a,0x622,0x362,0x4bd)+thumbnail),'contextInfo':_0x30433d});}catch(_0x500269){reply('reply\x20pesa'+_0x3f34aa(0x65d,-0x2c7,0x1d0,0x574)),reply(_0x500269),console[_0x1da9f3(0x6d2,0x382,0x76e,0x90f)](_0x500269);}await limitAdd(sender,limit);break;case _0x1da9f3(0x4da,0xa34,-0x5e,0xdf):const _0x3cf666={};_0x3cf666[_0x3f34aa(0xb28,0xbc6,0x9c5,0x4fd)+'t']='Check\x20Limi'+'t';const _0x4009f5={};_0x4009f5[_0x3f34aa(0x37b,0x777,0x30f,-0xea)]=_0x3f34aa(0x6dc,0x5b7,0x6d2,0xac9),_0x4009f5['buttonText']=_0x3cf666,_0x4009f5[_0x3f34aa(0x35e,0xa7d,0x693,0x675)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x977,0xb3a,0x6d2,0x30c)](prefix),'©\x20'+ownername,[_0x4009f5]);const kta=['Lebih\x20baik'+'\x20mengerti\x20'+_0x1da9f3(0x87d,0x990,0xba7,0xbf2)+_0x1da9f3(0x91a,0x571,0xb72,0x85b)+_0x3f34aa(0x97a,0xa61,0x952,0x48b)+'i.',_0x3f34aa(0x864,0x86a,0x8bc,0xa79)+_0x3f34aa(0x5be,0x7f2,0x3ae,0x6c9)+_0x3f34aa(-0x2a7,-0x18b,0x227,0x577)+'\x20bertahan\x20'+'menghadapi'+_0x1da9f3(0x538,0xa83,0x8,0x18f)+'.\x20Namun,\x20j'+_0x3f34aa(0x62a,0x83c,0x86e,0xafc)+_0x3f34aa(0x5d9,-0x284,0x142,-0xe0)+_0x3f34aa(0x70d,0x61,0x27c,0x21b)+_0x3f34aa(0x4da,0x41f,0x479,0x158)+_0x3f34aa(0x8eb,0xa72,0x932,0xc06)+'dia\x20kekuas'+_0x3f34aa(0x11a,0x56c,0x619,0x283),_0x1da9f3(0x274,0x662,0x1d5,-0x1ad)+_0x1da9f3(0x8f0,0x9f7,0x4a5,0x846)+_0x1da9f3(0x345,0x5fd,0x625,0x324)+'teguh,\x20Tuh'+_0x3f34aa(0x4be,0x4ff,0x75f,0xc69)+_0x3f34aa(-0xf0,0x242,0x468,0x1f1)+'lam\x20usahan'+_0x1da9f3(0xc47,0xe64,0xbf9,0x878),'Penderitaa'+_0x3f34aa(0x6b4,0x5ac,0x531,0x441)+'elajaran.',_0x3f34aa(0x56a,0x62e,0x775,0x605)+_0x1da9f3(0x80c,0x56a,0x6df,0x839)+'pa\x20agama\x20a'+'dalah\x20pinc'+_0x3f34aa(0x18f,0x7f0,0x56e,0x909),_0x1da9f3(0x7e3,0xd08,0x492,0x57f)+_0x3f34aa(0x1c0,0x539,0x261,0x54f)+_0x1da9f3(0x62f,0x9d9,0x2f6,0xb1d)+_0x3f34aa(0x4b0,0x60b,0x5c4,0x723)+_0x1da9f3(0xbcc,0x86d,0x68c,0x1037)+_0x1da9f3(0xc1c,0xc20,0xf0e,0xcd6)+'rus\x20tetap\x20'+_0x1da9f3(0x80f,0x4f2,0x72b,0x6ea),'Perbedaan\x20'+_0x3f34aa(-0xf5,0x2d8,0xa3,0x1f3)+_0x3f34aa(0xa12,0x596,0x6ab,0x8b1)+_0x3f34aa(0x291,0x609,0x661,0x62e)+'depan\x20tak\x20'+'lebih\x20dari'+'\x20ilusi\x20yan'+'g\x20keras\x20ke'+_0x1da9f3(0xb84,0xb55,0x107b,0x9f2),_0x3f34aa(0xd77,0xcef,0xa25,0xec1)+_0x1da9f3(0x7d0,0x85b,0x604,0x47e)+_0x1da9f3(0x552,0x637,0x7a7,0xc6)+_0x1da9f3(0x659,0x5c9,0x8b9,0x3b7)+'h,\x20dan\x20seb'+_0x3f34aa(0x431,0x596,0x68d,0x5ab)+_0x1da9f3(0x873,0x956,0x7a9,0x875)+_0x1da9f3(0xb46,0xd3a,0x1078,0xea4)+_0x3f34aa(0x658,0x1b0,0x53f,0xa6e)+_0x3f34aa(0x475,0x364,0x73c,0x318)+_0x1da9f3(0x6ca,0xa00,0x5b5,0x6d9)+_0x1da9f3(0x807,0xd28,0x3cb,0x594)+'?',_0x3f34aa(0x5ca,0x962,0x901,0x5a9)+_0x1da9f3(0x31d,0x653,0x80,0x5e9)+_0x1da9f3(0x586,0x2c7,0x160,0xc6)+_0x3f34aa(-0x17b,0x12d,0x29a,0x757)+_0x1da9f3(0x60b,0x805,0x9dd,0x8f8)+_0x1da9f3(0x655,0x101,0x833,0xa9e)+_0x1da9f3(0x925,0x76e,0x690,0x5cb)+_0x3f34aa(0xb4,0x2fb,0x487,0x5bb),_0x1da9f3(0x4be,0x42,0x576,0x456)+'g\x20baik\x20unt'+_0x1da9f3(0xb35,0x9bd,0xed9,0xa2e)+_0x3f34aa(-0x119,0x45b,0x185,0x410)+_0x1da9f3(0x71f,0x421,0x551,0x2e5)+_0x3f34aa(0x3d4,0x139,0x5f2,0x6b9)+_0x3f34aa(0xa57,0x570,0xa28,0x79c)+_0x3f34aa(0x49f,0x3b3,0x707,0x939)+'ndiri.',_0x3f34aa(0x69d,0x94c,0x647,0xa1f)+_0x1da9f3(0x334,0x74a,0x11f,0x74c)+_0x3f34aa(0x40a,0x570,0x8af,0xc46)+_0x1da9f3(0x9d0,0x878,0xaad,0x51e)+_0x3f34aa(0x961,0x52a,0x9f0,0xbf2)+'lah\x20stimul'+'an\x20terkuat'+_0x3f34aa(-0x2a0,-0x23a,0x159,0x567)+_0x1da9f3(0x990,0x5c9,0x88c,0x8dc)+'rkreasi\x20da'+_0x1da9f3(0x6ee,0xbc9,0x2c3,0x300)+_0x3f34aa(0x4d7,0x600,0x99c,0x6a9)+_0x1da9f3(0x690,0xa5d,0xa8c,0x87c)+_0x3f34aa(0x611,0x1fe,0x53e,0x487),'Manusia\x20ak'+_0x3f34aa(0xafe,0xc64,0x910,0x75f)+_0x1da9f3(0x79c,0x4c3,0x41f,0xc17)+_0x3f34aa(0xa92,0x957,0x6a1,0x1ed)+_0x3f34aa(0x432,0x457,0x311,0xe)+'ia.',_0x3f34aa(0x91b,0xc43,0x8ca,0x3c8)+_0x3f34aa(0x828,0x15a,0x667,0x9f1)+_0x1da9f3(0x9a9,0x72e,0xa0f,0xd95)+_0x3f34aa(0x6ae,0x790,0x5fe,0x2cd)+_0x3f34aa(0x314,0x50f,0x3de,0x3fe)+'orang.\x20Say'+'a\x20hanya\x20in'+_0x3f34aa(0x31c,-0x332,0x32,0x145)+_0x1da9f3(0x8d6,0x777,0xcd3,0x3fa)+_0x1da9f3(0x593,0x534,0x777,0x718)+'orang.','Apabila\x20se'+_0x3f34aa(0x321,-0x20,0x195,0x2b7)+_0x3f34aa(0xe3,0x2cc,0x329,0x3ee)+_0x3f34aa(0x27a,-0xc3,0x46,0x114)+_0x3f34aa(0xb22,0x165,0x5e2,0x73f)+_0x3f34aa(0x47c,0x4ba,0x1c4,0x6a),_0x1da9f3(0x8e6,0x537,0xe26,0xdde)+_0x1da9f3(0xace,0xe13,0x693,0x645)+_0x1da9f3(0x232,0x6d2,-0x323,-0x18a)+'njadi\x20tuan'+_0x3f34aa(0x59f,-0x2fa,0x91,0x10e)+_0x1da9f3(0x5d1,0x277,0x680,0x686)+'adi\x20kusir\x20'+_0x1da9f3(0xbf9,0xd2d,0xcfc,0x8ed)+_0x1da9f3(0xaca,0x70c,0xed0,0xdf6)+'enjadi\x20kap'+_0x1da9f3(0x6bf,0x7af,0x3f4,0x1d4)+_0x1da9f3(0xa65,0x735,0x52d,0xe6b)+'dupnya.',_0x1da9f3(0x718,0x1ed,0x296,0x902)+_0x1da9f3(0x27a,0x494,0x198,0xe0)+'ebih\x20besar'+_0x3f34aa(0x621,0x480,0x738,0xa37)+_0x3f34aa(0xafd,0xfd0,0xaa1,0x777)+_0x3f34aa(0x12f,-0x536,0xb,0x25)+_0x1da9f3(0x4f3,0x7ec,0x22c,0x461)+'risi\x20dari\x20'+'nenek\x20moya'+_0x1da9f3(0x710,0x394,0xb84,0xc28),_0x3f34aa(0x7c5,0x22c,0x490,-0xb7)+'g\x20melelahk'+_0x1da9f3(0x27e,0x4f9,0x508,0x20d)+_0x1da9f3(0xb77,0xe9a,0xd06,0xf09)+'h\x20menjadi\x20'+_0x1da9f3(0x350,0xb2,0x75,-0xbe)+'\x20tidak\x20tul'+'us.','Terbuka\x20un'+'tuk\x20Anda,\x20'+_0x3f34aa(0x678,0xdd0,0x9eb,0x4af)+_0x3f34aa(0x2dd,0x793,0x437,0x325)+'eri\x20kita\x20j'+_0x1da9f3(0x58e,0x95a,0x844,0x9fb)+_0x1da9f3(0xbad,0x79a,0x96a,0x8b4)+_0x1da9f3(0x318,0x4c1,0x4bf,0x441)+'rnah\x20berfi'+_0x3f34aa(0x69a,0x735,0x807,0x6f5)+_0x1da9f3(0x3c0,0x54f,-0x186,0x26b)+_0x3f34aa(0x6ad,0x741,0x709,0xaf3),_0x3f34aa(0xa57,0x61c,0x57a,0x22c)+'adalah\x20kub'+_0x3f34aa(0x452,0x550,0x5a5,0x671)+_0x3f34aa(0x32a,0x66,0x3a3,0x418)+_0x1da9f3(0x78b,0xcd1,0xa3f,0x38f)+'.','Cinta\x20buka'+'n\x20saling\x20m'+_0x3f34aa(0x643,0x6d2,0xa33,0xb94)+_0x3f34aa(0x98d,0x571,0x5b4,0x1a6)+_0x1da9f3(0x550,0x8dd,0xc9,0xa91)+'arah\x20yang\x20'+_0x1da9f3(0x3d9,0x647,0x7dd,0x1aa)+_0x3f34aa(-0x86,0x1b8,0x307,0x1a8),'Kita\x20adala'+_0x1da9f3(0xbdf,0x104f,0xab3,0xaeb)+_0x3f34aa(-0x1e4,0x668,0x2bb,0x57c)+_0x3f34aa(0xb39,0xdc0,0xaa3,0x647)+'ang\x20kali.\x20'+_0x1da9f3(0x812,0xabd,0x7fc,0x7bc)+'ikian,\x20kec'+'emerlangan'+'\x20bukan\x20tin'+_0x1da9f3(0x904,0xb7c,0x5b7,0xe14)+'api\x20kebias'+_0x3f34aa(0x51a,0x69e,0x619,0xb33),_0x1da9f3(0x43c,0x46a,0x6fa,0x6ca)+_0x3f34aa(0xd8e,0xa40,0x838,0x33a)+_0x3f34aa(0x9c,0x218,0x316,0x6e)+_0x3f34aa(0x40e,0xd94,0x966,0x851)+_0x3f34aa(0xd20,0xb8f,0xa3d,0xcfe)+_0x1da9f3(0x8d8,0x803,0x3b7,0x952)+_0x3f34aa(0x8a6,0xc40,0x753,0xac1)+'\x20Anda.\x20Dir'+'i\x20Anda\x20han'+'ya\x20cukup\x20s'+_0x3f34aa(0xb67,0x9a1,0x94c,0x820),_0x1da9f3(0x518,0x277,0x9f8,0x56a)+_0x1da9f3(0x6ab,0x5d9,0x9c8,0x99b)+_0x3f34aa(0x8b2,0x3d4,0x7c2,0xc8f)+_0x1da9f3(0x5fc,0x80b,0x1c3,0xa59)+_0x1da9f3(0x3e3,0x127,-0x122,-0x145)+'da\x20akan\x20me'+_0x1da9f3(0x393,0x114,0x5b0,0x47a)+_0x1da9f3(0xb8c,0x10b8,0xd23,0xccb)+_0x3f34aa(0x49e,0xb4a,0x8b0,0xb08)+_0x3f34aa(0x58f,0x225,0x50,0x16e)+'.',_0x3f34aa(0xc13,0x63a,0x78b,0x30a)+'an\x20datang\x20'+'cepat\x20atau'+'\x20lambat.\x20J'+'ika\x20masala'+_0x3f34aa(0x8a3,0x26,0x538,0x381)+'sambut\x20den'+_0x1da9f3(0xbfb,0xf6d,0x885,0xbd0)+_0x3f34aa(0x14f,0x6a4,0x26a,0x247)+_0x3f34aa(0x1e2,0x566,0x577,0x852)+_0x1da9f3(0xa2f,0x4e1,0xe15,0xbbc)+_0x3f34aa(0x3de,0x3bd,0x131,0x2c1)+'\x20semakin\x20c'+_0x3f34aa(0x2e2,0x40e,0x108,-0x266)+'rgi.',_0x1da9f3(0x744,0x6dc,0xa19,0x41c)+_0x3f34aa(0x22,0x28b,0x554,0x611)+_0x1da9f3(0x638,0x143,0x588,0x75a)+_0x1da9f3(0x30d,0x70c,0x678,0x325)+_0x1da9f3(0x914,0x5c1,0x81c,0x9da)+'\x20lalu.\x20Tap'+_0x3f34aa(0xb05,0xc05,0x908,0xa72)+'ang\x20kita\x20l'+'akukan\x20bis'+_0x1da9f3(0x555,0x1ea,0x7d4,0x85b)+_0x1da9f3(0xacb,0xc5b,0xa68,0xfcf)+'n.','Kesabaran\x20'+_0x3f34aa(0x67a,0x39b,0x53d,0x1f9)+_0x3f34aa(0x551,0x846,0x59c,0x2d3)+_0x1da9f3(0x1f7,-0x175,0x9a,-0x31e)+'n.',_0x1da9f3(0x346,0x278,0x3f9,0x3a7)+_0x3f34aa(0x9f1,0x3fa,0x761,0x801)+_0x3f34aa(-0xba,0x15a,0x5d,-0x453)+'i\x20oleh\x20kei'+'nginan\x20unt'+'uk\x20maju,\x20b'+_0x3f34aa(0x7d7,0xb02,0x63b,0xa2a)+'keinginan\x20'+_0x1da9f3(0xa0f,0x9a9,0x4b8,0x824)+_0x1da9f3(0xad5,0x975,0xdd0,0xd1b)+_0x1da9f3(0x29d,0x483,0x41a,0x1d8),_0x1da9f3(0x612,0x29c,0x7c1,0x59a)+'engkau\x20ber'+'ada\x20selalu'+_0x3f34aa(0x682,0xf1a,0xa80,0xcdf)+'i\x20yang\x20ter'+_0x3f34aa(-0x27e,-0x17a,0x82,0x3a)+_0x3f34aa(0x7a2,0x609,0xa0e,0x9e7)+_0x3f34aa(0xbf9,0x79b,0x990,0xe23)+_0x1da9f3(0xb05,0x5b5,0xb24,0xce4)+'bisa\x20kita\x20'+'berikan.','Kebencian\x20'+_0x1da9f3(0x8a4,0x6d6,0x428,0xcba)+_0x3f34aa(-0x2c7,0x4e6,0xac,0x1c9)+_0x1da9f3(0x275,0x235,0x3b9,-0xcd)+_0x1da9f3(0xad0,0xd72,0x9f3,0xcca)+_0x1da9f3(0x6c8,0x6dd,0x759,0xb0b)+'l.','Anda\x20tidak'+_0x3f34aa(0x3fb,-0x49e,0x97,-0x342)+_0x1da9f3(0x295,0x121,0x122,0x3a7)+'l\x20pada\x20kal'+_0x3f34aa(0x47b,0x6a4,0x98e,0x725),_0x3f34aa(0x70f,0x8dd,0x999,0xdc7)+_0x3f34aa(0x913,0xa81,0x5c5,0x7ff)+_0x1da9f3(0xa7b,0x790,0x522,0xfbd)+_0x1da9f3(0x464,0x41b,0x105,0x12c)+_0x3f34aa(0xa89,0xa69,0x60a,0x83e)+_0x3f34aa(0xa48,0xbac,0x8a4,0x765)+'ripada\x20ber'+_0x1da9f3(0xc06,0x6e8,0x907,0xd32)+'n\x20bermimpi'+_0x3f34aa(0x77a,0x5f7,0x2f5,0x732)+'ung-renung'+'.','Hal\x20terbai'+'k\x20yang\x20bis'+_0x3f34aa(0xbb1,0x915,0x8b5,0x595)+_0x1da9f3(0x29b,-0xcd,0x4ae,-0xd4)+_0x1da9f3(0x667,0x7be,0x57a,0x8bf)+_0x1da9f3(0x7d5,0x3de,0x675,0x359)+_0x1da9f3(0x5bb,0x637,0x389,0x30b)+_0x3f34aa(0x72f,0x2b2,0x567,0x58c)+_0x3f34aa(0xa8,-0x376,0x1b1,0xc1)+_0x3f34aa(0x100,0x124,0x28c,0x5d8)+_0x1da9f3(0x39a,0x8b9,0x655,0x804)+_0x3f34aa(-0x4f,-0x3e1,0xd4,0x532)+_0x3f34aa(0x427,-0x2bb,0x0,-0x2c4)+_0x1da9f3(0x626,0x16f,0x1b9,0x272)+'i.',_0x1da9f3(0x44c,0x611,0x6a3,0x873)+_0x3f34aa(0x19b,0x66b,0x4ce,0x4b9)+_0x3f34aa(0x5c3,0x3bd,0x107,-0x126)+',\x20tetapi\x20t'+_0x3f34aa(0x281,0x8df,0x435,0x3d9)+_0x1da9f3(0x48a,0x3a7,0x3d2,0x3c9)+_0x3f34aa(0x304,-0x165,0x10b,-0x9d)+'egagalan.','Aku\x20tidak\x20'+_0x3f34aa(0xd74,0x4a5,0x912,0xcb9)+_0x3f34aa(-0xec,-0x32b,0x21,-0x2d8)+_0x3f34aa(0x63b,0x51a,0x645,0x2ee)+_0x3f34aa(0x1ce,0x2cf,0x393,-0x123)+_0x1da9f3(0x848,0xa21,0x7cd,0x6da)+_0x1da9f3(0x26e,0x201,0xba,-0x24f)+_0x1da9f3(0x44f,0x470,0x840,0x193)+_0x1da9f3(0x40a,0x28e,-0x116,0x9e)+'mua\x20orang\x20'+_0x3f34aa(0x1a9,-0x1d9,0x161,0x4a7)],su=kta[Math[_0x3f34aa(0x7f1,0x78,0x3cc,0x323)](Math['random']()*kta[_0x3f34aa(0x747,0x93e,0x92e,0xb52)])],_0x397e3a={};_0x397e3a[_0x1da9f3(0xb79,0xb0f,0xaf0,0xa6e)+'t']='☠️\x20Owner\x20';const _0x55c9c5={};_0x55c9c5[_0x1da9f3(0x4c3,0x1a9,0x53,0x62a)]=_0x3f34aa(-0x3e,-0x44,0x12d,-0x2c9),_0x55c9c5['buttonText']=_0x397e3a,_0x55c9c5[_0x1da9f3(0x847,0x2f4,0xccb,0x74e)]=0x1;const _0x37f3ba={};_0x37f3ba['displayTex'+'t']=_0x3f34aa(0x402,0x631,0x781,0x37b);const _0x59cd8f={};_0x59cd8f[_0x1da9f3(0x4c3,0x40,0x8eb,0x826)]=''+command,_0x59cd8f['buttonText']=_0x37f3ba,_0x59cd8f[_0x1da9f3(0x847,0x5b8,0xd29,0x2fa)]=0x1;var buttonns=[_0x55c9c5,_0x59cd8f];buttonMessage={'contentText':su,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+(_0x3f34aa(0xd06,0x462,0x848,0xae9)+_0x1da9f3(0x599,0x427,0x6b0,0x2fe)+_0x1da9f3(0x92f,0xd57,0xc0c,0xa58))+enter+_0x1da9f3(0xacc,0xff0,0xb9f,0x96a)+sender[_0x3f34aa(0x5ef,0x5b5,0x7c7,0x3fd)]('@')[-0x1345+-0x1*-0x2333+-0xfee],'buttons':buttonns,'headerType':0x1};const _0x3f49c3={};_0x3f49c3[_0x1da9f3(0x5e8,0x45a,0xa17,0xad3)]='hi',_0x3f49c3[_0x3f34aa(0x205,0x1fa,0x237,0x401)+_0x3f34aa(0xace,0xad1,0x5b3,0x2a9)]=0x3b9aca00,_0x3f49c3[_0x1da9f3(0x703,0x68f,0x26d,0x387)+'d']=!![],_0x3f49c3[_0x3f34aa(0xa82,0x781,0x8a7,0xa34)+_0x3f34aa(0x4ee,0x949,0x9a6,0x8b2)]=!![],_0x3f49c3['mentionedJ'+'id']=[sender];const _0x4dc05b={};_0x4dc05b[_0x1da9f3(0x484,0x7a2,0x9a2,0x6f4)]=_0x3f34aa(0x25f,0x1bf,0x2,0x19d)+'21',_0x4dc05b['contextInf'+'o']=_0x3f49c3,_0x4dc05b[_0x1da9f3(0x96d,0xc13,0xc83,0xd0e)]=ftoko,_0x4dc05b['sendEpheme'+_0x1da9f3(0xb5a,0x67f,0xf5a,0xcda)]=!![],alpha[_0x3f34aa(0x6b,0x11,0x69,-0x1d4)+'e'](from,buttonMessage,MessageType[_0x1da9f3(0x5e5,0x5bc,0x41b,0x9d7)+_0x3f34aa(0xd5d,0x378,0x871,0xaed)],_0x4dc05b),await limitAdd(sender,limit);break;case _0x1da9f3(0xa18,0xe5b,0x6a6,0xbcd):const _0x7abc25={};_0x7abc25[_0x1da9f3(0xb79,0xbbb,0xed9,0xefb)+'t']='Check\x20Limi'+'t';const _0x514665={};_0x514665[_0x1da9f3(0x4c3,0x18f,0x69a,0x4a7)]=_0x3f34aa(0xa7e,0x2cd,0x6d2,0xc19),_0x514665[_0x3f34aa(0x56d,0x283,0x6ef,0x624)]=_0x7abc25,_0x514665['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x514665]);const dare=['Kirim\x20pesa'+_0x1da9f3(0x3f3,0x8fe,0x23f,0x5b7)+_0x1da9f3(0x540,0x3a9,0x54c,0x150)+_0x1da9f3(0x9f7,0xc28,0xd2e,0x9f6)+_0x3f34aa(0x8a8,0xcdf,0x9e8,0xbcf)+_0x1da9f3(0xc33,0x700,0x9a7,0xa46)+_0x1da9f3(0x4e9,0x9d6,0x82a,0x78e),_0x1da9f3(0xa4f,0xb15,0xe38,0x822)+_0x3f34aa(0x93d,0x40d,0x6cd,0x415)+_0x3f34aa(-0x4fd,0x2ef,0x9,-0x445)+_0x1da9f3(0x504,0x6e2,0x8da,0x113)+_0x1da9f3(0x581,0x172,0x6c1,0x4cb),_0x3f34aa(0x312,0x473,0x666,0x985)+_0x3f34aa(0x8fb,0xb80,0x90a,0x411)+_0x1da9f3(0x2fe,0x76d,-0x7f,0x3a8),'Bilang\x20\x22KA'+_0x3f34aa(0x7fc,0x12a,0x4e9,0x240)+_0x1da9f3(0x611,0x1a2,0x2fc,0x1ee)+_0x3f34aa(0x4e3,0x814,0x6d9,0x2f6)+'\x20ke\x20cowo',_0x1da9f3(0xa44,0x67f,0x862,0xe17)+_0x1da9f3(0x482,0xed,0x1cc,0x6ea)+_0x3f34aa(-0x278,-0x1e2,0x2a,-0x373),_0x3f34aa(0x2f1,0x834,0x64f,0x522)+_0x3f34aa(-0x46,-0xf8,0x1da,0x134)+_0x3f34aa(0x997,0xae7,0x86b,0x9aa)+'c/pc\x20selam'+_0x1da9f3(0x238,-0x43,0x5e5,-0xab),'kirim\x20voic'+_0x3f34aa(0x4ef,0xb3e,0x833,0x667)+'ang\x20can\x20i\x20'+'call\x20u\x20bab'+'y?',_0x3f34aa(0x357,0x6ef,0x827,0x79c)+'an\x20lagu/qu'+_0x1da9f3(0x4d2,0x3a5,0x701,0x25b)+'\x20tag\x20membe'+_0x1da9f3(0x82f,0x43b,0x45d,0x735)+_0x1da9f3(0x5da,0x824,0x202,0x992)+_0x3f34aa(0x6e0,0x323,0x7d9,0x78f),_0x3f34aa(-0xed,0x279,0xcd,0x604)+_0x3f34aa(0xa34,0x3d0,0x52e,0x5ef)+'\x203\x20hari',_0x3f34aa(0x47a,0x281,0x6cb,0x1fb)+'\x20bahasa\x20da'+_0x1da9f3(0x830,0x4b9,0x9f9,0xd46)+'m','ganti\x20nama'+_0x3f34aa(-0x29d,-0x1d5,0x239,-0x58)+_0x1da9f3(0x391,0x879,0x1a9,0x695)+_0x3f34aa(0x388,-0x2ad,0xb3,0x81)+_0x1da9f3(0x248,0x110,0xeb,0x6fb)+'5\x20jam',_0x1da9f3(0x56c,0x348,0x71d,0x1a2)+'ntak\x20wa\x20ur'+'utan\x20sesua'+_0x3f34aa(0xdf8,0x49b,0x8db,0xafd)+_0x1da9f3(0xb02,0xe94,0xdec,0xcc0)+_0x3f34aa(0xd69,0x592,0xa46,0x787)+_0x3f34aa(0x851,-0x8f,0x4bc,0x2d9)+_0x3f34aa(0x442,0x57a,0x346,0x73b)+_0x1da9f3(0x645,0x27c,0xac0,0x3cc),_0x1da9f3(0xb7e,0xc0f,0x796,0xcdb)+_0x1da9f3(0x2f0,0x161,-0xce,-0x91)+_0x3f34aa(0x9ec,0xc6b,0x973,0xc19)+_0x1da9f3(0x60e,0x5a8,0x64b,0x867)+_0x3f34aa(0x4f7,0x543,0x654,0x5f3)+'an',_0x3f34aa(0x6c3,-0x202,0x294,0x513)+_0x3f34aa(0x616,0x3a,0x493,0x83d)+_0x3f34aa(0xb64,0xcd9,0xa08,0x595)+_0x3f34aa(0xe9,-0x355,0x18,-0x188),'bilang\x20\x22i\x20'+_0x3f34aa(0x2d8,-0x3c,0x186,-0xd)+'n\x20you,\x20mau'+_0x3f34aa(0x5aa,-0x284,0x147,0x3a8)+_0x3f34aa(0x557,0x556,0x50e,0x36c)+_0x1da9f3(0x434,0x571,0x873,0x83)+_0x1da9f3(0xae5,0xac3,0x6f3,0xfa3)+_0x1da9f3(0x9ce,0xe47,0xe53,0x55f)+_0x1da9f3(0x5ae,0x20a,0x945,0x660)+_0x1da9f3(0x87b,0x5a6,0x8dc,0x5da)+'di\x20wa/tele'+'),\x20tunggu\x20'+_0x1da9f3(0x8ba,0xd30,0x7ff,0xafd)+'\x20kalo\x20udah'+_0x3f34aa(0xe7c,0xd63,0x998,0xa1e)+_0x3f34aa(0x742,0x4b,0x56d,0x955),_0x1da9f3(0x949,0x5a0,0x82e,0x7ff)+_0x3f34aa(-0x13f,0x55f,0x70,0x120)+_0x3f34aa(0x927,0x2fc,0x508,0x5cb),_0x1da9f3(0xb5c,0xfad,0xfa6,0xce2)+_0x3f34aa(-0x26a,0x612,0xc2,0x1a1)+_0x3f34aa(0x20e,-0x12c,0x1b,-0x4e5),_0x1da9f3(0x2af,0x479,0x613,0x204)+_0x3f34aa(-0xb4,0x29,0x3bf,0x60d)+_0x1da9f3(0x490,0x688,0x2f,0x940)+_0x3f34aa(0x803,0x321,0x414,0x5d1),'pap\x20mukamu'+_0x1da9f3(0x20f,-0x1a6,-0x59,-0x318)+_0x3f34aa(0x42d,0x5d,0x322,0x3b1)+_0x1da9f3(0xc13,0xf6d,0xd61,0xde8)+_0x1da9f3(0x994,0xdef,0xbf0,0x836),_0x1da9f3(0x358,0x4d0,0x7bb,0x60f)+_0x3f34aa(-0x12a,-0x144,0x1,-0x471)+'caption,\x20a'+_0x1da9f3(0x46a,0xb7,0x4c3,0x4aa)+_0x3f34aa(0x6a5,0x640,0x518,0x7bc),_0x3f34aa(0x353,0xa08,0x4e2,0x81)+_0x1da9f3(0x924,0x598,0x65c,0xc4e)+_0x3f34aa(0x518,-0x212,0x177,0x3fd)+_0x1da9f3(0x9ba,0x624,0x62a,0xe27)+_0x1da9f3(0x287,0x766,0x717,0x4b8),_0x1da9f3(0xa8d,0xe68,0xf94,0xe00)+_0x3f34aa(0x3d7,0x3c8,0x71,0x30c)+_0x3f34aa(0x145,0x642,0x663,0xadc)+'\x20\x22\x20di\x20depa'+_0x1da9f3(0x37f,0x73f,0x190,-0x182),_0x1da9f3(0xa6b,0x896,0xa48,0x80b)+_0x1da9f3(0x999,0xe4d,0x853,0x54a)+_0x3f34aa(0x123,0x18d,0x25b,0x38d)+_0x1da9f3(0x34d,0x3c2,0x5b,0x789),_0x1da9f3(0x894,0x6d7,0x8f9,0x657)+_0x3f34aa(0x56e,0x8e4,0x8ce,0x7f8)+_0x1da9f3(0x392,0x32c,0x5ee,-0x81)+_0x3f34aa(0x409,0x3da,0x458,0x61a)+'maung,\x20ker'+_0x3f34aa(0xb0c,0x290,0x62c,0x1d6)+'alang,\x20ker'+_0x3f34aa(-0x246,-0x86,0x27d,0x742)+'kas,\x20dll'],der=dare[Math[_0x1da9f3(0x580,0x85e,0x308,0x6cb)](Math[_0x1da9f3(0x957,0x45f,0x7c3,0x932)]()*dare['length'])],_0x376852={};_0x376852['displayTex'+'t']='☠️\x20Owner\x20';const _0x3a9de={};_0x3a9de[_0x1da9f3(0x4c3,0x776,0x14a,0x2ff)]=_0x3f34aa(0x400,-0x39d,0x12d,-0xc9),_0x3a9de[_0x1da9f3(0x8a3,0xa98,0x5bb,0x550)]=_0x376852,_0x3a9de[_0x1da9f3(0x847,0x5c6,0xc17,0x94f)]=0x1;const _0xfe12b7={};_0xfe12b7[_0x1da9f3(0xb79,0x8b4,0xf70,0x694)+'t']='Next\x20➡️';const _0x22e56e={};_0x22e56e['buttonId']=''+command,_0x22e56e['buttonText']=_0xfe12b7,_0x22e56e[_0x1da9f3(0x847,0x58a,0x9c5,0x694)]=0x1;var buttonns=[_0x3a9de,_0x22e56e];buttonMessage={'contentText':der,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+(_0x3f34aa(0xce4,0xa9d,0x848,0x318)+_0x3f34aa(0x809,0x602,0x3e5,0x702)+_0x1da9f3(0x92f,0xae4,0x8e2,0x6f6))+enter+_0x3f34aa(0xd32,0xd27,0x918,0x4e2)+sender[_0x1da9f3(0x97b,0x619,0xe7a,0xa85)]('@')[0x2*0x9e3+0xaad+-0x1e73],'buttons':buttonns,'headerType':0x1};const _0x4f2c28={};_0x4f2c28[_0x3f34aa(-0x19,0x7f8,0x434,0x3a4)]='hi',_0x4f2c28[_0x1da9f3(0x3eb,0x37e,-0x13,0x1af)+_0x3f34aa(0x8b0,0x33e,0x5b3,0xa3a)]=0x3b9aca00,_0x4f2c28[_0x3f34aa(0x66e,0x982,0x54f,0x989)+'d']=!![],_0x4f2c28[_0x1da9f3(0xa5b,0xc47,0x8f3,0xc75)+'ral']=!![],_0x4f2c28['mentionedJ'+'id']=[sender];const _0x27b558={};_0x27b558[_0x3f34aa(0x60a,-0x5c,0x2d0,0xc3)]=_0x1da9f3(0x1b6,-0x26a,-0xba,0x698)+'21',_0x27b558[_0x1da9f3(0x39f,0x17b,0xb1,0x469)+'o']=_0x4f2c28,_0x27b558[_0x3f34aa(0x269,0x2cc,0x7b9,0x9ff)]=ftoko,_0x27b558['sendEpheme'+_0x3f34aa(0x957,0x741,0x9a6,0x920)]=!![],alpha['sendMessag'+'e'](from,buttonMessage,MessageType['buttonsMes'+_0x3f34aa(0x717,0xd94,0x871,0x459)],_0x27b558),await limitAdd(sender,limit);break;case _0x3f34aa(0x45e,0x6b9,0x641,0x2e8):const _0x27bd52={};_0x27bd52[_0x1da9f3(0xb79,0xe8d,0x74a,0xf9f)+'t']=_0x3f34aa(0x7b7,0x726,0x4e1,0xdb)+'t';const _0x8810={};_0x8810[_0x3f34aa(0x166,0x88,0x30f,-0x45)]='limit',_0x8810['buttonText']=_0x27bd52,_0x8810[_0x1da9f3(0x847,0x419,0x692,0x890)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xb66,0x479,0x903)](prefix),'©\x20'+ownername,[_0x8810]);const trut=[_0x3f34aa(0x573,0x7d,0x267,0x768)+_0x3f34aa(0xca,0x3ec,0x397,0x6e9)+_0x1da9f3(0x753,0x31c,0xa3c,0x464)+'rapa\x20lama?',_0x3f34aa(0x830,0x494,0x723,0x466)+_0x3f34aa(0x31e,-0x344,0x1a1,0x2fe)+'au\x20mau,\x20di'+_0x1da9f3(0x7d1,0x737,0x463,0x75c)+'c\x20siapa\x20ya'+_0x1da9f3(0x3be,0x376,0xff,0x56f)+_0x1da9f3(0x71a,0xba7,0xba0,0x4ad)+_0x3f34aa(0x6d9,0x537,0x19d,-0xd9)+_0x3f34aa(0xac2,0x7d1,0x7cb,0xcce)+'/sma\x20jenis'+')',_0x1da9f3(0x55c,0x61c,0x6c1,0x47b)+_0x3f34aa(0xa9e,0x53a,0x913,0x5b9)+_0x3f34aa(0x310,0x611,0x576,0x42f),_0x3f34aa(0xb0b,0x60e,0x798,0x9a7)+_0x3f34aa(0x366,-0x25a,0x181,-0x1dd)+_0x3f34aa(0x8ce,0x3f2,0x79f,0x2c8)+_0x1da9f3(0x2b6,0x779,-0x17e,-0x17b)+'itu\x20suka\x20s'+'ama\x20kamu\x20j'+_0x1da9f3(0xaf1,0xd00,0xbe5,0xf4b),_0x1da9f3(0xa85,0xe22,0xef2,0x61f)+_0x3f34aa(0x96d,0x434,0x8e0,0xe36)+'car\x20teman\x20'+_0x3f34aa(0x387,0x770,0x4aa,0x4fb)+_0x3f34aa(0x7c5,0xb5d,0x778,0x470)+_0x3f34aa(0x6f5,0x957,0x983,0xe83)+_0x1da9f3(0xb47,0xa25,0x857,0xe91),_0x3f34aa(0x1b5,-0x20,0x2e8,0x3ff)+_0x3f34aa(-0x74,0x57e,0x23d,0x49f)+_0x1da9f3(0x978,0x46a,0x740,0x64c)+'tau\x20bokap?'+'\x20Alesanya?','hal\x20yang\x20b'+_0x1da9f3(0x8f7,0x634,0x729,0x3d8)+'g\x20pas\x20lu\x20l'+_0x3f34aa(0x328,0xac8,0x742,0x53f)+_0x3f34aa(0x659,0x5ec,0x9ac,0x683),_0x1da9f3(0x701,0x4f6,0x447,0x5d8)+_0x3f34aa(0x77e,0xa3f,0x82c,0x360)+_0x3f34aa(0xb62,0xb78,0x947,0xc00)+_0x3f34aa(0xc57,0xf57,0xa9b,0x895)+_0x1da9f3(0x2cf,-0x1c6,-0x168,0x4fd)+_0x3f34aa(0x2f0,0x86,0x25,-0x16f)+_0x1da9f3(0x254,-0x18b,-0x237,0x7)+'gimana\x20bro'+'u?',_0x3f34aa(0x632,0x8f4,0xaab,0x99f)+_0x3f34aa(-0xf1,0x133,0x1e1,-0x2e8)+'han\x20orang?',_0x1da9f3(0x483,0x194,0x121,0x1c1)+_0x3f34aa(-0x8c,-0x3d3,0xbf,0x53)+_0x1da9f3(0xa6d,0xe0d,0xc94,0x6f9),_0x1da9f3(0x361,0x536,-0x167,0x28)+_0x3f34aa(0x600,0xb80,0x7bc,0x3ed)+_0x1da9f3(0x21e,0x6ec,0x1b,0x480)+_0x3f34aa(0x476,0xb8f,0x879,0x350)+_0x3f34aa(0x4d4,-0x315,0x2d,-0x47d)+'anmu',_0x3f34aa(-0x2f,0x6b1,0x32d,0x754)+'ggakan\x20apa'+_0x1da9f3(0x98a,0x5ee,0x689,0x8aa)+_0x3f34aa(0xc0d,0x46d,0x953,0x886)+_0x3f34aa(0x282,0xd03,0x7b5,0xbf9)+'ni','siapa\x20oran'+_0x1da9f3(0x6c9,0x8af,0xc0d,0xadc)+_0x3f34aa(0x8ae,0x4fe,0x49f,0x892)+_0x3f34aa(0x943,0x493,0x72a,0x2b7),_0x3f34aa(0x283,0xc8,0x1ad,0x67c)+'g\x20yang\x20per'+_0x3f34aa(0x9dd,0x7a8,0x9bc,0x48d)+_0x1da9f3(0x6c1,0x924,0x9b3,0x61c),_0x3f34aa(0x918,0xb81,0xa91,0x54a)+_0x1da9f3(0x5f7,0xa5b,0x7bd,0x19c)+_0x3f34aa(0x410,0x5c8,0x943,0x879)+_0x3f34aa(0x445,0x806,0x8d5,0x9f4)+'?','Siapa\x20yang'+_0x1da9f3(0x6a2,0x4f4,0x1e3,0x34c)+_0x1da9f3(0xc20,0x9ba,0xeee,0xe63)+_0x1da9f3(0xb7a,0xf94,0xa0e,0xa25)+'n\x20idealmu\x20'+_0x3f34aa(0xe8b,0xd36,0x9c0,0x692),'suka\x20mabar'+_0x1da9f3(0x385,-0x1c2,-0x2f,-0x98)+_0x1da9f3(0x41d,0x5d0,0x68b,0x619)+_0x3f34aa(0x763,0x586,0x956,0x94a),_0x1da9f3(0x3b2,0x77e,0x5,-0xba)+_0x3f34aa(0x98d,0x87c,0x7fc,0x7c9)+_0x1da9f3(0x3a4,0x219,0x391,0x77a)+_0x3f34aa(0x462,-0xa7,0x1b4,0x6f0),_0x3f34aa(0xcb1,0x531,0x970,0xcdd)+_0x1da9f3(0x477,0x8b4,0x877,0x87f)+'ng\x20bikin\x20k'+'amu\x20sakit\x20'+_0x3f34aa(0x2ec,-0x53,0x130,0x461)+_0x3f34aa(0x6e6,0xabf,0x92c,0x65b)+_0x3f34aa(0x76f,0x35c,0x21e,0x71b),_0x1da9f3(0x292,0x695,-0x12e,0x5e7)+_0x3f34aa(0x970,0xb23,0x796,0xb26)+_0x3f34aa(0x887,0x766,0x6a5,0x844)+_0x3f34aa(0x6be,0xbf2,0x9be,0x8b0)+_0x3f34aa(0xe35,0xdec,0xa61,0xe85),_0x1da9f3(0x4c7,0x472,0x604,0x18e)+_0x1da9f3(0x5e3,0x251,0x577,0xaed)+_0x3f34aa(-0x10d,0x51d,0x343,0x293)+_0x1da9f3(0x429,0x66,0x3f1,0x13c)+'?'],ttrth=trut[Math[_0x1da9f3(0x580,0x6f5,0x723,0x336)](Math['random']()*trut['length'])],_0x1ce772={};_0x1ce772[_0x3f34aa(0xf1e,0x814,0x9c5,0x830)+'t']=_0x1da9f3(0xac5,0xd14,0x85d,0x5b6);const _0x38ea47={};_0x38ea47[_0x1da9f3(0x4c3,0x2ca,0x38c,0x64f)]=_0x1da9f3(0x2e1,0x5cd,0x7e4,-0x22a),_0x38ea47[_0x3f34aa(0x957,0x8a2,0x6ef,0x7a1)]=_0x1ce772,_0x38ea47[_0x1da9f3(0x847,0xaf3,0x7d0,0x53c)]=0x1;const _0x284ca9={};_0x284ca9[_0x1da9f3(0xb79,0xab6,0x93e,0xe16)+'t']=_0x1da9f3(0x935,0xdd0,0xb90,0x487);const _0x12ab4f={};_0x12ab4f['buttonId']=''+command,_0x12ab4f['buttonText']=_0x284ca9,_0x12ab4f[_0x1da9f3(0x847,0xa01,0x9b0,0x7ed)]=0x1;var buttonns=[_0x38ea47,_0x12ab4f];buttonMessage={'contentText':ttrth,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+(_0x3f34aa(0x707,0x874,0x848,0xadb)+_0x1da9f3(0x599,0x31d,0x186,0x24e)+_0x3f34aa(0xb5c,0x376,0x77b,0x9bb))+enter+_0x1da9f3(0xacc,0xd29,0xd04,0x83c)+sender['split']('@')[-0x160e+0x545+-0x1*-0x10c9],'buttons':buttonns,'headerType':0x1};const _0x2b952c={};_0x2b952c['text']='hi',_0x2b952c[_0x3f34aa(0x6f2,0x6ce,0x237,0x305)+_0x1da9f3(0x767,0x6a3,0xbc6,0x70c)]=0x3b9aca00,_0x2b952c[_0x1da9f3(0x703,0x9b5,0x672,0x296)+'d']=!![],_0x2b952c['sendEpheme'+'ral']=!![],_0x2b952c[_0x3f34aa(0x691,0x108,0x17b,-0x281)+'id']=[sender];const _0x4a7ec8={};_0x4a7ec8[_0x1da9f3(0x484,0x5d8,0x4c,0x5ca)]=_0x1da9f3(0x1b6,0x5d7,0x63c,-0x14a)+'21',_0x4a7ec8[_0x1da9f3(0x39f,0x697,0x86a,0x6e4)+'o']=_0x2b952c,_0x4a7ec8[_0x1da9f3(0x96d,0x54c,0xb87,0xb21)]=ftoko,_0x4a7ec8[_0x1da9f3(0xa5b,0x900,0xd73,0xbb5)+'ral']=!![],alpha[_0x1da9f3(0x21d,0x416,0x42d,0x116)+'e'](from,buttonMessage,MessageType[_0x3f34aa(-0xea,0x52b,0x431,0x2ee)+_0x1da9f3(0xa25,0x5d7,0xc1a,0x61d)],_0x4a7ec8),await limitAdd(sender,limit);break;case'katadilan':const _0x5a908f={};_0x5a908f[_0x1da9f3(0xb79,0xb2c,0xf90,0x827)+'t']=_0x1da9f3(0x695,0xa7f,0x348,0x8a2)+'t';const _0xa9e080={};_0xa9e080['buttonId']=_0x3f34aa(0x3cf,0x859,0x6d2,0x5a0),_0xa9e080[_0x3f34aa(0x46f,0x3be,0x6ef,0x1aa)]=_0x5a908f,_0xa9e080[_0x1da9f3(0x847,0x9f8,0x759,0xb8d)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0xa9e080]);const cc=[_0x3f34aa(0x4d7,0x269,0x36e,0x74c)+_0x1da9f3(0x910,0x742,0x598,0x449)+_0x3f34aa(0xa91,0x588,0x8d0,0xca7)+_0x3f34aa(-0x1ff,-0x335,0x19c,-0x67)+_0x1da9f3(0x502,0x323,0x5c8,0x57f)+_0x1da9f3(0x674,0x5b9,0x651,0x297)+_0x3f34aa(0xc1c,0x71a,0x8f3,0x5a4)+'ang\x20aku\x20se'+_0x1da9f3(0x223,0x178,-0x16c,-0x2af)+'\x20percaya\x20d'+_0x3f34aa(0x6d4,0x114,0x487,0x43c),_0x1da9f3(0x3d0,0x316,0x2c9,0x5a6)+_0x1da9f3(0xbcd,0x9b3,0x943,0xf84)+_0x1da9f3(0x9c0,0x5a4,0x996,0x8be)+_0x3f34aa(0x3c9,0x805,0x8f9,0xd20)+_0x1da9f3(0xc32,0xa0a,0xaaa,0xb5f)+'lin\x20aku,\x20a'+'ku\x20gak\x20bis'+_0x3f34aa(0xb9c,0xa40,0x698,0x20e)+_0x3f34aa(-0x46a,0x235,-0x5,0x39a)+_0x1da9f3(0x2d2,-0x250,0x337,0x226)+_0x1da9f3(0x284,-0xf5,0x146,0x470),_0x3f34aa(0x87,-0x214,0x5f,-0x47b)+_0x3f34aa(0x459,0x1c,0x33,-0x394)+_0x3f34aa(0x931,0xb9a,0xa3a,0xd32)+_0x1da9f3(0x9dd,0xea6,0xf1e,0x8bb)+_0x3f34aa(0xac9,0xb1a,0x9cd,0xe81)+_0x1da9f3(0xc0c,0xc64,0x110d,0xb7b),_0x1da9f3(0xb12,0xd9e,0xc8f,0xf29)+_0x3f34aa(0x854,0xace,0x955,0xb8e)+_0x1da9f3(0x782,0x9d9,0x696,0x797)+_0x1da9f3(0x755,0xa4c,0x30c,0xbfd)+_0x3f34aa(0x9ab,0x365,0x763,0x955)+_0x1da9f3(0x650,0x306,0x2e6,0xa62)+_0x3f34aa(0x5eb,0x51b,0x19e,-0x30)+_0x1da9f3(0x9e5,0xefe,0x8f4,0xcbc)+_0x1da9f3(0xa7a,0xb5a,0xd88,0xef7)+_0x3f34aa(0xa24,0xa82,0x844,0x52a)+_0x1da9f3(0x414,0x5bd,0x67b,0x29a)+_0x1da9f3(0xbfe,0xbcf,0xfca,0xed6)+_0x1da9f3(0xb7d,0x700,0x80a,0xbf6)+'mburu.',_0x1da9f3(0x3b6,0x518,0x849,0x78e)+'yang\x20kita\x20'+'kira\x20udah,'+_0x1da9f3(0xa13,0x631,0x9ae,0x87a)+'adi\x20udah\x20p'+_0x1da9f3(0x2f1,0xd5,0x194,0x34a)+_0x1da9f3(0x4df,0x139,0x38,0xa17)+_0x3f34aa(0x678,0x6b0,0x936,0xb46)+'\x20ku\x20ulang\x20'+_0x3f34aa(0x183,0x3e8,0x56c,0x5a5)+_0x3f34aa(0x605,0x5ac,0x8cd,0xbf3),_0x3f34aa(0x69b,0x20e,0x4db,0x360)+_0x3f34aa(0x857,0xc9,0x372,-0xc5)+_0x3f34aa(0x8ac,0xaff,0x8d8,0x6e8)+'ak\x20akan\x20ku'+_0x1da9f3(0x7ee,0xbb8,0x7fd,0x898)+_0x1da9f3(0x264,-0x172,0x68e,-0xa6),_0x3f34aa(0x609,0x597,0x5c1,0x3a4)+_0x3f34aa(0x5c9,0x8df,0x6a0,0x8c0)+_0x3f34aa(-0x314,-0x3ee,0xf1,-0x391)+_0x3f34aa(0x63e,-0x34,0x45c,0x4f6)+_0x3f34aa(0x766,0x588,0x85c,0xa02)+_0x1da9f3(0xa3c,0xc8e,0x67a,0x637)+_0x3f34aa(-0x394,-0x173,0x144,-0x19),'Jika\x20hujan'+',\x20aku\x20tak\x20'+_0x1da9f3(0x8ca,0xd6a,0xdb0,0x3e8)+_0x1da9f3(0xa90,0x833,0x657,0xc39)+'.\x20Sebab\x20ji'+_0x3f34aa(0x827,0x90e,0x764,0x409)+_0x1da9f3(0x3ec,0x6ae,0x351,0x15d)+_0x1da9f3(0x872,0x8bb,0x3bb,0x81f)+_0x3f34aa(0x976,0x3cf,0x551,0x29b)+_0x3f34aa(-0xd3,0x4f0,0x14d,-0x3b8),'Kalau\x20menc'+'intaimu\x20ad'+'alah\x20kesal'+'ahan,\x20yasu'+_0x3f34aa(0x97e,0x757,0x971,0x9d5)+'\x20Aku\x20salah'+_0x1da9f3(0x792,0xc5f,0x5e1,0xa6b)+'a.',_0x1da9f3(0x53f,0x24e,0x80b,0x445)+_0x1da9f3(0x2c7,0x7ee,0x715,0x6b4)+_0x3f34aa(0xce,0x55b,0x46c,0x99c)+_0x3f34aa(0x32f,0x7a3,0x808,0x321)+_0x3f34aa(0x3a9,0x1e1,0xef,-0x2d7)+_0x1da9f3(0x74f,0x403,0xc2c,0x7fa)+_0x1da9f3(0x96c,0x5b6,0x4e9,0x80a)+_0x3f34aa(0x45b,0x929,0x8d6,0x6bb)+_0x3f34aa(0x1c0,0x294,0x2f,0x2f5)+_0x1da9f3(0x216,-0x9b,0x2eb,0x1d0)+_0x1da9f3(0x801,0x9e8,0x415,0xb67),_0x3f34aa(0x439,0x741,0x5c1,0x656)+_0x1da9f3(0x6ed,0x8d4,0x5ec,0x381)+'\x20aku,\x20itu\x20'+'hak\x20kamu,\x20'+_0x3f34aa(0x51,0xa3,0x244,0x411)+_0x1da9f3(0xb7c,0x985,0x6c8,0xbc7)+_0x1da9f3(0x6ed,0x7dd,0x637,0xb5e)+_0x1da9f3(0xc3e,0x115e,0xe47,0x927)+_0x1da9f3(0x7f0,0x7eb,0x615,0xd42)+_0x3f34aa(-0x33c,0x20d,0x212,0x3d7),'Aku\x20gak\x20pa'+'ndai\x20cembu'+_0x1da9f3(0x9c0,0x66f,0xba6,0xd85)+_0x1da9f3(0xaad,0xeae,0xb33,0xdaf)+'amu\x20ningga'+_0x3f34aa(0x415,0x621,0x5a3,0x569)+_0x3f34aa(0x202,0x7ee,0x42b,0x4f8)+'a\x20apa-apa.'+_0x1da9f3(0x1af,-0x340,-0x156,0x6f9)+_0x3f34aa(-0x34e,-0xe0,0x11e,0x635)+_0x1da9f3(0x8b8,0x41a,0x719,0x82b),_0x3f34aa(0xa9e,0x803,0x9dc,0xb12)+'\x20pergi\x20jau'+_0x3f34aa(0x890,0xded,0xa49,0x524)+'\x20ada\x20darah'+_0x3f34aa(0x6bd,0x7ea,0x3ea,0x1fc)+_0x3f34aa(0x29e,0x50f,0x201,0x4a4),_0x3f34aa(0x411,0xd4,0x2a9,-0x147)+_0x1da9f3(0xb03,0xfbf,0x713,0xb19)+_0x1da9f3(0x68a,0xa62,0x7ac,0x96b)+_0x1da9f3(0x508,0x50c,0x685,0x770)+',\x20waktu\x20me'+_0x3f34aa(0x77e,0x83a,0x586,0x489)+_0x1da9f3(0x691,0x27b,0x261,0x832)+_0x1da9f3(0x1fc,-0x1a9,0x3bb,0x10b)+_0x3f34aa(0x220,0x484,0x4fe,0x84a)+_0x1da9f3(0x5ad,0x3e8,0x185,0x26c)+_0x3f34aa(0x13a,0x44f,0x20,0xe0)+_0x3f34aa(0x343,0xab9,0x70c,0x6aa)+_0x1da9f3(0xb3a,0x893,0x5e2,0xa80),'Bagiku,\x20Ka'+_0x1da9f3(0xaa3,0x8cc,0xa3b,0xc2f)+_0x3f34aa(0x64b,0x59e,0x7ef,0xa38)+_0x1da9f3(0x4b5,0x35f,0x2bd,0x9de)+_0x1da9f3(0x636,0x512,0x9af,0x951)+_0x1da9f3(0x99e,0x900,0x820,0xc89)+_0x1da9f3(0xb56,0xe54,0x8f6,0xe1d)+_0x3f34aa(0x269,0xb91,0x6d1,0xc07)+_0x3f34aa(0xba2,0x7c3,0x72b,0x4d5)+'\x20yang\x20kamu'+_0x3f34aa(0x1ef,-0x1c6,0x183,0x27d)+'\x20tetapi\x20ti'+_0x3f34aa(-0x2d3,0x4de,0x174,-0x268)+_0x3f34aa(0x46b,0x554,0x249,-0x23e)+_0x3f34aa(0x66a,0x355,0x834,0x63d)+_0x3f34aa(0x405,0x65f,0x132,0x652)+_0x1da9f3(0x609,0x137,0x860,0x3cb)],si=cc[Math[_0x1da9f3(0x580,0x1ec,0x295,0x6c8)](Math['random']()*cc[_0x3f34aa(0x3dc,0x468,0x92e,0x9ea)])],_0x81562c={};_0x81562c[_0x1da9f3(0xb79,0xe79,0x1004,0xcbc)+'t']=_0x3f34aa(0x8eb,0xa75,0x911,0xd35);const _0x20c2cd={};_0x20c2cd['buttonId']=_0x3f34aa(0x8f,0x54f,0x12d,0x44),_0x20c2cd['buttonText']=_0x81562c,_0x20c2cd['type']=0x1;const _0x1e8eb0={};_0x1e8eb0['displayTex'+'t']=_0x1da9f3(0x935,0xd69,0x3ef,0x769);const _0x3598f6={};_0x3598f6[_0x3f34aa(-0x122,-0x8c,0x30f,-0x1f1)]=''+command,_0x3598f6[_0x3f34aa(0x876,0x717,0x6ef,0x5f1)]=_0x1e8eb0,_0x3598f6[_0x1da9f3(0x847,0xd84,0xcca,0x9a2)]=0x1;var buttonns=[_0x20c2cd,_0x3598f6];buttonMessage={'contentText':si,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+(_0x1da9f3(0x9fc,0x53f,0x6bf,0x8ac)+_0x3f34aa(-0x12a,0x819,0x3e5,-0x1f)+_0x1da9f3(0x92f,0xd18,0x943,0x916))+enter+_0x1da9f3(0xacc,0xbcc,0x778,0xd37)+sender[_0x3f34aa(0x945,0x341,0x7c7,0x80e)]('@')[0x7*0x133+0x109d+-0x1902],'buttons':buttonns,'headerType':0x1};const _0x37de73={};_0x37de73[_0x3f34aa(0x1b6,0x28f,0x434,0x420)]='hi',_0x37de73['forwarding'+_0x3f34aa(0xa41,0xe3,0x5b3,0x4ba)]=0x3b9aca00,_0x37de73[_0x1da9f3(0x703,0x38a,0xa23,0x696)+'d']=!![],_0x37de73[_0x3f34aa(0x903,0x794,0x8a7,0x9c6)+_0x1da9f3(0xb5a,0x802,0x931,0x8fb)]=!![],_0x37de73['mentionedJ'+'id']=[sender];const _0x3353ca={};_0x3353ca['caption']=_0x1da9f3(0x1b6,0x47e,0xbc,0x180)+'21',_0x3353ca[_0x1da9f3(0x39f,0x614,-0x18c,0x7a7)+'o']=_0x37de73,_0x3353ca['quoted']=ftoko,_0x3353ca[_0x3f34aa(0x774,0x80a,0x8a7,0x895)+_0x3f34aa(0x7e3,0x50d,0x9a6,0xa1a)]=!![],alpha[_0x3f34aa(0x3a4,0x4f1,0x69,-0x32a)+'e'](from,buttonMessage,MessageType[_0x1da9f3(0x5e5,0x297,0x4a2,0x5b3)+_0x1da9f3(0xa25,0xee7,0xd38,0x987)],_0x3353ca),await limitAdd(sender,limit);break;case _0x1da9f3(0x48d,-0x81,0x254,-0x62)+'lucu':const _0x1d2c17={};_0x1d2c17[_0x1da9f3(0xb79,0xc35,0x80c,0xf78)+'t']=_0x3f34aa(0x379,0x425,0x4e1,0x1d0)+'t';const _0xa439ec={};_0xa439ec[_0x1da9f3(0x4c3,0x7de,0x555,0x3d3)]='limit',_0xa439ec['buttonText']=_0x1d2c17,_0xa439ec['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0xa439ec]);const kata_lucu=['“Orang\x20bij'+_0x3f34aa(0x3ca,-0x24,0x3f,0x212)+_0x1da9f3(0xa6c,0xf9f,0xf58,0xc58)+_0x3f34aa(0x475,0x798,0x87e,0x4ce)+'menikah\x20me'+_0x1da9f3(0xb62,0x6f5,0x954,0xad4)+_0x1da9f3(0xa7c,0xaba,0xa7c,0xfcd)+_0x3f34aa(0x761,0x279,0x46f,0x464)+_0x3f34aa(0x187,0x156,0x2b4,-0x2a5),_0x1da9f3(0x377,0x387,-0xa,0x4c3)+_0x3f34aa(0x7a,-0x3a0,0xc9,-0x1d7)+_0x1da9f3(0x926,0xc7e,0x5e6,0xa76)+'ang\x20tertun'+'da…kebohon'+_0x1da9f3(0x81d,0x64e,0x9d4,0xd30)+_0x3f34aa(0x6f0,0xc85,0xa2c,0x7f2)+_0x1da9f3(0x685,0x4ff,0x429,0x305)+_0x3f34aa(0x859,0xc7d,0xa7c,0xd08),_0x1da9f3(0x2b7,0x23c,-0x1f8,0x488)+_0x3f34aa(0x625,0x5f6,0x9bd,0x66f)+_0x1da9f3(0x3d4,0x557,0x7e,0x2e3)+_0x1da9f3(0x694,0x8fe,0x6a8,0x362)+_0x3f34aa(0x207,0x319,0x1c9,0x345)+_0x3f34aa(0x2ec,0x503,0x3a9,0x13),'“Tidak\x20ada'+_0x1da9f3(0x7a9,0xae0,0x954,0xa10)+_0x1da9f3(0xa15,0x697,0xbf1,0xd22)+_0x1da9f3(0x6b7,0x458,0x2d2,0x7b3)+_0x3f34aa(0xde,0x13a,0x1f4,-0x15)+'rjaan\x20sebe'+_0x1da9f3(0x6ff,0x49d,0x774,0x7f6)+_0x3f34aa(0x2ab,0xad0,0x58d,0x45a)+_0x3f34aa(0xcd9,0xefd,0x9cb,0x9bc)+_0x1da9f3(0x683,0x73f,0x15a,0x927)+'k\x20dikerjak'+_0x3f34aa(0x954,0x68d,0x9d1,0x4fa),'“Sayang…\x20t'+'adi\x20pagi\x20a'+_0x1da9f3(0x570,0xa3,0x7f8,0xa62)+_0x3f34aa(0x4c9,-0xc8,0x1fd,0x2ab)+'karena\x20mer'+_0x3f34aa(0x30a,0x606,0x74a,0x687)+_0x3f34aa(0x4f4,0x142,0x16d,-0x3d)+_0x1da9f3(0x42f,0x78c,0x392,0x682)+_0x3f34aa(0x43e,0x16e,0x253,0x30)+_0x3f34aa(-0x4f,0x86d,0x3d9,0x8d3)+_0x1da9f3(0x892,0x67e,0xcf4,0x3a4)+_0x3f34aa(0x99f,0x168,0x684,0x7ca)+_0x3f34aa(0x56c,0x870,0x3ec,0x684)+'\x20tidak\x20bis'+'a\x20tidur\x20ka'+'rena\x20kelap'+_0x1da9f3(0x928,0x669,0x628,0x9b2),_0x3f34aa(0xa18,0x68e,0x545,0x212)+_0x1da9f3(0xc38,0xec2,0xb18,0xc9d)+_0x3f34aa(0x40d,0x579,0x8eb,0xe0d)+_0x1da9f3(0x227,0x602,0x4f8,0x49a)+'an,\x20dan\x20ji'+_0x1da9f3(0xc16,0x9e8,0xe74,0x8a7)+_0x1da9f3(0x469,0x67,0x55a,-0x6)+_0x1da9f3(0x3aa,0x35f,0x73c,0x7fd)+_0x3f34aa(0x616,0xa8,0x489,-0xcf)+'gkin\x20jodoh'+_0x3f34aa(0x629,0xc0d,0x866,0x3e1)+_0x3f34aa(0x98b,0xc65,0x949,0xcb3)+_0x3f34aa(0x1e4,0xa44,0x5ad,0xa59)+_0x1da9f3(0x2d6,0x1c5,0x528,0x162),'“Kesuksesa'+_0x1da9f3(0x374,-0x3,-0x7d,-0x190)+'wal\x20dari\x20s'+_0x3f34aa(0x3d5,0xca0,0x907,0xe10)+_0x1da9f3(0x4e5,0x2a5,0x1af,0x14f)+'pian\x20hadir'+'\x20disaat\x20ki'+_0x3f34aa(0x42,-0xb0,0x351,0x445)+'Jadi\x20jika\x20'+_0x1da9f3(0x1db,0x43c,0x53f,-0x2bc)+_0x3f34aa(0x32f,0x370,0x7b,0x4c4)+_0x1da9f3(0x57c,0x3b3,0x557,0x923)+_0x3f34aa(0x6a7,0x87,0x2b0,0x6c6)+_0x1da9f3(0x931,0x798,0xe19,0x549)+_0x1da9f3(0x93a,0x9ab,0x916,0x446)+_0x3f34aa(0x960,0xd9a,0x99b,0x7d1),_0x3f34aa(-0xde,0x237,0x3fb,0x7b7)+_0x1da9f3(0xc00,0xe21,0xcdb,0xad7)+_0x1da9f3(0x70b,0x78c,0x659,0x3ea)+'aminan\x20unt'+'uk\x20kehidup'+'an\x20yang\x20ba'+_0x1da9f3(0x1c5,0x56a,0x217,0x9c)+_0x3f34aa(-0x145,0x5cf,0x268,0x7c1)+_0x1da9f3(0x897,0x64f,0x96a,0x83e)+'apalagi\x20wa'+_0x1da9f3(0x8ae,0xdaa,0xc45,0x764)+'jelek.”',_0x1da9f3(0x330,0x598,-0x1a4,0x82c)+_0x3f34aa(0x3e9,0x7da,0x44d,0x7f9)+'ehat,\x20terd'+_0x3f34aa(0x578,-0x454,0x5a,0x3b9)+_0x1da9f3(0x652,0x4bc,0x4e0,0x45b)+_0x1da9f3(0x26c,0x530,-0x113,0x420),_0x1da9f3(0x290,-0x7d,0x3f8,0x701)+_0x1da9f3(0xb19,0xa1a,0xbb3,0xd58)+'\x20Tetapi\x20ja'+_0x3f34aa(0x870,0xb6a,0x646,0x91d)+_0x1da9f3(0x544,0x32c,0x910,0x89f)+_0x3f34aa(0xa8b,0x6e4,0xa2f,0xe10),'“Uang\x20buka'+_0x3f34aa(0xa47,0x807,0x926,0x4b5)+'a…Masih\x20ad'+_0x1da9f3(0x9b5,0xb52,0xb23,0x7d3)+_0x1da9f3(0x542,0x64a,0xa86,0x113)+_0x1da9f3(0x233,-0x2e1,0x2dd,-0x21e),_0x1da9f3(0x536,0x312,0x5a,0x8ea)+'aik\x20untuk\x20'+_0x3f34aa(0x94b,0x2a2,0x697,0x3f2)+_0x3f34aa(0x68e,0x4e3,0x996,0xc6a)+'adalah\x20ban'+_0x1da9f3(0x71d,0x3d9,0xbd4,0x2f3)+_0x1da9f3(0x65d,0x1d7,0x648,0x7a9)+_0x3f34aa(0x87d,0x9e0,0x9bf,0x957),'“Diam\x20itu\x20'+_0x1da9f3(0x5ee,0x7ce,0x515,0x88e)+_0x1da9f3(0x4f2,0x5e5,0x3ae,0x9d0)+_0x1da9f3(0x4b2,0x109,0x165,0x86f)+_0x1da9f3(0xab9,0x8c5,0x622,0x705)+_0x1da9f3(0x5dc,0x8e9,0x149,0x866)+_0x1da9f3(0xb1d,0x76d,0xec5,0xd7a)+_0x3f34aa(0x229,-0x267,0xb8,-0x441),_0x1da9f3(0xb95,0x1058,0x8c4,0xf8c)+'\x20mencapai\x20'+'kesuksesan'+_0x1da9f3(0x6e3,0xb01,0xbe5,0x8cd)+_0x3f34aa(0x73e,0xb31,0x7de,0xd29)+_0x3f34aa(0x1cb,0x4f5,0x575,0x642)+_0x1da9f3(0x849,0x65b,0x9c2,0x7e0)+_0x1da9f3(0xc4d,0xfcd,0xd29,0xada)+_0x3f34aa(0x493,0x5f4,0x41b,-0x4d)+_0x1da9f3(0x766,0xb0f,0xbfa,0xa45)+_0x3f34aa(0x55f,-0x92,0xa9,0x30c)+_0x3f34aa(0x31b,0xa3a,0x4ef,0x142)+_0x3f34aa(0x391,0xa24,0x6ce,0x5ee)+'\x20saja\x20tema'+'nmu\x20yang\x20a'+'kan\x20menert'+'awakan.”','“Setiap\x20ma'+_0x3f34aa(0x17c,0x456,0x605,0x9dc)+'i\x20akan\x20ada'+_0x3f34aa(0x543,0x3b2,0x440,0x790)+_0x1da9f3(0x282,0x116,0x2a2,0x5c0)+_0x1da9f3(0x5a1,0xa9e,0xa9f,0x837)+_0x1da9f3(0x7ac,0x7d5,0x2ef,0x6d7)+_0x1da9f3(0x1d0,0x23d,-0x9d,0x189)+_0x3f34aa(0xb4,0x21,0x1bf,-0x26b)+_0x1da9f3(0xba8,0x8c0,0xca7,0x1006)+_0x1da9f3(0xa06,0x5b4,0x89b,0x4ac)+_0x1da9f3(0x438,0x763,0x11a,0x8ff)+_0x1da9f3(0x4ef,0x8f6,0x91e,0xc7),_0x3f34aa(-0x3f7,0x20d,0x31,0x46f)+'ntaranmu\x20t'+'idak\x20sangg'+_0x3f34aa(0x528,-0x91,0x66,0x46f)+_0x1da9f3(0x795,0xa84,0x5d3,0xae8)+'dan\x20memuka'+_0x3f34aa(0x387,0x430,0x5cb,0x91b)+_0x3f34aa(0x3d5,0xf9,0x44b,0x5b8)+_0x3f34aa(0xcc5,0x60a,0x7df,0xc09)+_0x3f34aa(0xabe,0x531,0x656,0x100)+'an\x20kebodoh'+_0x1da9f3(0x8f5,0xb56,0x610,0x886),'“Pintar\x20it'+'u\x20adalah\x20s'+'eseorang\x20y'+_0x3f34aa(0xb1c,0x385,0x71e,0xb3c)+_0x1da9f3(0xbd4,0xc58,0xfa5,0xf0e)+_0x1da9f3(0x465,-0x29,0x3b7,-0x35)+_0x3f34aa(0xa1a,0x35c,0x6bd,0x5d6)+_0x3f34aa(-0x462,-0x1bb,0x22,-0x4ad)+_0x3f34aa(0x689,0x253,0x48d,0xe3)+'lah\x20seseor'+_0x1da9f3(0xae9,0x67f,0xab9,0xb06)+_0x1da9f3(0x466,0x3d,0x807,0x48a)+_0x3f34aa(-0x252,0x1a7,0x21d,-0x29d)+_0x3f34aa(0x45a,0x6db,0x463,0x475),_0x3f34aa(0x6f5,-0x2f7,0x1c3,-0x1f2)+_0x1da9f3(0x27d,-0x1be,-0xf0,0x101)+_0x1da9f3(0x2bb,0x37,0xba,0x621)+'\x20yang\x20tert'+_0x3f34aa(0x75f,0x961,0x558,0x620)+_0x1da9f3(0x958,0x84c,0xdcb,0x878)+'\x20adalah\x20ke'+'jujuran\x20ya'+'ng\x20tertund'+_0x1da9f3(0x233,-0x2d6,-0x323,0x44b),_0x1da9f3(0x410,0x21a,-0x142,0x48e)+_0x1da9f3(0x5fb,0x2cd,0x9c1,0x643)+'itu\x20tidak\x20'+_0x3f34aa(0x121,0x374,0x678,0x2c6)+_0x3f34aa(-0x14c,0x102,0x40a,0x13a)+_0x1da9f3(0x3cd,0x584,0x5ea,0x6dc)+_0x1da9f3(0xc22,0xaa6,0xce2,0xb6d)+_0x1da9f3(0x36f,0x288,0x2f0,0x84f)+_0x1da9f3(0xbd2,0x67b,0xc39,0x1064)+_0x1da9f3(0x959,0xcbc,0x546,0x936)+'\x20Valery.',_0x1da9f3(0x2ab,0x3bd,0x600,-0x1d5)+_0x3f34aa(0x57f,0x221,0x4e3,0x4af)+_0x1da9f3(0x46e,0x3e5,0x9c6,0x87c)+_0x1da9f3(0x3a2,0x675,0x170,0x8ab)+'ta…tapi\x20sa'+_0x1da9f3(0x8b4,0xc0c,0x423,0x73a)+_0x1da9f3(0x5ed,0x334,0x845,0x57e)+_0x1da9f3(0x220,-0x22e,0x3c8,0xf3),_0x3f34aa(0x93d,0x44f,0x7b7,0x34f)+_0x3f34aa(0xb80,0xea4,0xa64,0x7c5)+_0x1da9f3(0xa4c,0xc0f,0xc4a,0x5d9)+_0x3f34aa(0x3b8,0x802,0x823,0xb98)+'ar\x20seharus'+_0x3f34aa(-0x12c,0x4a7,0x7d,0x22b)+_0x3f34aa(0x4f9,0x323,0x78c,0xba6)+'\x20menghalan'+_0x3f34aa(0x51b,0x99,0x3c6,0x88f)+_0x1da9f3(0x932,0xdf4,0x96c,0x8c8)+_0x1da9f3(0x4aa,0x457,0x201,0x254),'“Berakit-r'+_0x3f34aa(0xadb,0x647,0x5d6,0x18f)+_0x3f34aa(0x2cc,-0x15,0x309,0x170)+'-renang\x20ke'+_0x1da9f3(0x8bc,0x7f8,0x90b,0x5f1)+'rsakit-sak'+_0x3f34aa(0xe1d,0x92f,0xa6a,0x8e6)+_0x1da9f3(0xbd6,0xa1b,0x92c,0x8d0)+_0x1da9f3(0x660,0x84b,0x93d,0x361)+_0x3f34aa(0xad4,0xa35,0x8ba,0x967),_0x1da9f3(0x24c,0x4b,-0xb4,0x3e7)+'rnah\x20jatuh'+_0x3f34aa(0x6f4,0x882,0x747,0x640)+_0x3f34aa(0x1f0,0x274,0x19b,0x5fb)+'g,\x20karena\x20'+_0x3f34aa(0x556,-0x1ea,0xb6,0x46e)+_0x1da9f3(0xbce,0xad9,0x948,0xe7b)+_0x3f34aa(0xdc2,0xc3b,0x9d6,0xe74)+'na\x20kalau\x20s'+_0x3f34aa(0x5d1,0x5af,0x240,0x281)+_0x3f34aa(0x508,0x1b0,0x6b8,0x529),_0x3f34aa(0x833,0x97d,0x9b8,0xd0a)+'membuat\x20ki'+_0x1da9f3(0x6ba,0x2c0,0x1f6,0x3ae)+_0x1da9f3(0x205,0x3d9,0x709,0x1cd)+_0x1da9f3(0x9f2,0xb1f,0x549,0xad3)+_0x1da9f3(0xb01,0x6ae,0x9c9,0x867)+'sia\x20yang\x20s'+_0x3f34aa(0x6c0,0xcf4,0x92f,0x994)+_0x3f34aa(0x420,0x2a0,0x48a,0x736)+_0x1da9f3(0x649,0x88e,0x453,0x452)+_0x1da9f3(0x360,-0x1cf,0x5b1,0x816)+_0x1da9f3(0xb52,0xd27,0x6ba,0xfe6),_0x1da9f3(0x9a8,0xdc5,0xbc6,0x546)+_0x1da9f3(0xb3d,0xbab,0x89a,0xc1c)+_0x3f34aa(0x431,0x713,0x221,0x59f)+_0x3f34aa(0x629,0xc88,0x7cd,0x9f7)+_0x3f34aa(0x355,0xa6b,0x887,0x77d)+_0x3f34aa(0x513,0x7a0,0x38d,0x53a)+_0x3f34aa(-0x306,-0x3,0x24b,-0x12)+'ik…\x20Lupa\x20a'+'ku\x20pake\x20ce'+_0x1da9f3(0x31e,0x29a,0x7f3,0x5c8),_0x3f34aa(0xce1,0xda2,0x939,0x3ec)+'anyak\x20bela'+_0x3f34aa(0x469,0xd0,0x40b,0x869)+_0x3f34aa(0x484,0x96d,0x9c7,0xdab)+_0x3f34aa(0xb18,0x990,0x832,0xc6f)+_0x1da9f3(0x47a,0x9b6,0x520,0x397)+'kin\x20banyak'+_0x3f34aa(0x6fc,0xc0,0x5ea,0x656)+_0x1da9f3(0x87e,0xb4c,0x48e,0x9f2)+_0x1da9f3(0x75e,0x546,0x2a9,0x91b)+_0x1da9f3(0x8dd,0xc06,0x9df,0x611)+_0x3f34aa(0x620,0x824,0x686,0xa44)+_0x3f34aa(0x178,0x3b8,0x9e,-0x47b)+'ak\x20yang\x20ki'+_0x3f34aa(0xa1f,0x903,0x60e,0x148)+_0x3f34aa(0x5d6,0x4dc,0x5c7,0x6dc)+_0x1da9f3(0x411,0x4eb,0x181,0x531)+_0x1da9f3(0x251,-0x1fd,0x305,0x529)+'\x20Jadi\x20kena'+'pa\x20kita\x20si'+_0x3f34aa(0xd5a,0xe34,0x8df,0xcb0)+_0x3f34aa(0x7f3,0x5f0,0x2c0,0x21c),_0x1da9f3(0x485,0x7f4,0x51d,0x2a6)+_0x1da9f3(0x951,0xd63,0x59c,0xb0e)+_0x3f34aa(0x669,0x782,0x8bf,0x7d1)+_0x1da9f3(0x73b,0xb71,0xa40,0x63e)+'\x20janganlah'+_0x3f34aa(0x5e6,0x94a,0x7c1,0x90e)+_0x3f34aa(0x1ec,0x58c,0x449,-0x4e)+_0x1da9f3(0xbb4,0x6f0,0xa77,0xdc1)+'aslah\x20deng'+_0x3f34aa(0x52d,0x59f,0x713,0x371)+'n\x20dan\x20lemp'+_0x3f34aa(0x907,0xa00,0x7ff,0x9ac)+_0x1da9f3(0x5db,0x33c,0x9a4,0x93f)+'\x20Tapi\x20usah'+'akan\x20potny'+_0x1da9f3(0xb6b,0x89e,0x1003,0x7bf)+'lempar.”',_0x3f34aa(-0x5a,0x460,0x15,0x2ce)+_0x1da9f3(0x5c2,0x9b5,0xa9b,0x7b7)+'\x20menjadi\x20j'+_0x1da9f3(0x560,0x3f,0x13,0x324)+_0x3f34aa(0xbd9,0x484,0x984,0x5f1)+'a\x20menyenan'+'gkan,\x20apal'+_0x3f34aa(0x467,-0x178,0x24f,0x4a)+_0x1da9f3(0xbd5,0x97f,0x8b8,0x6d5),_0x1da9f3(0x898,0xa66,0x772,0x9dc)+'stri\x20yang\x20'+_0x3f34aa(0x450,0x44,0x278,-0x287)+_0x1da9f3(0x8b1,0x427,0xae9,0x3f3)+_0x1da9f3(0x293,0x217,0x1c,0x497)+'gar\x20jadi\x20S'+'AKINAH\x20(Se'+_0x3f34aa(0x16f,0x531,0x2e6,0x10d)+_0x1da9f3(0x569,0x37f,0x14d,0x4a9)+'ah)\x20&\x20jadi'+_0x1da9f3(0x500,0x548,0x667,0x300)+'yang\x20SIAGA'+'\x20(Siap\x20Ant'+_0x1da9f3(0xc3c,0xfcd,0xda2,0x10a8)+_0x3f34aa(0x3f3,0x14a,0x205,-0x4f)+_0x1da9f3(0x20d,-0x286,0x5a1,-0x336),_0x1da9f3(0xc2d,0x92f,0x10a8,0x10f3)+_0x1da9f3(0x7a9,0xbf9,0x86d,0x632)+_0x1da9f3(0xa15,0xdc3,0xb37,0xcc0)+_0x1da9f3(0xc46,0xb01,0x871,0xfee)+_0x1da9f3(0xb68,0x809,0x68b,0x996)+_0x3f34aa(0x9dc,0x1fc,0x5b1,0x4cc)+_0x1da9f3(0x56a,0x5f8,0x73c,0x4c2)+'terasa\x20rin'+_0x3f34aa(0x9e0,0x7bd,0x7bb,0xa68)+_0x3f34aa(0xbd,0x753,0x40f,0x526)+_0x1da9f3(0x4e7,0x5e3,0x14d,0x939)+_0x1da9f3(0xa2a,0x73b,0x81c,0x71f)],kata_lucu_=kata_lucu[Math['floor'](Math[_0x3f34aa(0x3a1,0xcb3,0x7a3,0x583)]()*kata_lucu[_0x1da9f3(0xae2,0xa97,0x7a1,0xbdf)])],_0x4f67f3={};_0x4f67f3[_0x1da9f3(0xb79,0xce2,0xfa4,0xf4a)+'t']=_0x3f34aa(0x90b,0xabd,0x911,0x84e);const _0x161dde={};_0x161dde['buttonId']=_0x1da9f3(0x2e1,0x86,0x63d,0x63b),_0x161dde[_0x1da9f3(0x8a3,0x401,0xa35,0x64f)]=_0x4f67f3,_0x161dde[_0x3f34aa(0x744,0x9a5,0x693,0x7b4)]=0x1;const _0x32f5de={};_0x32f5de[_0x3f34aa(0xc96,0x55f,0x9c5,0xbc5)+'t']=_0x1da9f3(0x935,0x474,0x880,0x526);const _0x440a3e={};_0x440a3e[_0x1da9f3(0x4c3,0x43c,0x748,0x4db)]=''+command,_0x440a3e[_0x3f34aa(0x38f,0x584,0x6ef,0x2a2)]=_0x32f5de,_0x440a3e['type']=0x1;var buttonns=[_0x161dde,_0x440a3e];buttonMessage={'contentText':kata_lucu_,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+('Next\x20\x20for\x20'+_0x1da9f3(0x599,0x304,0x358,0x19d)+_0x1da9f3(0x92f,0x86f,0xb46,0x9d1))+enter+'~\x20@'+sender[_0x1da9f3(0x97b,0xde1,0x4c0,0x6a5)]('@')[0x1106+-0x207*0x1+-0xeff],'buttons':buttonns,'headerType':0x1};const _0x2be4c3={};_0x2be4c3[_0x1da9f3(0x5e8,0x823,0x4b4,0x89d)]='hi',_0x2be4c3[_0x1da9f3(0x3eb,0x8d2,0x131,0x77)+_0x1da9f3(0x767,0x383,0x8cc,0x6fd)]=0x3b9aca00,_0x2be4c3[_0x1da9f3(0x703,0x8dc,0xa82,0x769)+'d']=!![],_0x2be4c3[_0x3f34aa(0x91a,0x70b,0x8a7,0xa74)+_0x3f34aa(0x74c,0x769,0x9a6,0x9f8)]=!![],_0x2be4c3['mentionedJ'+'id']=[sender];const _0x12701f={};_0x12701f['caption']=_0x3f34aa(0x469,-0x6e,0x2,-0x1b)+'21',_0x12701f[_0x1da9f3(0x39f,0x703,-0x18f,-0x155)+'o']=_0x2be4c3,_0x12701f[_0x3f34aa(0xa0a,0x33e,0x7b9,0x4ed)]=ftoko,_0x12701f['sendEpheme'+_0x3f34aa(0x786,0x609,0x9a6,0x6ac)]=!![],alpha[_0x3f34aa(-0x247,-0x32b,0x69,0x262)+'e'](from,buttonMessage,MessageType[_0x3f34aa(-0x57,0x7a0,0x431,0x234)+'sage'],_0x12701f),await limitAdd(sender,limit);break;case _0x1da9f3(0x454,0x705,0x816,0x392)+'tong':const _0x1d4df8={};_0x1d4df8[_0x1da9f3(0xb79,0xb7f,0xea2,0x10b3)+'t']=_0x1da9f3(0x695,0x2aa,0x667,0x1a7)+'t';const _0x4dd3b0={};_0x4dd3b0[_0x3f34aa(0x271,0x744,0x30f,0x519)]=_0x3f34aa(0xbfd,0x6a4,0x6d2,0x803),_0x4dd3b0[_0x1da9f3(0x8a3,0x54b,0x91f,0x9fc)]=_0x1d4df8,_0x4dd3b0[_0x1da9f3(0x847,0xca0,0xcc7,0x56a)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xdb6,0xbf1,0x6ee)](prefix),'©\x20'+ownername,[_0x4dd3b0]);const longtong=[_0x3f34aa(0xd62,0xc22,0x982,0xa97)+_0x3f34aa(0xa03,0xb54,0x82a,0x35b)+_0x1da9f3(0xb10,0x910,0xc05,0x783)+_0x3f34aa(0x6fc,-0x164,0x33a,0x73f)+_0x3f34aa(0x19b,0x1ed,0x36a,-0x90)+_0x3f34aa(0x796,0x5b2,0x9a1,0xa54)+_0x3f34aa(0xed,0x3a2,0x39f,0x263)+_0x3f34aa(0x36c,0x6e5,0x7c6,0xd21)+'lihatan',_0x3f34aa(0x2c0,0x811,0x7e4,0x5c9)+_0x3f34aa(0x49d,0x668,0x96b,0xe38)+_0x3f34aa(0x657,0x7de,0x5cf,0xa97)+_0x1da9f3(0x9f5,0x941,0x75b,0xac8)+_0x3f34aa(0x50b,0x409,0xd8,0x5c8)+'\x20adalah\x20se'+'telah\x20kopi'+'\x20itu\x20jadi\x20'+'dibuat',_0x3f34aa(0x7c1,0x976,0x4b2,0x3b0)+'eorang\x20pri'+_0x1da9f3(0xbf7,0x6f0,0xcb0,0xf88)+_0x1da9f3(0x6db,0x3d2,0x420,0x685)+_0x1da9f3(0x1bb,-0x53,0x4e7,0x4d3)+'nusia.\x20Tap'+_0x3f34aa(0xbb6,0x901,0x9f8,0x593)+_0x1da9f3(0x65f,0x552,0x818,0x7af)+_0x3f34aa(-0xb3,0x3c5,0x342,-0x1ae)+_0x3f34aa(-0x7c,0x242,0x1b7,-0x17d)+_0x3f34aa(0x105,0x8f9,0x552,0x79)+_0x1da9f3(0x302,-0x9c,0x59b,0x451)+_0x1da9f3(0x50e,0x9ee,0xba,0x4ad),_0x3f34aa(0xabf,0xc53,0x824,0x803)+_0x3f34aa(0x4ec,0x332,0x2af,0x3e6)+'\x20per\x20hari\x20'+_0x3f34aa(0x1ff,0x500,0x453,0x7ba)+'engan\x20memb'+'aca\x2030\x20men'+_0x1da9f3(0x9eb,0xaa2,0x98d,0xe6e)+'i',_0x1da9f3(0x49b,0x730,0x157,0x16f)+'jelek,\x20jan'+_0x3f34aa(-0x219,0x5a2,0x273,0x2f6)+_0x1da9f3(0x51f,0x765,0x26f,0x70b)+_0x1da9f3(0x2c2,0x76a,0x123,0x3a2)+_0x1da9f3(0xbdb,0x943,0xfff,0x7dd)+_0x1da9f3(0xba3,0x6df,0x10cf,0xdee)+_0x3f34aa(0x543,0x8ff,0x6c0,0x39e)+_0x3f34aa(0xa37,0xcb7,0xa63,0xac5)+_0x3f34aa(0x266,-0x110,0x40c,0x826),'Saya\x20tidak'+_0x1da9f3(0x8cf,0x6a9,0xd01,0x916)+'\x20orang\x20yan'+'g\x20sok\x20tau,'+'\x20banyak\x20ng'+'omong\x20tapi'+_0x1da9f3(0x6d5,0x3a6,0xac6,0x958)+_0x1da9f3(0xb6e,0xf50,0x7ff,0x102e)+_0x1da9f3(0x6d6,0x4e1,0x762,0xb46)+_0x1da9f3(0xa6f,0x9a8,0x538,0xc2d)+_0x3f34aa(0xb0e,0x4ce,0x79b,0x3d4)+'membenci\x20d'+'iri\x20saya\x20s'+_0x1da9f3(0x6d9,0x772,0x805,0xa56),_0x3f34aa(-0x2c9,-0x485,0x37,0x3e5)+_0x3f34aa(0x163,0x752,0x41c,0x8f6)+'kaki\x20lima\x20'+_0x3f34aa(0x38d,0x720,0x29e,-0x3b)+_0x1da9f3(0x906,0xe5f,0xc49,0x97a)+_0x1da9f3(0x900,0x3d8,0x8cc,0x727)+_0x3f34aa(0xdf5,0xb5b,0xa5a,0xa44)+_0x1da9f3(0x776,0x9de,0x53c,0x613)+_0x3f34aa(0x1ea,0x3c7,0x375,0x219)+_0x1da9f3(0x381,0xc0,0x404,0x859)+'\x20kakinya\x20h'+_0x3f34aa(0x436,0x210,0x332,0x373),_0x3f34aa(0x57d,0x6eb,0x652,0x749)+_0x1da9f3(0xb54,0xeac,0xb2c,0xa7d)+'anan,\x20tapi'+_0x1da9f3(0x22d,-0x263,-0x1f5,0x5c8)+_0x1da9f3(0x9b1,0xe9d,0xc0d,0x8bf)+_0x1da9f3(0x7ff,0xad0,0xc9d,0x955)+_0x3f34aa(0x9b2,0x648,0x744,0x975)+_0x1da9f3(0x3dd,0x79,0x84d,0x8da),'Jika\x20kamu\x20'+_0x1da9f3(0x83f,0x54f,0xb18,0x563)+_0x3f34aa(0x49c,0x62d,0x9ad,0x72f)+_0x1da9f3(0x342,0x296,0x312,-0x122)+_0x1da9f3(0xb5e,0x107f,0xbfd,0x956)+_0x3f34aa(0x34d,-0x45,0x245,0x6c2)+'lebih\x20baik'+'\x20kamu\x20putu'+_0x3f34aa(0x569,0x978,0x98f,0x7a1)+_0x1da9f3(0x576,0x23b,0xa39,0xa37)+_0x1da9f3(0x3af,0x7c3,0x636,0x1de),_0x3f34aa(0x407,0x669,0x8ca,0xc99)+'\x20takut\x20sam'+'a\x20istri\x20sa'+_0x1da9f3(0xa03,0xb38,0x73a,0xdc9)+_0x1da9f3(0xa08,0x5be,0x9c5,0x672)+_0x1da9f3(0xa39,0xd72,0xbcc,0x606)+'\x20di\x20dunia\x20'+_0x1da9f3(0xa97,0xa7a,0x8f4,0xd68)+_0x3f34aa(0x219,0x7f2,0x563,0x8f2)+_0x3f34aa(0x770,0x204,0x5a7,0x423)+_0x1da9f3(0xaeb,0xc4c,0xf0a,0xb13),_0x1da9f3(0xb4e,0xb4c,0xba6,0x834)+'gar\x20hidup\x20'+_0x1da9f3(0xbc8,0x1072,0x6cf,0xb4a)+_0x3f34aa(0x6d0,0x519,0x18d,-0x3c3)+_0x3f34aa(0xa30,0xc1d,0x9b2,0xe40)+_0x3f34aa(-0x33f,-0x2e,0xfe,-0x2f8)+_0x3f34aa(0x103,0x9c2,0x5e4,0x2ea)+_0x3f34aa(0x1de,0x508,0x5bf,0x981)+_0x3f34aa(0xaf7,0xc90,0x8ea,0xb69)+_0x1da9f3(0xa88,0xe17,0xbf1,0xcb3)+_0x1da9f3(0x787,0xc0c,0x2ca,0x916)+_0x3f34aa(0x123,0xb27,0x638,0x3a6)+_0x1da9f3(0x43d,0xde,0x503,0x345),_0x3f34aa(0x701,0x48,0x299,-0x1ae)+_0x1da9f3(0x9ff,0x5c1,0xc5f,0xe2c)+_0x1da9f3(0x36c,-0x17b,0x7a0,-0x32)+'yang\x20memba'+_0x3f34aa(0x2cd,0x221,0x395,0x64f)+_0x3f34aa(0x4b5,0x98d,0x8a6,0xbf9)+_0x3f34aa(0x54f,0x21c,0x3a4,0x7ba)+'ah\x20bahwa\x20y'+_0x1da9f3(0x5d8,0xd1,0x42d,0x495)+_0x3f34aa(0x572,-0x3b8,0x64,-0x4ba)+_0x1da9f3(0x6cb,0xbdc,0xa24,0x60d)+'\x20balasan\x20d'+_0x1da9f3(0x76a,0xaec,0xb8c,0x741),_0x1da9f3(0x4ba,0x1e2,0x187,0x968)+_0x3f34aa(0x5a6,0x44c,0x58c,0x8f)+_0x3f34aa(0xd9,0x351,0x271,0x324)+'t\x20itu\x20mene'+'ntukan\x20tem'+_0x1da9f3(0x50f,0x82b,0x5c1,0x64d)+_0x1da9f3(0xb60,0xed1,0x10b7,0xbb2),_0x3f34aa(0x2e,0x91e,0x417,0x6ad)+_0x3f34aa(0x6fb,0x321,0x77f,0x7ec)+_0x1da9f3(0xc4a,0xfdb,0x9f4,0xbd3)+_0x1da9f3(0x85e,0xd7a,0x32e,0xced)+_0x3f34aa(-0x229,0x789,0x2ef,-0x9d)+_0x1da9f3(0xa35,0x7a8,0x4e3,0x87c)+_0x3f34aa(0x301,0x57b,0x730,0x8f7)+_0x3f34aa(0x4ab,0xaac,0x735,0x5d5)+'da\x20uang\x20ya'+'ng\x20dihabis'+_0x3f34aa(0x7e4,0x2ca,0x4ba,0x58d)+'ya\x20setiap\x20'+'bulannya','Sebenarnya'+_0x1da9f3(0xa37,0x5bb,0x587,0xb11)+_0x3f34aa(0x23e,0xa37,0x793,0xb41)+_0x1da9f3(0xc05,0x96e,0xbd9,0xaed)+_0x3f34aa(0x502,0x4ca,0x363,0x62d)+'dar\x20tidak\x20'+_0x3f34aa(0x604,0xa0a,0x87c,0x9d4)+_0x3f34aa(0x2c3,0x78b,0x5d4,0x3a1)+'jomblo',_0x1da9f3(0x2d8,0x725,-0x134,0x342)+_0x3f34aa(0x15e,0x879,0x5a9,0xa27)+_0x3f34aa(0x70a,0xa47,0x839,0x429)+_0x1da9f3(0x3de,0x79c,0x714,0x4)+_0x3f34aa(0x473,0x909,0x7f3,0xad5)+_0x1da9f3(0x572,0x869,0x64d,0x472)+_0x1da9f3(0x680,0x807,0x61d,0x71a)+_0x1da9f3(0x27c,0x2ea,0x344,0xab)+',\x20tapi\x20seb'+'uah\x20negara'+_0x3f34aa(-0x1b2,0x175,0x30b,0x35f)+_0x1da9f3(0x57d,0x45,0x11e,0x306)+_0x3f34aa(0x543,0xabb,0x927,0xe41)+_0x3f34aa(0x441,0x261,0x4d3,0x408)+_0x3f34aa(0x4b5,0xca0,0x7f0,0x87a)+'\x20negara\x20te'+_0x3f34aa(-0x43a,0x31f,0x54,0x54c),_0x1da9f3(0x6fa,0x6d6,0xbfc,0x562)+'t\x20dipastik'+_0x1da9f3(0x980,0xaae,0x96d,0x5bb)+_0x3f34aa(0x4e6,0x38c,0x560,0x41c)+'sa\x20tidak\x20m'+_0x1da9f3(0x9bd,0xca3,0xb48,0x9e9)+_0x1da9f3(0x69a,0xa2b,0x6fd,0x30b)+_0x3f34aa(0x71a,0xc23,0x9db,0x523)+_0x1da9f3(0x793,0x8b8,0x540,0x4c6)+_0x1da9f3(0x739,0xb4c,0x95f,0x983)+'an\x20tidak\x20m'+'inum\x20pasti'+'\x20mereka\x20se'+_0x3f34aa(0x13a,0x3bb,0x262,0x749)+'asa','Putus\x20cint'+_0x1da9f3(0x671,0x553,0x9f3,0x5bb)+_0x3f34aa(0xb3b,0xc9e,0x9f3,0xecf)+_0x1da9f3(0x831,0x9ea,0x457,0xc9a)+'n.\x20Tapi,\x20b'+_0x1da9f3(0x7ae,0x63d,0x2d2,0x676)+_0x1da9f3(0x944,0x851,0xb48,0x9ed)+_0x3f34aa(0x553,0x40c,0x1fc,-0x1af),_0x3f34aa(0xafc,0x3cb,0x73b,0xc01)+'\x20takut\x20men'+_0x3f34aa(0xbab,0xa84,0x893,0x4bf)+'\x20miskin,\x20t'+_0x1da9f3(0x7e2,0xc0f,0x28e,0x35b)+_0x3f34aa(0xb97,0xbf9,0x755,0x4d9)+'ak\x20takut\x20m'+_0x3f34aa(0x4fc,-0xfc,0x1f9,0x62c)+_0x3f34aa(0xaff,0x706,0x6ed,0x7f8),_0x1da9f3(0x419,0x1db,-0x13e,0x15a)+'tidak\x20ada\x20'+_0x1da9f3(0x844,0xce9,0xd40,0xa1b)+_0x3f34aa(-0x2ce,-0x7c,0x101,-0x80)+_0x1da9f3(0x903,0x4b3,0x7ef,0xe12)+_0x1da9f3(0x2b1,0x62c,-0x2a0,-0x189)+'i,\x20sederha'+_0x1da9f3(0x663,0x912,0x631,0x30a)+_0x1da9f3(0x9d3,0x8cd,0x80d,0x55a)+_0x1da9f3(0x31c,-0x148,0xfe,0x6a9)+_0x1da9f3(0x460,0x771,0x603,0x933)],longtong_=longtong[Math[_0x1da9f3(0x580,0x528,0x1f5,0x9f1)](Math[_0x3f34aa(0x451,0x277,0x7a3,0xbc2)]()*longtong[_0x3f34aa(0x782,0xde8,0x92e,0x87a)])],_0x37d2d5={};_0x37d2d5['displayTex'+'t']=_0x1da9f3(0xac5,0x73f,0x635,0x6b5);const _0x36d313={};_0x36d313[_0x1da9f3(0x4c3,0x49c,0x61d,0x7c7)]=_0x1da9f3(0x2e1,-0xa7,0x1d3,0xec),_0x36d313['buttonText']=_0x37d2d5,_0x36d313[_0x1da9f3(0x847,0xafb,0x897,0x632)]=0x1;const _0x5f0e04={};_0x5f0e04['displayTex'+'t']=_0x1da9f3(0x935,0xb8e,0x488,0x5f4);const _0x297449={};_0x297449[_0x3f34aa(0x5b7,0x35,0x30f,0x17b)]=''+command,_0x297449[_0x1da9f3(0x8a3,0xa9d,0x89d,0x9e9)]=_0x5f0e04,_0x297449[_0x1da9f3(0x847,0x696,0x922,0x3ab)]=0x1;var buttonns=[_0x36d313,_0x297449];buttonMessage={'contentText':longtong_,'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+(_0x1da9f3(0x9fc,0x633,0x89a,0x874)+_0x3f34aa(-0x70,-0x39,0x3e5,0x14a)+_0x1da9f3(0x92f,0xd39,0xb37,0x6a9))+enter+_0x3f34aa(0x578,0xadd,0x918,0xa02)+sender[_0x3f34aa(0x5cb,0x335,0x7c7,0x4eb)]('@')[0x265f*-0x1+-0x1aa+0x2809],'buttons':buttonns,'headerType':0x1};const _0x5c92c5={};_0x5c92c5[_0x1da9f3(0x5e8,0x610,0x76d,0x9a9)]='hi',_0x5c92c5[_0x1da9f3(0x3eb,0x1ac,0x44a,0x759)+_0x3f34aa(0x914,0x6bc,0x5b3,0x495)]=0x3b9aca00,_0x5c92c5[_0x1da9f3(0x703,0xbb4,0x425,0x860)+'d']=!![],_0x5c92c5[_0x3f34aa(0x853,0xbe4,0x8a7,0x806)+'ral']=!![],_0x5c92c5['mentionedJ'+'id']=[sender];const _0x56f334={};_0x56f334[_0x3f34aa(0x724,0x151,0x2d0,0x11)]=_0x3f34aa(-0x488,-0x553,0x2,-0x411)+'21',_0x56f334[_0x3f34aa(0x13,-0xd8,0x1eb,0x300)+'o']=_0x5c92c5,_0x56f334['quoted']=ftoko,_0x56f334[_0x1da9f3(0xa5b,0x5db,0xc7b,0x7d9)+_0x1da9f3(0xb5a,0xe89,0x109d,0x991)]=!![],alpha[_0x1da9f3(0x21d,-0x11e,0x540,-0x12a)+'e'](from,buttonMessage,MessageType[_0x1da9f3(0x5e5,0x3a2,0x62a,0x835)+_0x1da9f3(0xa25,0x736,0xda2,0x6c9)],_0x56f334),await limitAdd(sender,limit);break;case _0x1da9f3(0x664,0xb83,0x239,0x870):case _0x3f34aa(0x9e0,0xb1f,0x922,0x642):case _0x1da9f3(0x71c,0x72c,0x250,0x6de):case _0x1da9f3(0xc2a,0xbe3,0xfa2,0xca6):case _0x1da9f3(0xc35,0x10a7,0xe2b,0x9d3):case _0x3f34aa(0x402,0x399,-0x6,-0x315):case _0x1da9f3(0x88f,0x3f2,0x672,0xb23):case _0x1da9f3(0x3a6,0x81d,0x71,0x94):case'inori':case _0x3f34aa(0x80c,0x661,0x510,0x336):case _0x3f34aa(0x5a7,0x335,0x12a,-0x179)+_0x1da9f3(0x5cc,0x826,0x4eb,0x2d4):case'kotori_min'+_0x1da9f3(0x7eb,0x48f,0xc30,0x83c):case'mikasa':case _0x1da9f3(0x55a,0x3a9,0x262,0x89b)+'a':case _0x1da9f3(0x5ef,0x78a,0x9c5,0x930)+_0x1da9f3(0x405,0xd2,-0x14c,0xf3):case'nakiri_ali'+'ce':case _0x1da9f3(0x534,0x791,0x4ca,0x89b):case'riyas_grem'+_0x1da9f3(0x6ad,0x337,0x433,0x454):case _0x1da9f3(0x6d4,0x33a,0x956,0xb54):case _0x1da9f3(0xa9a,0xe8d,0xfa1,0xaab):case _0x1da9f3(0x9b8,0xae2,0x870,0x9b8)+'u':case _0x1da9f3(0x634,0xa1a,0x7e3,0x8b5):case _0x1da9f3(0x876,0x38b,0xd13,0x338):case _0x1da9f3(0x2ee,0x813,0x338,0x488):case _0x3f34aa(0x184,0x103,0x507,0x382):case _0x1da9f3(0x4e4,0x6fb,0x965,0x629):case'yuzuki':case _0x1da9f3(0x2c9,0x2be,-0x24d,0x758):const _0x18e6e4={};_0x18e6e4[_0x1da9f3(0xb79,0x914,0x696,0x913)+'t']='Check\x20Limi'+'t';const _0x1371da={};_0x1371da[_0x3f34aa(-0x6b,0x721,0x30f,-0x5b)]=_0x1da9f3(0x886,0x596,0x57e,0x3e0),_0x1371da[_0x3f34aa(0x4bb,0x3dc,0x6ef,0x920)]=_0x18e6e4,_0x1371da[_0x1da9f3(0x847,0x4e5,0x691,0x3a9)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x1371da]);reply(lang[_0x1da9f3(0x723,0x8ae,0x5f8,0xaeb)]());var anime=_0x3f34aa(0x3d1,0x8b1,0x91f,0xd59)+_0x1da9f3(0x794,0x417,0x345,0x2b0)+_0x1da9f3(0x237,0x238,0x6ca,0x597)+_0x3f34aa(0x18b,-0xfd,0x264,0x245)+_0x3f34aa(0xd55,0xc5e,0xa60,0x8b4)+command+(_0x3f34aa(0x1cc,0x2a9,0x2c1,0x5c7)+_0x3f34aa(0x801,0xbb5,0xa31,0xc0c));let random_anime=await getBuffer(anime);const mediaxxxxxx=await alpha[_0x1da9f3(0xc58,0x8e7,0x10f8,0xad0)+_0x3f34aa(0x5da,0x699,0x871,0x956)](from,random_anime,MessageType[_0x3f34aa(0x253,0x27e,0x597,0x7a4)],{'thumbnail':Buffer[_0x1da9f3(0x8c2,0x45a,0xbf7,0x5ff)](0x31*-0x1+0x1*0x1d23+-0x1a*0x11d)});let bacotluxxxxxx=mediaxxxxxx[_0x3f34aa(0x3d8,0xcf5,0x7d8,0xc91)][_0x1da9f3(0x7ea,0x576,0xc44,0x474)+_0x1da9f3(0x9a1,0xd31,0x556,0x677)]?mediaxxxxxx[_0x1da9f3(0x98c,0xed3,0x508,0xdf8)][_0x3f34aa(0x8b0,0xb72,0x636,0x6df)+_0x1da9f3(0x9a1,0xca4,0x6fc,0x629)]:mediaxxxxxx;const _0x441493={};_0x441493[_0x1da9f3(0xb79,0xad9,0xa89,0x6af)+'t']='🐨\x20Owner';const _0x1f1aac={};_0x1f1aac['buttonId']='owner',_0x1f1aac[_0x1da9f3(0x8a3,0x839,0x512,0x606)]=_0x441493,_0x1f1aac['type']=0x1;const _0x4c0067={};_0x4c0067[_0x3f34aa(0x9da,0x7db,0x9c5,0x78e)+'t']='Donasi\x20💰';const _0xf4b79f={};_0xf4b79f['buttonId']=_0x1da9f3(0x23e,0x690,0x6ba,0x512),_0xf4b79f['buttonText']=_0x4c0067,_0xf4b79f[_0x1da9f3(0x847,0x4f0,0x646,0x8f3)]=0x1;const buttonsxxxxxx=[_0x1f1aac,_0xf4b79f],btnxxxxx__={'contentText':lang[_0x3f34aa(0x180,0x535,0x618,0x962)](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x1da9f3(0x2f3,0x47b,-0x25a,0x5cd)+_0x3f34aa(0x152,0xa94,0x5be,0xa7a))+sender[_0x1da9f3(0x97b,0x4e6,0x425,0x881)]('@')[0x2fa+0x1a03+0x1cfd*-0x1],'buttons':buttonsxxxxxx,'headerType':0x4,'imageMessage':bacotluxxxxxx[_0x1da9f3(0x98c,0x9ce,0xdaf,0x936)]['imageMessa'+'ge']},_0x12eb19={};_0x12eb19[_0x3f34aa(-0x88,-0x300,0x17b,0x1c)+'id']=[sender];const _0x352fd9={};_0x352fd9['quoted']=mek,_0x352fd9[_0x3f34aa(-0x21a,0x6ca,0x1eb,-0x1b9)+'o']=_0x12eb19,alpha[_0x3f34aa(0x1fb,-0x349,0x69,-0x365)+'e'](from,btnxxxxx__,MessageType[_0x1da9f3(0x5e5,0x4a4,0x5b6,0x5eb)+_0x1da9f3(0xa25,0x642,0xe72,0x82f)],_0x352fd9)[_0x3f34aa(0x405,0x26e,0x6df,0x661)](_0x403131=>{function _0x52099c(_0xd24fa0,_0x96aacc,_0x2a3547,_0x47ae4d){return _0x3f34aa(_0xd24fa0-0x16,_0xd24fa0,_0x96aacc- -0x1c6,_0x47ae4d-0x7e);}const _0x239614={'QSCYG':function(_0x2d9a17,_0x2f5de5){return _0x2d9a17(_0x2f5de5);}};function _0x5569b9(_0x2d1241,_0x575b96,_0x471c84,_0x39eacc){return _0x3f34aa(_0x2d1241-0x12c,_0x2d1241,_0x471c84-0x51d,_0x39eacc-0x138);}_0x239614[_0x5569b9(0xaff,0xdee,0xd92,0x1041)](reply,lang[_0x5569b9(0xbbf,0x78d,0xbd8,0xc13)]());}),await limitAdd(sender,limit);break;case'baekhyung':case _0x3f34aa(0x517,0xa0d,0x703,0x649)+'o':case _0x3f34aa(0x751,0xb5b,0x9fe,0x870):case _0x3f34aa(0x6af,0x74a,0x290,-0x1c):case _0x3f34aa(0xcb9,0x78d,0x8f7,0x4df):case _0x1da9f3(0x9ca,0x4e1,0x5b4,0xb09):case _0x3f34aa(0x14b,0x575,0x15e,-0x23c):case _0x1da9f3(0x8ce,0xa9a,0x790,0xb4a):case _0x1da9f3(0x8b7,0x579,0x88f,0x39a)+'o':case _0x3f34aa(0x76,0x396,0x5ac,0x278)+'n':case _0x3f34aa(0x26d,0x3ab,0x15f,0x22c):case _0x1da9f3(0x389,-0x149,0x329,0x615):case _0x3f34aa(-0x292,0x45b,0x4,-0x463):case _0x1da9f3(0xa01,0xcae,0x9b8,0x964)+'g':case _0x3f34aa(0xcdb,0xd36,0xa56,0x8b7):case _0x3f34aa(0x1f8,-0x213,0x18f,-0x316):case _0x3f34aa(0x42a,0x4b9,0x5a8,0x4c8)+'ol':case _0x1da9f3(0x8b6,0x5a5,0x40b,0x881):case _0x3f34aa(0x5f2,0x43d,0x79a,0x76e):const _0x540211={};_0x540211['displayTex'+'t']=_0x3f34aa(0xa3b,0x590,0x4e1,0x5de)+'t';const _0x5e50c1={};_0x5e50c1['buttonId']=_0x1da9f3(0x886,0x77e,0x754,0x87f),_0x5e50c1[_0x3f34aa(0x545,0x802,0x6ef,0x42d)]=_0x540211,_0x5e50c1['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x38a,0x9ba,0xadf)](prefix),'©\x20'+ownername,[_0x5e50c1]);reply(lang[_0x1da9f3(0x723,0x522,0x505,0xad1)]());var cogan=await fetchJson(_0x1da9f3(0xad3,0xbc8,0xd3a,0x58a)+_0x3f34aa(0x193,0x411,0x5e0,0x803)+_0x3f34aa(0x33c,0x36b,0x83,-0x3ec)+_0x1da9f3(0x699,0xad3,0x7fe,0x78f)+_0x3f34aa(0x772,0x441,0x23c,0x347)+command+('?apikey=of'+_0x1da9f3(0xbe5,0x927,0x77d,0xfa1)));let random_cogan=await getBuffer(cogan[_0x1da9f3(0x43f,0xc0,0x408,0x2de)]);const mediaxxxxx=await alpha[_0x1da9f3(0xc58,0xb42,0xf91,0xf0a)+_0x1da9f3(0xa25,0x999,0xd16,0xe49)](from,random_cogan,MessageType[_0x1da9f3(0x74b,0x431,0x67f,0x63d)],{'thumbnail':Buffer[_0x3f34aa(0x84b,0x41a,0x70e,0x2eb)](0x1bd2+0x1194+-0x2d66)});let bacotluxxxxx=mediaxxxxx[_0x3f34aa(0x4b0,0x600,0x7d8,0x9ac)][_0x3f34aa(0x870,0x638,0x636,0xf7)+_0x3f34aa(0x96f,0xaa4,0x7ed,0x447)]?mediaxxxxx[_0x1da9f3(0x98c,0x759,0xd76,0xb92)][_0x3f34aa(0x78d,0x6d1,0x636,0x73c)+_0x3f34aa(0x67a,0xacc,0x7ed,0x757)]:mediaxxxxx;const _0x558e81={};_0x558e81['displayTex'+'t']=_0x3f34aa(-0x227,0x611,0x231,0x41e);const _0x1e85a1={};_0x1e85a1[_0x1da9f3(0x4c3,0x673,0x82b,0x77b)]=_0x1da9f3(0x2e1,0x263,0x5e,0x1a5),_0x1e85a1[_0x1da9f3(0x8a3,0xa5b,0xb75,0xbae)]=_0x558e81,_0x1e85a1['type']=0x1;const _0x657e42={};_0x657e42[_0x1da9f3(0xb79,0xd6b,0xcca,0xe4c)+'t']='Donasi\x20💰';const _0x41dcaa={};_0x41dcaa['buttonId']=_0x1da9f3(0x23e,0x25b,0x31a,0x57e),_0x41dcaa[_0x3f34aa(0xb7c,0x3d0,0x6ef,0x766)]=_0x657e42,_0x41dcaa[_0x1da9f3(0x847,0x460,0x5b4,0x4c2)]=0x1;const buttonsxxxxx=[_0x1e85a1,_0x41dcaa],btnxxxx__={'contentText':lang[_0x3f34aa(0x427,0x824,0x618,0x6f5)](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+('\x20|\x20Request'+_0x1da9f3(0x772,0x7b0,0xb78,0x99a))+sender[_0x3f34aa(0xac5,0x2e2,0x7c7,0x296)]('@')[-0x1fec+-0x1*-0x1ad5+0x517],'buttons':buttonsxxxxx,'headerType':0x4,'imageMessage':bacotluxxxxx[_0x3f34aa(0xa40,0x50a,0x7d8,0x9e5)][_0x1da9f3(0x6fe,0x385,0x502,0x49e)+'ge']},_0x20938f={};_0x20938f[_0x3f34aa(-0x148,0x157,0x17b,-0x18e)+'id']=[sender];const _0x31296c={};_0x31296c[_0x1da9f3(0x96d,0x496,0xcc1,0xae7)]=mek,_0x31296c['contextInf'+'o']=_0x20938f,alpha[_0x3f34aa(-0x3d1,-0x38a,0x69,0x34)+'e'](from,btnxxxx__,MessageType[_0x1da9f3(0x5e5,0x82d,0xa15,0x1c5)+'sage'],_0x31296c)[_0x3f34aa(0x7de,0x845,0x6df,0x2b1)](_0x5180c0=>{function _0x357128(_0x243e9e,_0x246e52,_0x713bfc,_0x300873){return _0x3f34aa(_0x243e9e-0x59,_0x713bfc,_0x246e52-0x536,_0x300873-0x71);}reply(lang[_0x357128(0xf09,0xbf1,0x79f,0xfed)]());}),await limitAdd(sender,limit);break;case _0x3f34aa(0xe,-0x3d9,0x10f,-0x201):case'ass':case _0x1da9f3(0x7f7,0x2dd,0x807,0x371):case'cuckold':case _0x3f34aa(0x9ec,0xe4a,0x9f6,0x6b8):case'gangbang':case'glasses':case _0x3f34aa(0x160,0x666,0x6b4,0x46d):case _0x1da9f3(0x347,0x2bd,0x1bd,0x198):case _0x3f34aa(0x41f,0x5d5,0x631,0xeb)+'on':case'orgy':case _0x1da9f3(0x48f,0x5bf,0x384,0x447):case _0x3f34aa(0x5dd,0x8ac,0x692,0xb27):case _0x1da9f3(0x323,-0x3c,-0x12,0x7cf):case _0x1da9f3(0x94b,0x8e8,0x72c,0xa1b):const _0xc60579={};_0xc60579['displayTex'+'t']=_0x1da9f3(0x695,0x89c,0x2b5,0x181)+'t';const _0x18a296={};_0x18a296[_0x3f34aa(-0xd4,0x230,0x30f,0x1d5)]=_0x3f34aa(0x699,0x6df,0x6d2,0x5e7),_0x18a296['buttonText']=_0xc60579,_0x18a296[_0x3f34aa(0x5e5,0x3ec,0x693,0xa15)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xa50,0x578,0xc5b)](prefix),'©\x20'+ownername,[_0x18a296]);if(!isNsfw)return reply(lang[_0x3f34aa(0x2f1,0x4d1,0x324,-0x34)]());reply(lang['wait']());var random_nsfww=await getBuffer(_0x3f34aa(0x908,0xe4a,0x91f,0x88d)+_0x1da9f3(0x794,0x4dd,0x9b0,0xc0a)+'.herokuapp'+_0x3f34aa(0x7bd,0x143,0x319,0x31e)+_0x3f34aa(0x658,0x60c,0x1e2,0x55f)+command+(_0x3f34aa(0x3b4,0x78b,0x2c1,0x3d8)+_0x3f34aa(0xcff,0xe86,0xa31,0xdae)));const mediaxxxx=await alpha[_0x3f34aa(0x76b,0xaad,0xaa4,0xe39)+_0x3f34aa(0x735,0xc31,0x871,0x6f7)](from,random_nsfww,MessageType['image'],{'thumbnail':Buffer['alloc'](-0x2290*0x1+-0x16*0x137+0x3d4a)});let bacotluxxxx=mediaxxxx[_0x3f34aa(0x650,0x9f5,0x7d8,0xbd4)][_0x3f34aa(0xa9c,0x4a0,0x636,0xac3)+_0x3f34aa(0x406,0x5ae,0x7ed,0x3e4)]?mediaxxxx[_0x3f34aa(0xc58,0xb26,0x7d8,0xb77)][_0x3f34aa(0x80a,0x91d,0x636,0x1a1)+_0x3f34aa(0x47c,0x2a7,0x7ed,0x5a2)]:mediaxxxx;const _0x474910={};_0x474910[_0x1da9f3(0xb79,0xc9d,0xc82,0xbe7)+'t']='🐨\x20Owner';const _0x2ddfbd={};_0x2ddfbd['buttonId']=_0x1da9f3(0x2e1,-0xe1,0x2f8,0x633),_0x2ddfbd['buttonText']=_0x474910,_0x2ddfbd[_0x3f34aa(0x706,0x496,0x693,0x7b6)]=0x1;const _0x22a234={};_0x22a234[_0x3f34aa(0xc32,0xdd5,0x9c5,0xb53)+'t']=_0x3f34aa(0x865,0xcdc,0x90e,0x40c);const _0x359a20={};_0x359a20[_0x1da9f3(0x4c3,0x30c,0x7fe,0x76a)]=_0x1da9f3(0x23e,0x153,0x72,0x19c),_0x359a20['buttonText']=_0x22a234,_0x359a20[_0x3f34aa(0x638,0x42b,0x693,0x81b)]=0x1;const buttonsxxxx=[_0x2ddfbd,_0x359a20],btnxxx__={'contentText':lang[_0x1da9f3(0x7cc,0x70b,0x948,0x52b)](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x3f34aa(-0xc5,-0x3de,0x13f,-0x35e)+_0x1da9f3(0x772,0x75f,0xc28,0x717))+sender[_0x3f34aa(0xb97,0xae1,0x7c7,0x834)]('@')[0x2165+-0x6b2*-0x2+-0x2ec9],'buttons':buttonsxxxx,'headerType':0x4,'imageMessage':bacotluxxxx[_0x3f34aa(0x618,0x71c,0x7d8,0xc51)][_0x1da9f3(0x6fe,0x3ab,0x63a,0x571)+'ge']},_0x4b9288={};_0x4b9288[_0x1da9f3(0x32f,0x6b3,0x611,0x1d7)+'id']=[sender];const _0x1b7c70={};_0x1b7c70[_0x1da9f3(0x96d,0xb2a,0x8ac,0x793)]=mek,_0x1b7c70[_0x3f34aa(0x679,-0xb,0x1eb,0x3a3)+'o']=_0x4b9288,alpha['sendMessag'+'e'](from,btnxxx__,MessageType[_0x1da9f3(0x5e5,0x693,0x210,0x915)+'sage'],_0x1b7c70)[_0x1da9f3(0x893,0xbc1,0x76d,0x832)](_0x130adc=>{function _0x4417de(_0x1d3893,_0x1a4e90,_0x5e3cf9,_0xbc7f08){return _0x3f34aa(_0x1d3893-0x181,_0x1a4e90,_0xbc7f08- -0x15c,_0xbc7f08-0x178);}const _0x3129a7={'TENza':function(_0x579838,_0x145378){return _0x579838(_0x145378);}};function _0x55482b(_0x188689,_0x14206c,_0x5247cf,_0x4b8561){return _0x1da9f3(_0x188689-0x343,_0x4b8561,_0x5247cf-0xf5,_0x4b8561-0xfd);}_0x3129a7[_0x4417de(0x8ec,0x4a8,0xc49,0x82f)](reply,lang[_0x4417de(0xa61,0x147,0x52a,0x55f)]());}),await limitAdd(sender,limit);break;case _0x1da9f3(0xa09,0xc14,0xf01,0xbfb):case _0x3f34aa(0xb40,0x83b,0x8fa,0x671):case _0x3f34aa(0x699,0x5ee,0xa42,0xf00):case _0x1da9f3(0x6c5,0xc0d,0x6c8,0xadb):case'slap':case _0x3f34aa(0xee,0x33c,0x49a,0x99e):case _0x1da9f3(0x6a5,0xa98,0x760,0x784):case'bj':case'pwankg':case _0x3f34aa(0xcee,0xc0d,0xa1f,0xe9f):case'poke':case _0x1da9f3(0x71e,0xb3f,0x2cc,0xb08):case'nsfw_neko_'+_0x1da9f3(0x9fb,0xd4a,0x699,0xa28):case _0x1da9f3(0x9b7,0xdc8,0xb54,0xf0d):case _0x1da9f3(0x88b,0xdbb,0x414,0x877):case _0x3f34aa(0x430,-0x177,0x266,0x461):case _0x3f34aa(0x3a6,0x135,0x48c,-0xac):case _0x1da9f3(0xb17,0xa85,0x8b2,0x977):case'tickle':case _0x1da9f3(0x456,0x2e5,-0xcd,0x97b):case _0x1da9f3(0x401,0x3d8,-0x3,0x78a):case _0x3f34aa(0x5c5,0x732,0x2c2,0x68d):case'boobs':case'anal':case _0x3f34aa(-0xfb,-0x131,0x17f,-0x30d):const _0x52fdf4={};_0x52fdf4[_0x1da9f3(0xb79,0xa23,0x9a6,0x7cb)+'t']=_0x3f34aa(0x63,0x89b,0x4e1,0x4ac)+'t';const _0x27eeb0={};_0x27eeb0['buttonId']='limit',_0x27eeb0[_0x3f34aa(0x3b9,0x310,0x6ef,0xb20)]=_0x52fdf4,_0x27eeb0[_0x3f34aa(0x2c9,0x237,0x693,0x5c7)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x438,0x3f3,0x6d2,0x654)](prefix),'©\x20'+ownername,[_0x27eeb0]);if(!isNsfw&&!mek[_0x1da9f3(0xc3d,0x8a9,0xe9c,0x828)][_0x3f34aa(0x748,0x417,0x5ff,0x758)]&&!isOwner&&!isCreator)return reply(lang[_0x1da9f3(0x4d8,0x2d,0x9b6,0x1ff)]());var ini_gif=await fetchJson(_0x3f34aa(0x3ca,0x40a,0x670,0x879)+_0x3f34aa(0x606,0xd8b,0x97e,0x5c2)+_0x1da9f3(0xa28,0xd6f,0xf33,0x89c)+command),ini_gif_=await getBuffer(ini_gif['url']);const mediax_=await alpha[_0x1da9f3(0xc58,0x113a,0x70d,0xe2f)+_0x1da9f3(0xa25,0xd0b,0x7b3,0xb85)](from,ini_gif_,MessageType[_0x3f34aa(0x5eb,0x60b,0x856,0x42b)],{'thumbnail':Buffer[_0x1da9f3(0x8c2,0x87d,0xe17,0x528)](-0x7*-0x2f1+0x1a8*-0x4+-0xdf7)});let bacotlux_=mediax_['message']['ephemeralM'+_0x1da9f3(0x9a1,0x567,0xd2f,0x737)]?mediax_[_0x1da9f3(0x98c,0x567,0xc53,0x957)][_0x3f34aa(0xfc,0x83c,0x636,0xa83)+_0x3f34aa(0xbf0,0x9bf,0x7ed,0x3b9)]:mediax_;const _0x17c7c9={};_0x17c7c9[_0x3f34aa(0x9a6,0xdc7,0x9c5,0xbba)+'t']=_0x1da9f3(0x3e5,0x70e,0x78c,0x62c);const _0x2de2a7={};_0x2de2a7[_0x3f34aa(0x35e,-0xdb,0x30f,0x2a1)]=_0x3f34aa(0x52c,-0x17a,0x12d,0x253),_0x2de2a7[_0x3f34aa(0x6f8,0x801,0x6ef,0x3e9)]=_0x17c7c9,_0x2de2a7[_0x3f34aa(0xb6c,0x43b,0x693,0x66e)]=0x1;const _0x4baa54={};_0x4baa54[_0x1da9f3(0xb79,0xb7f,0x95a,0xdc7)+'t']=_0x1da9f3(0xa56,0x588,0x814,0x62f);const _0x3d463b={};_0x3d463b[_0x3f34aa(0x3b5,0x31,0x30f,0x2d7)]='sewabot',_0x3d463b[_0x3f34aa(0x604,0x9e2,0x6ef,0x58c)]=_0x4baa54,_0x3d463b[_0x1da9f3(0x847,0x694,0x991,0x62e)]=0x1;const buttonsx_=[_0x2de2a7,_0x3d463b],btnx_={'contentText':lang[_0x1da9f3(0x7cc,0xba7,0xc84,0xa6b)](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x3f34aa(0x115,-0x2f0,0x13f,0x44c)+_0x3f34aa(0x755,0x3d1,0x5be,0xa8f))+sender['split']('@')[-0x1*0x1d7+-0x2589+0x2760*0x1],'buttons':buttonsx_,'headerType':0x5,'videoMessage':bacotlux_[_0x1da9f3(0x98c,0x695,0xbf8,0xd5d)][_0x1da9f3(0x329,0x4f5,0x7ca,0x5a0)+'ge']},_0x2cda63={};_0x2cda63[_0x1da9f3(0x32f,-0x12c,0x47,0x584)+'id']=[sender];const _0x461c93={};_0x461c93[_0x1da9f3(0x96d,0xbac,0xce8,0x4ba)]=mek,_0x461c93[_0x1da9f3(0x39f,-0xf4,0x4,0x60d)+'o']=_0x2cda63,alpha[_0x1da9f3(0x21d,-0x175,-0x40,0x1dc)+'e'](from,btnx_,MessageType['buttonsMes'+'sage'],_0x461c93)[_0x1da9f3(0x893,0xb2d,0x348,0x61d)](_0x2311b2=>{function _0x594bd7(_0x246550,_0x42443c,_0x4b068d,_0x3aef8e){return _0x3f34aa(_0x246550-0xa,_0x3aef8e,_0x42443c-0x1e,_0x3aef8e-0x37);}function _0x13f211(_0x5344ac,_0x5ae479,_0x1af2c1,_0x375372){return _0x3f34aa(_0x5344ac-0x27,_0x5344ac,_0x1af2c1-0x115,_0x375372-0x8);}const _0xc31445={'EdsnR':function(_0x2b763a,_0x55de67){return _0x2b763a(_0x55de67);}};_0xc31445[_0x13f211(0xa33,0x441,0x8d3,0x5d2)](reply,lang[_0x594bd7(0x2d9,0x6d9,0xa7f,0x891)]());});break;case'feet':case _0x3f34aa(0x60,0x193,0x56,-0x120):case _0x1da9f3(0xb15,0xa6d,0x1060,0x1032):case'gasm':case'solo':case _0x1da9f3(0x8b5,0xd76,0x97c,0x6c8):case'goose':case _0x3f34aa(0x77d,0x66a,0x5fc,0x8bf):case _0x3f34aa(0xb9,0x9b0,0x488,0x91b):case _0x3f34aa(0x353,0x515,0x246,0x6c):case'gecg':case _0x3f34aa(0x2fd,0x647,0x73d,0x59e):case _0x3f34aa(0xea,0x6eb,0x1e8,0x4a9):case _0x3f34aa(0x533,0xabd,0x6cc,0x695):case _0x1da9f3(0x2c1,0x532,0x3e3,-0xbb):case _0x1da9f3(0xa3e,0xaf8,0xcc5,0xce9):case'lizard':case _0x3f34aa(-0x4a7,-0x29d,0x47,0x21c):case _0x1da9f3(0x7c6,0x59f,0xbb7,0x3a6):case _0x1da9f3(0x67d,0x85e,0x8cf,0x180):case _0x3f34aa(0xc45,0x5b8,0x857,0x6cb):case _0x3f34aa(0x83e,0x610,0xa1c,0xd16):case'erok':case _0x1da9f3(0x936,0x518,0x8ef,0xcf7):case'cum_jpg':case _0x1da9f3(0x1bc,0x577,-0x2c1,0x4e0)+'r':case _0x1da9f3(0x99b,0x976,0xd6f,0xd19):case _0x3f34aa(0x26e,0x6e7,0x791,0xa57):case _0x3f34aa(0x28,-0x6b,0x3ba,0x7ea):case _0x1da9f3(0x1f6,0x6e,-0x356,-0x5c):case _0x3f34aa(0xb0c,0x5c8,0xaac,0x869):case _0x1da9f3(0x781,0x9f1,0xb5a,0xa56):case _0x1da9f3(0x3c4,0x103,0x2e8,0x300):case _0x1da9f3(0x74a,0x4e2,0x91a,0x85a):case _0x1da9f3(0x8cd,0xc90,0xb7b,0x863):case'futanari':case _0x3f34aa(0x47f,0x7d6,0x523,0x75b):const _0x378244={};_0x378244['displayTex'+'t']='Check\x20Limi'+'t';const _0x51bdbd={};_0x51bdbd[_0x1da9f3(0x4c3,0x6a2,0xd5,0x29c)]='limit',_0x51bdbd[_0x3f34aa(0x9f4,0x8fe,0x6ef,0x8e5)]=_0x378244,_0x51bdbd['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x51bdbd]);if(!isNsfw&&!mek[_0x1da9f3(0xc3d,0xe0d,0x8d8,0x822)][_0x1da9f3(0x7b3,0x538,0xadf,0x7c5)]&&!isOwner&&!isCreator)return reply(lang[_0x3f34aa(0x3ae,-0xcc,0x324,0x469)]());var ini_img=await fetchJson('https://ne'+_0x1da9f3(0xb32,0xfa7,0xab1,0xd40)+'pi/v2/img/'+command),ini_img_=await getBuffer(ini_img[_0x1da9f3(0x4ad,0x78a,0x8b3,0x387)]);const mediax=await alpha['prepareMes'+_0x1da9f3(0xa25,0xa75,0xcda,0xf54)](from,ini_img_,MessageType[_0x1da9f3(0x74b,0x98b,0x5a9,0x26b)],{'thumbnail':Buffer[_0x1da9f3(0x8c2,0x421,0x375,0xb22)](-0x2275*0x1+-0x1b3c+-0x3a1*-0x11)});let bacotlux=mediax[_0x1da9f3(0x98c,0xba5,0xa45,0xbe9)][_0x1da9f3(0x7ea,0x72b,0x7fd,0xaec)+_0x1da9f3(0x9a1,0x897,0x7db,0x4a6)]?mediax['message'][_0x3f34aa(0xaa2,0x19a,0x636,0xb44)+_0x3f34aa(0xacd,0x712,0x7ed,0x57f)]:mediax;const _0x11f2e7={};_0x11f2e7['displayTex'+'t']='🐨\x20Owner';const _0x2c3188={};_0x2c3188[_0x3f34aa(0x7f6,0x682,0x30f,0x7a3)]=_0x1da9f3(0x2e1,0x6af,0x39c,0x4dd),_0x2c3188[_0x1da9f3(0x8a3,0x892,0x4d1,0x93a)]=_0x11f2e7,_0x2c3188['type']=0x1;const _0x3a8d60={};_0x3a8d60[_0x3f34aa(0xa3a,0x4fa,0x9c5,0xc8c)+'t']='Donasi\x20💰';const _0x463248={};_0x463248['buttonId']='donasi',_0x463248[_0x3f34aa(0xc29,0x2c3,0x6ef,0x292)]=_0x3a8d60,_0x463248['type']=0x1;const buttonsx=[_0x2c3188,_0x463248],btnx__={'contentText':lang['success'](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x1da9f3(0x2f3,-0x39,0x3dc,0x297)+'\x20by\x20@')+sender[_0x1da9f3(0x97b,0xa9d,0x4c9,0x9c0)]('@')[-0x459+0x1*0x22a9+-0x10*0x1e5],'buttons':buttonsx,'headerType':0x4,'imageMessage':bacotlux[_0x3f34aa(0xc95,0x3e2,0x7d8,0x9c0)][_0x3f34aa(0x63d,0x4a1,0x54a,0x467)+'ge']},_0x2023a9={};_0x2023a9['mentionedJ'+'id']=[sender];const _0x55fd07={};_0x55fd07[_0x1da9f3(0x96d,0xa19,0xdb5,0xeb7)]=mek,_0x55fd07[_0x1da9f3(0x39f,0x596,0x7ad,0x2be)+'o']=_0x2023a9,alpha[_0x3f34aa(-0x33,-0x3ae,0x69,-0x5e)+'e'](from,btnx__,MessageType['buttonsMes'+'sage'],_0x55fd07)[_0x3f34aa(0x6cc,0xad4,0x6df,0x80c)](_0x23dec7=>{function _0x40da6b(_0x499df7,_0x36a4e9,_0x1483c5,_0x173040){return _0x1da9f3(_0x1483c5- -0xf1,_0x36a4e9,_0x1483c5-0x1ef,_0x173040-0x84);}const _0x233834={'zkNnQ':function(_0x305db4,_0x429993){return _0x305db4(_0x429993);}};function _0x25ac5d(_0x144c62,_0x178842,_0x16b5e3,_0x45a320){return _0x3f34aa(_0x144c62-0x5,_0x144c62,_0x45a320- -0x17d,_0x45a320-0x1b8);}_0x233834[_0x25ac5d(0xa34,0x128,0xad1,0x577)](reply,lang[_0x25ac5d(0x25c,0x845,0x3cb,0x53e)]());}),await limitAdd(sender,limit);break;case _0x1da9f3(0x851,0xca6,0x4e6,0x85c):const _0x3f7126={};_0x3f7126[_0x3f34aa(0xd87,0xab4,0x9c5,0x918)+'t']='Check\x20Limi'+'t';const _0x2569a1={};_0x2569a1[_0x3f34aa(0x6fb,0x46d,0x30f,-0x20)]='limit',_0x2569a1[_0x3f34aa(0x7ab,0x54b,0x6ef,0x253)]=_0x3f7126,_0x2569a1[_0x3f34aa(0xa76,0xa51,0x693,0x9ad)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xbd5,0xc07,0x70f)](prefix),'©\x20'+ownername,[_0x2569a1]);const pref=_0x3f34aa(0x266,0x1c0,0x6e6,0x90d)+enter+('\x20amongus\x20u'+_0x3f34aa(0x8fe,0x704,0x3e0,0x535)+_0x3f34aa(0x847,0x496,0x6e7,0x64e)+'rue\x20/\x20not\x20'+_0x1da9f3(0x4ec,-0x2,0x3ec,-0x19)+_0x1da9f3(0x4f4,0x58f,0x972,0x4c1)+'lor\x20=\x20blac'+'k|blue|bro'+_0x1da9f3(0x8e8,0x692,0x967,0x4fb)+_0x3f34aa(0xb77,0xdd2,0x88c,0xba3)+'me|orange|'+_0x1da9f3(0x704,0xaa3,0x92a,0x893)+_0x3f34aa(0x9ab,0x9e6,0x830,0x9fa)+_0x3f34aa(-0x1d,0x8ae,0x4a7,0x69f))+enter+enter+_0x1da9f3(0x4eb,0x6f7,0x9ff,0x506)+enter+'\x20'+prefix+(_0x3f34aa(0x20d,0x54b,0x1a0,-0x1bc)+'ontoh|true'+'|cyan');if(args['length']<-0x2e*0xb9+-0x16dc+0x381b)return reply(pref);var kntl_=args[_0x3f34aa(0x49,0x31,0x318,0x744)]('\x20'),nama_=kntl_[_0x1da9f3(0x97b,0xcc1,0x620,0x538)]('|')[0x10*0x124+-0x3a*0x7a+0x1*0x964],impostor_=kntl_['split']('|')[-0x133*-0xf+0x23d9+0x1*-0x35d5],color_=kntl_['split']('|')[-0x1*-0x100d+-0x22fb+0x12f0];const _0x372709={};_0x372709[_0x1da9f3(0x86b,0x7be,0xc2e,0xb41)]='get';var amongus=await getBuffer(_0x1da9f3(0x84d,0x758,0x9c7,0xa52)+_0x1da9f3(0x214,0x54e,0x3cf,0x55c)+_0x3f34aa(0x55b,0x92c,0x972,0x4d1)+'d?name='+nama_+_0x3f34aa(0x55c,-0xbc,0x139,0x5b3)+impostor_+_0x3f34aa(0x93d,0x60d,0x5e7,0x1d9)+color_,_0x372709);amongusp=''+lang[_0x1da9f3(0x7cc,0xb96,0x9d0,0x5cf)]()+enter+enter+(_0x1da9f3(0x5a2,0x5d7,0x208,0x3c3)+'scribe\x20htt'+_0x1da9f3(0x481,0x949,0x212,0x91b)+'.be/w4iQ4r'+_0x1da9f3(0x1dd,0x584,0x425,-0x22a)),alpha[_0x1da9f3(0x21d,0x751,0x425,0x3b5)+'e'](from,amongus,image,{'thumbnail':Buffer[_0x3f34aa(0x332,0xb57,0x70e,0x77c)](-0x5d4+0x5*0x595+-0x1615),'caption':lang[_0x1da9f3(0x7cc,0xaf4,0x887,0x405)](),'quoted':mek}),await limitAdd(sender,limit);break;case _0x3f34aa(0x76,0x129,0xb9,-0x3b9):case _0x1da9f3(0x54c,0x7cf,0x95b,0x439):case'tweet':if(args[_0x1da9f3(0xae2,0x985,0xdad,0xbac)]<-0x2*0xfb3+0x10*0xff+0xf77*0x1)return reply('..');const _0x53de59={};_0x53de59[_0x1da9f3(0xb79,0xf8d,0xe8d,0x985)+'t']=_0x1da9f3(0x695,0x3fc,0x3ad,0x661)+'t';const _0x225e70={};_0x225e70[_0x3f34aa(0x639,0x22,0x30f,0x56c)]=_0x3f34aa(0x93d,0x5d6,0x6d2,0x6c9),_0x225e70[_0x3f34aa(0x21a,0x397,0x6ef,0x5c4)]=_0x53de59,_0x225e70[_0x1da9f3(0x847,0x626,0xb30,0x951)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xd86,0x4d2,0x983)](prefix),'©\x20'+ownername,[_0x225e70]);let trump=args[_0x3f34aa(0x436,0x328,0x318,0x540)]('\x20');const _0x496867={};_0x496867[_0x3f34aa(0x868,0x5d6,0x6b7,0x984)]=_0x3f34aa(0x2ee,0x1c0,0x31b,0x456);let trump1=await fetchJson(_0x3f34aa(0x789,0x4fd,0x670,0x222)+_0x3f34aa(0x6e6,0x5a1,0x2f3,-0x1a7)+_0x1da9f3(0x5fe,0xab4,0xf0,0x79b)+_0x3f34aa(0x4c0,0x79f,0x8f5,0xa12)+'umptweet&t'+_0x3f34aa(0x91,-0x83,0x1c1,0x2dc)+trump+_0x1da9f3(0xbb8,0xefb,0x931,0xf84),_0x496867),trump2=await getBuffer(trump1[_0x1da9f3(0x98c,0xc0a,0xcf1,0xe9d)]);alpha[_0x1da9f3(0x21d,-0x4e,0x5d4,0x24)+'e'](from,trump2,image,{'thumbnail':Buffer[_0x3f34aa(0x39d,0x7e4,0x70e,0x21b)](0x241f+-0x1633*0x1+-0xdec),'caption':lang[_0x3f34aa(0x8d7,0x290,0x618,0xa7d)](),'quoted':mek}),await limitAdd(sender,limit);break;case _0x3f34aa(0x453,0x245,0xee,-0xa6):if(args[_0x1da9f3(0xae2,0xf06,0xdf0,0x8e0)]<0xf4*-0xc+-0x102c*0x2+-0x3fb*-0xb)return reply('..');const _0x4bbb15={};_0x4bbb15[_0x3f34aa(0x82b,0xdfb,0x9c5,0xd4d)+'t']=_0x3f34aa(0x37e,0x613,0x4e1,0x96d)+'t';const _0x5c4896={};_0x5c4896[_0x1da9f3(0x4c3,0x9c5,0x9e8,-0x33)]=_0x3f34aa(0x382,0x7ca,0x6d2,0x47d),_0x5c4896['buttonText']=_0x4bbb15,_0x5c4896[_0x3f34aa(0x47b,0xb49,0x693,0xb90)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x5c4896]);let cmm=args[_0x3f34aa(0x45d,0x4df,0x318,-0x205)]('\x20');const _0x640506={};_0x640506[_0x1da9f3(0x86b,0xcb9,0x8b5,0x39a)]='get';let anuuu=await fetchJson('https://ne'+_0x3f34aa(0x6f9,0x4d8,0x2f3,0x2bd)+_0x1da9f3(0x5fe,0xa1f,0x55d,0xca)+_0x3f34aa(0x4ca,-0x1c,-0x4,-0x122)+_0x3f34aa(0x545,0xc57,0x7ca,0x29c)+_0x3f34aa(0xa15,0xb0e,0xa41,0x9f0)+cmm+_0x1da9f3(0xbb8,0xb99,0xc4f,0x7b1),_0x640506),bbuffer=await getBuffer(anuuu['message']);alpha[_0x1da9f3(0x21d,0x13b,0x2ef,0x6dc)+'e'](from,bbuffer,image,{'thumbnail':Buffer[_0x3f34aa(0xa0a,0x950,0x70e,0x810)](0x1f8e*-0x1+0xa8+0x1ee6),'caption':lang[_0x3f34aa(0x12a,0x775,0x618,0xa21)](),'quoted':mek}),await limitAdd(sender,limit);break;case'kanna':if(args['length']<0x6e*-0x26+0xdc8+-0x28d*-0x1)return reply('..');let kanna=args[_0x1da9f3(0x4cc,0x99d,0x99d,-0x1e)]('\x20');const _0x2b90de={};_0x2b90de['method']=_0x1da9f3(0x4cf,0x4db,0x8bf,0x87d);var anu_=await fetchJson(_0x3f34aa(0x79f,0x396,0x670,0x3d3)+_0x1da9f3(0x4a7,0x1f2,0x625,0x81c)+_0x1da9f3(0x5fe,0x370,0x7de,0x3d9)+'en?type=ka'+_0x3f34aa(0x5d0,0x8f3,0x3c0,0x32b)+'t='+kanna+'&raw=7',_0x2b90de);let buffer_h=await getBuffer(anu_[_0x1da9f3(0x98c,0x9be,0x8b0,0x719)]);alpha['sendMessag'+'e'](from,buffer_h,image,{'thumbnail':Buffer[_0x1da9f3(0x8c2,0xcb8,0x51e,0x9a4)](0x2218+-0x1349+-0x11*0xdf),'caption':lang['success'](),'quoted':mek}),await limitAdd(sender,limit);break;case _0x3f34aa(0x2a3,0x6a,0x11d,0x2ea):case _0x1da9f3(0x81c,0x353,0x355,0xd59):case _0x3f34aa(0xb68,0x6fe,0xa70,0xb50):case _0x1da9f3(0x33c,-0xe8,0x6d5,0x63c):case _0x1da9f3(0x6f0,0xc1f,0x1d9,0x849)+'g':case _0x1da9f3(0x961,0x97a,0x971,0x782):case _0x1da9f3(0x7d6,0x74d,0x2a4,0x6cb):case _0x3f34aa(0x4a4,0x86d,0x884,0xd19):case _0x1da9f3(0x5ab,0x5f3,0x505,0x76):case _0x1da9f3(0x2e0,0x60,-0x124,0x42):case _0x1da9f3(0x819,0x421,0x7e3,0xc66):case _0x3f34aa(0x395,0x93b,0x72d,0x7eb):case _0x1da9f3(0x7d8,0x4ab,0x543,0x9c2)+'di':case _0x1da9f3(0xb18,0x892,0xf43,0x8c4):case _0x3f34aa(0xb83,0x288,0x791,0x690):case'nicholas':case _0x1da9f3(0x8ff,0xa57,0x40e,0xb10):case _0x3f34aa(-0x1f5,0x5a,0x223,0x579):case _0x3f34aa(-0x93,0x756,0x3f3,0x338):case _0x1da9f3(0x7a7,0x47a,0x5a3,0x55d)+_0x1da9f3(0x948,0x995,0x8f2,0xac2):case _0x3f34aa(0x8ea,0x78f,0x69b,0x4d8):const _0x2c6bc6={};_0x2c6bc6['displayTex'+'t']=_0x3f34aa(0x940,0x22a,0x4e1,0x61f)+'t';const _0x55f88a={};_0x55f88a[_0x1da9f3(0x4c3,0x5a,0x486,0x3e0)]=_0x3f34aa(0xc15,0x23a,0x6d2,0xaca),_0x55f88a[_0x3f34aa(0xbce,0xb51,0x6ef,0x8da)]=_0x2c6bc6,_0x55f88a[_0x3f34aa(0x4e8,0x43b,0x693,0xaa3)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xb78,0x32f,0xc2a)](prefix),'©\x20'+ownername,[_0x55f88a]);reply(lang[_0x1da9f3(0x723,0x2e1,0x579,0x430)]());var random_telestc=await getBuffer(_0x1da9f3(0xad3,0x97e,0xfab,0xc97)+_0x3f34aa(0x7bb,0x70a,0x5e0,0x360)+_0x1da9f3(0x237,-0x22,0x558,-0xc1)+_0x3f34aa(0x58c,0x4e6,0xae,-0x3e2)+_0x1da9f3(0x87c,0x4b6,0x8bd,0x3ef)+'r/'+command+(_0x3f34aa(0x37,0x7bd,0x2c1,0x57a)+_0x1da9f3(0xbe5,0x884,0x811,0xf8e)));const _0x38c7cd={};_0x38c7cd[_0x1da9f3(0x96d,0x526,0xd99,0x9cc)]=fgclink,await alpha[_0x1da9f3(0x21d,0x28a,0x57d,-0x17f)+'e'](from,random_telestc,sticker,_0x38c7cd),await limitAdd(sender,limit);break;case _0x3f34aa(0xba5,0x565,0x819,0x859):case _0x1da9f3(0xc11,0xeca,0xa3e,0xc9d):case _0x3f34aa(0xe5e,0x83a,0x9f1,0xf46):case _0x1da9f3(0x2b0,0x7ac,0x2bd,0x539):case _0x1da9f3(0x376,0x41,0xb7,0x849):case _0x1da9f3(0x40c,0x6f0,0x7eb,-0x11d):case _0x1da9f3(0xbf3,0xdb1,0xdd9,0x101f):case _0x3f34aa(0xa9c,0x6d9,0xa83,0xbaf):case _0x1da9f3(0x3fe,0x20c,0x489,0x412):case'syania':case'riri':case _0x1da9f3(0xb11,0x743,0x828,0xc1b):case _0x3f34aa(0x53a,0x546,0xb7,0x5f8):case _0x3f34aa(0x9db,0x3ed,0x4d9,0x2f7):case _0x3f34aa(0xd32,0xcee,0x9e9,0xce3):case'rikagusria'+'ni':case'asupan':case _0x3f34aa(0x376,0x350,0x4e7,0x2b):case _0x3f34aa(0x52d,0x6dd,0x7fa,0x99c):case'santuy':case _0x1da9f3(0x5b5,0x5a,0x947,0x63a):const _0x3f33f9={};_0x3f33f9['displayTex'+'t']='Check\x20Limi'+'t';const _0x336768={};_0x336768['buttonId']='limit',_0x336768['buttonText']=_0x3f33f9,_0x336768[_0x1da9f3(0x847,0x58b,0x9c9,0xba7)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x485,0xba7,0x6d2,0x8f9)](prefix),'©\x20'+ownername,[_0x336768]);reply(lang['wait']());var random_asupan=await getBuffer(_0x3f34aa(0x978,0xd78,0x91f,0xb6a)+_0x3f34aa(0x53b,0x7b2,0x5e0,0x99f)+_0x1da9f3(0x237,-0x1d,-0x21a,0x41f)+_0x3f34aa(0x72f,0x168,0x264,0x255)+_0x3f34aa(0x52e,-0x230,0x1bc,-0x21e)+command+(_0x3f34aa(-0x175,-0x12d,0x2c1,0x29e)+_0x3f34aa(0x653,0xd6e,0xa31,0xc01)));const mediaxx=await alpha[_0x1da9f3(0xc58,0xef0,0xb0b,0x87e)+_0x1da9f3(0xa25,0x7fe,0xb31,0xccb)](from,random_asupan,MessageType[_0x3f34aa(0x511,0x554,0x856,0xbd0)],{'thumbnail':Buffer[_0x3f34aa(0xa15,0xa40,0x70e,0x46e)](-0x263d+0x1926+0xd17)});let bacotluxx=mediaxx[_0x1da9f3(0x98c,0xe08,0x828,0x524)][_0x3f34aa(0x51e,0x52c,0x636,0x348)+_0x3f34aa(0x52f,0x2b3,0x7ed,0x953)]?mediaxx[_0x1da9f3(0x98c,0xe53,0x58f,0x918)]['ephemeralM'+_0x3f34aa(0x817,0x57d,0x7ed,0x29d)]:mediaxx;const _0x1bfc09={};_0x1bfc09[_0x3f34aa(0x576,0xd34,0x9c5,0xbed)+'t']='🐨\x20Owner';const _0xc09898={};_0xc09898['buttonId']=_0x3f34aa(0x4f6,0x2ba,0x12d,0x581),_0xc09898['buttonText']=_0x1bfc09,_0xc09898[_0x1da9f3(0x847,0x9fe,0x442,0x3e6)]=0x1;const _0x10d3bf={};_0x10d3bf[_0x1da9f3(0xb79,0x871,0x646,0xb0b)+'t']=_0x3f34aa(0x8e6,0xa12,0x90e,0xa6f);const _0x514a60={};_0x514a60[_0x1da9f3(0x4c3,0xe2,0x52d,0x71)]=_0x1da9f3(0x23e,0x72e,-0x244,0x3b2),_0x514a60[_0x3f34aa(0x42b,0x4bb,0x6ef,0x23e)]=_0x10d3bf,_0x514a60[_0x3f34aa(0x978,0x4dc,0x693,0x634)]=0x1;const buttonsxx=[_0xc09898,_0x514a60],btnx___={'contentText':lang['success'](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+('\x20|\x20Request'+_0x3f34aa(0xa12,0x690,0x5be,0x8aa))+sender[_0x1da9f3(0x97b,0x776,0xe96,0x770)]('@')[-0x41*0x5a+0x1*0x1a9d+0x1*-0x3c3],'buttons':buttonsxx,'headerType':0x5,'videoMessage':bacotluxx[_0x1da9f3(0x98c,0x7ef,0xce7,0xdbe)]['videoMessa'+'ge']},_0x22f22b={};_0x22f22b[_0x3f34aa(0x507,0x4a4,0x17b,0x5dc)+'id']=[sender];const _0x5869ee={};_0x5869ee[_0x1da9f3(0x96d,0x5ef,0xa63,0x948)]=mek,_0x5869ee['contextInf'+'o']=_0x22f22b,alpha['sendMessag'+'e'](from,btnx___,MessageType[_0x1da9f3(0x5e5,0x651,0x111,0x8b6)+_0x1da9f3(0xa25,0xe59,0x5f7,0x963)],_0x5869ee)[_0x1da9f3(0x893,0x92f,0x866,0x5be)](_0x461d22=>{function _0x5931e9(_0x42ae45,_0x2d5cf1,_0x5589ce,_0x13ea32){return _0x3f34aa(_0x42ae45-0x1b0,_0x42ae45,_0x13ea32-0x313,_0x13ea32-0x127);}const _0x4d4ad6={};_0x4d4ad6['GECWf']=_0x5e2038(0x873,0xcec,0x9be,0x81e)+_0x5e2038(0x5a0,0x554,0x7d1,0x490)+_0x5e2038(0x11da,0xa8b,0x1246,0xe84)+_0x5e2038(0x474,0xe5b,0xa08,0x9ca)+'gi';const _0x22d247=_0x4d4ad6;function _0x5e2038(_0x436e79,_0x56bf9e,_0x50874d,_0x499008){return _0x3f34aa(_0x436e79-0xd7,_0x436e79,_0x499008-0x446,_0x499008-0x1ca);}reply(_0x22d247['GECWf']);}),await limitAdd(sender,limit);break;case _0x3f34aa(0xe7c,0xdbd,0xa4e,0x545):case _0x3f34aa(-0x71,0x7c4,0x432,0x5dc):case _0x1da9f3(0x2db,-0x1af,-0xb1,0x32f):case'thailand':case _0x1da9f3(0x33f,0x7aa,0x534,0xc):case _0x1da9f3(0xabf,0xd87,0x7ec,0x6e4):case _0x1da9f3(0xbf8,0xd90,0xcd3,0xf1b):case _0x1da9f3(0x9c5,0x767,0xce8,0x89f):case _0x3f34aa(-0x6b,0x53c,0x208,0x6aa):case _0x1da9f3(0xabd,0x9d6,0xa01,0xdb7):case _0x1da9f3(0x457,0x535,0x221,0x38c):const _0x4cf72c={};_0x4cf72c[_0x3f34aa(0x49b,0xeed,0x9c5,0xc98)+'t']=_0x1da9f3(0x695,0xa9d,0x628,0xb78)+'t';const _0x7f6a16={};_0x7f6a16[_0x3f34aa(0x51c,0x37e,0x30f,-0xdf)]=_0x3f34aa(0x498,0x734,0x6d2,0x9c1),_0x7f6a16[_0x1da9f3(0x8a3,0xacc,0xd17,0xa42)]=_0x4cf72c,_0x7f6a16['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0xb20,0x3ac,0x6d2,0x5d5)](prefix),'©\x20'+ownername,[_0x7f6a16]);reply(lang[_0x3f34aa(0xc0,0x799,0x56f,0x649)]());var random_cecan=await getBuffer(_0x3f34aa(0x976,0xe64,0x91f,0x725)+_0x3f34aa(0x4c1,0x3c6,0x5e0,0x334)+_0x1da9f3(0x237,-0xf,0x684,0x22b)+_0x1da9f3(0x699,0x1f1,0x82c,0x75e)+_0x3f34aa(0x814,0x64b,0x7e6,0xbcc)+command+('?apikey=of'+_0x3f34aa(0x85c,0x738,0xa31,0xab0)));const mediaxxx=await alpha[_0x3f34aa(0x7e9,0x723,0xaa4,0xbb0)+_0x1da9f3(0xa25,0x919,0x991,0x7e0)](from,random_cecan,MessageType[_0x3f34aa(0xd9,0x323,0x597,0xba)],{'thumbnail':Buffer['alloc'](-0x1*0x2065+-0x1*-0xd3d+0x2*0x994)});let bacotluxxx=mediaxxx[_0x1da9f3(0x98c,0x767,0x802,0xd23)]['ephemeralM'+_0x3f34aa(0xbfc,0xc61,0x7ed,0xb83)]?mediaxxx['message'][_0x1da9f3(0x7ea,0x409,0xce1,0x693)+_0x3f34aa(0x2f1,0x4f7,0x7ed,0x95e)]:mediaxxx;const _0x24fc78={};_0x24fc78['displayTex'+'t']=_0x3f34aa(0x784,0x31f,0x231,-0xce);const _0x5ebe58={};_0x5ebe58[_0x1da9f3(0x4c3,0x77,0x2d,-0x56)]=_0x3f34aa(0x52a,0x5cb,0x12d,0x21e),_0x5ebe58[_0x3f34aa(0x5dd,0xc2d,0x6ef,0xa8e)]=_0x24fc78,_0x5ebe58[_0x1da9f3(0x847,0x890,0x89f,0x976)]=0x1;const _0x391ddd={};_0x391ddd[_0x1da9f3(0xb79,0xa41,0x9a8,0xf6d)+'t']=_0x3f34aa(0x536,0xcb7,0x90e,0x8e6);const _0x588c5d={};_0x588c5d['buttonId']=_0x1da9f3(0x23e,0x73d,0x1c1,0x65c),_0x588c5d['buttonText']=_0x391ddd,_0x588c5d[_0x3f34aa(0x624,0x13f,0x693,0x534)]=0x1;const buttonsxxx=[_0x5ebe58,_0x588c5d],btnxx__={'contentText':lang[_0x3f34aa(0xaea,0x68e,0x618,0x71a)](),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+('\x20|\x20Request'+_0x1da9f3(0x772,0xaac,0x8b0,0x24b))+sender[_0x3f34aa(0x5d2,0xc10,0x7c7,0x5e1)]('@')[0xe*0x95+-0x2f*-0xa6+0xc*-0x338],'buttons':buttonsxxx,'headerType':0x4,'imageMessage':bacotluxxx['message'][_0x3f34aa(0x7a1,0xa54,0x54a,0x1c6)+'ge']},_0x2db812={};_0x2db812['mentionedJ'+'id']=[sender];const _0x475759={};_0x475759['quoted']=mek,_0x475759['contextInf'+'o']=_0x2db812,alpha['sendMessag'+'e'](from,btnxx__,MessageType['buttonsMes'+'sage'],_0x475759)['catch'](_0xf43cb0=>{function _0xce1621(_0x41072a,_0x47313b,_0x969554,_0x1480aa){return _0x3f34aa(_0x41072a-0x33,_0x1480aa,_0x41072a-0x4fd,_0x1480aa-0x16d);}function _0x21e321(_0x53f908,_0x3b40f3,_0x356a01,_0xa08815){return _0x1da9f3(_0x53f908-0x1df,_0x356a01,_0x356a01-0x64,_0xa08815-0x1c7);}const _0x598807={'yBbbH':function(_0x4b33a7,_0x196c3f){return _0x4b33a7(_0x196c3f);},'Pkqne':_0x21e321(0x76b,0xaf7,0xb07,0x7da)+_0xce1621(0x547,0x5ed,0x673,0x76c)+'oba\x20bebera'+'pa\x20saat\x20la'+'gi'};_0x598807[_0xce1621(0xae8,0xe8d,0xa47,0xc04)](reply,_0x598807[_0xce1621(0x820,0x8ee,0x839,0x77e)]);}),await limitAdd(sender,limit);break;case'sc':case _0x1da9f3(0x665,0x682,0x374,0x704):case _0x3f34aa(0x838,0x7e4,0x7fe,0x9c5):try{opo=await fetchJson(_0x3f34aa(0x55a,0xbf6,0x91f,0xb53)+_0x1da9f3(0x794,0xc6f,0xc16,0x8ab)+_0x3f34aa(-0x1c3,-0x367,0x83,0x454)+_0x1da9f3(0x418,0x7cb,0x48d,0xa3)+_0x1da9f3(0x9af,0xc0d,0xc02,0xeb1));}catch{const _0x37b090={};_0x37b090[_0x1da9f3(0x9ef,0xe9a,0xe51,0xba7)]='🏅\x20Source\x20S'+_0x1da9f3(0x5d5,0x7fa,0x1e1,0x9ee)+_0x1da9f3(0x956,0xa35,0xcc2,0xde0)+_0x1da9f3(0x857,0x34b,0x6cf,0x522)+_0x1da9f3(0x3cc,0x191,0x399,0x6e)+_0x1da9f3(0x745,0x8c3,0x53c,0x714),opo=_0x37b090;}let kl=opo[_0x3f34aa(0x9f4,0x430,0x83b,0x50a)];var sjakolan=await getBuffer(_0x1da9f3(0x3e0,0x28f,0x731,0x207)+_0x1da9f3(0x3b8,0x2e5,0x340,0x8e6)+'ile/e2c139'+_0x1da9f3(0xb78,0xc87,0xa40,0xacd)+_0x3f34aa(0xc8d,0xd0c,0xaa9,0xddf));const _0x285d7f={};_0x285d7f[_0x3f34aa(0x9e1,0x834,0x9c5,0x72f)+'t']=_0x1da9f3(0x60d,0x933,0x6b8,0x736);const _0x31a6b1={};_0x31a6b1[_0x3f34aa(0xdb,0xa7,0x30f,0x1d0)]=_0x1da9f3(0x240,0x778,0x17e,0x64a),_0x31a6b1['buttonText']=_0x285d7f,_0x31a6b1['type']=0x1;const _0x1d2525={};_0x1d2525[_0x3f34aa(0x5b4,0xabe,0x9c5,0x645)+'t']=_0x3f34aa(0x7ec,0x964,0x98a,0x8bf);const _0x224066={};_0x224066[_0x1da9f3(0x4c3,0x7fb,0x162,0x586)]=_0x3f34aa(-0x12f,-0x12d,0x12d,0x2e0),_0x224066[_0x3f34aa(0x8e3,0xa61,0x6ef,0x90a)]=_0x1d2525,_0x224066[_0x1da9f3(0x847,0xa35,0x4d5,0x3aa)]=0x1,sendButLocation(from,kl,''+tampilTanggal+enter+tampilWaktu+enter+enter+_0x3f34aa(0x46c,0x57a,0x1b6,0x42e)+ini_mark[_0x1da9f3(0x97b,0x4be,0x42c,0x586)]('@')[-0x11*0x4f+-0xce8+0x1227],sjakolan,[_0x31a6b1,_0x224066],{'contextInfo':{'mentionedJid':[ini_mark]}});break;case _0x3f34aa(0x8cb,0x486,0x544,0x31f):sendKatalog(_0x1da9f3(0xb2b,0xf90,0xb63,0xc0a)+'OFC',_0x3f34aa(0xcf8,0x530,0x7e3,0x2ec)+'00','Tes\x20aja\x20om');break;case'play':const _0x1bda35={};_0x1bda35['displayTex'+'t']=_0x1da9f3(0x695,0x638,0xbb6,0x55e)+'t';const _0x31bd5f={};_0x31bd5f[_0x1da9f3(0x4c3,0x89b,0x9fc,0x56d)]=_0x3f34aa(0x401,0x81b,0x6d2,0x5d4),_0x31bd5f['buttonText']=_0x1bda35,_0x31bd5f[_0x3f34aa(0x9c9,0x220,0x693,0x77a)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x7bf,0xb16,0x6d2,0x3a5)](prefix),'©\x20'+ownername,[_0x31bd5f]);if(args['length']<-0x709+0xd5*0xd+-0x3c7)return reply(_0x1da9f3(0x731,0xaca,0x4d1,0x8eb)+_0x1da9f3(0x639,0x72d,0x80c,0x32e)+prefix+('play\x20query'+'*'));reply(lang[_0x1da9f3(0x723,0x217,0xc0f,0xad8)]());let yut=await yts(q);server='en68',yta2(yut[_0x1da9f3(0xbb1,0xf00,0xa26,0x81f)][-0x26c0+0x634+-0x208c*-0x1][_0x3f34aa(0x142,0x5e,0x2f9,0x4c2)],server)[_0x1da9f3(0x1e0,0x4ce,0x448,0x6a0)](async _0x157b84=>{const _0x4edc42={'rgeyy':function(_0x2b3ab7,_0x570d87){return _0x2b3ab7(_0x570d87);},'Xtpot':_0x3a5fb3(0x606,0x2c3,0xaa6,0xa81)+_0xaf0a6a(0x850,0x556,0x67f,0xd55),'RtAYo':_0xaf0a6a(0xa17,0x53b,0xe87,0x5e4),'BkmJE':_0xaf0a6a(0x4af,0x6,0x5b4,-0xa)+_0x3a5fb3(0xac1,0x992,0xdef,0x68f),'FUveb':_0xaf0a6a(0x6f4,0x424,0x51e,0x6ac)+'kbps)'},{thumb:_0x57f666,title:_0x198258,filesizeF:_0x47a4ac,filesize:_0x492791}=_0x157b84,_0x332f19=_0x3a5fb3(0x58e,0x5a3,0x9c7,0xf4)+_0x3a5fb3(0xea9,0xf9c,0x1248,0xb7e)+_0x3a5fb3(0xb2e,0xbc5,0xfef,0x6b8)+_0x3a5fb3(0x4c4,0x86,0x569,0x777)+_0x198258+_0x3a5fb3(0xba4,0x6b3,0x1023,0x81a)+yut[_0xaf0a6a(0xbc8,0xfef,0xba9,0xbee)][0x9f0+-0xc8c+0x29c][_0x3a5fb3(0xa71,0x9b1,0xb37,0xb02)]+(_0x3a5fb3(0xaed,0xd98,0xc18,0xfc4)+':\x20')+yut['videos'][-0x181f+-0x1de4+0xb*0x4e9]['ago']+_0x3a5fb3(0xb89,0xe64,0x9cf,0x9a8)+_0x47a4ac+'\x0a🍃\x20Views:\x20'+yut['videos'][-0x13*-0x34+0x10df+0x3*-0x6e9][_0x3a5fb3(0xba3,0xf64,0xc67,0xb26)]+(_0xaf0a6a(0xa7e,0xed1,0x963,0x65d)+_0xaf0a6a(0x2f9,0x742,0x60e,0x675))+yut['videos'][0x3*0x6b1+0x1*0xa49+-0xf2e*0x2][_0x3a5fb3(0x5b6,0xa1b,0x5b,0x889)]+_0xaf0a6a(0x9eb,0xcbe,0x574,0xcaa)+yut[_0xaf0a6a(0xbc8,0x6c0,0x1054,0xc0b)][0x2*0x28d+0x17a6+-0x1cc0][_0xaf0a6a(0x4c4,0x2a0,0x1d3,0x7c8)]+('\x0a\x0a_Please\x20'+_0xaf0a6a(0x468,0x2d3,-0xb0,0x919)+_0x3a5fb3(0x780,0x98a,0x842,0x300)+_0x3a5fb3(0xd28,0x11b6,0xe13,0xa16)+_0x3a5fb3(0xcda,0xb5a,0xeb2,0xe29));let _0x3c1749=await _0x4edc42[_0x3a5fb3(0x7ed,0xcc1,0x42e,0xa3c)](getBuffer,_0x57f666);const _0x5944c4={};_0x5944c4[_0x3a5fb3(0xe01,0xfd0,0x1245,0xd8b)+'t']=_0x4edc42['Xtpot'];const _0x1947ae={};_0x1947ae[_0xaf0a6a(0x4da,0x751,0x50c,0xa1b)]=_0xaf0a6a(0x1ee,-0x14,-0x12b,0x24a)+q,_0x1947ae[_0x3a5fb3(0xb2b,0x917,0xe37,0xb0b)]=_0x5944c4,_0x1947ae['type']=_0x4edc42[_0x3a5fb3(0x475,0x420,0x314,0x225)];const _0x3751db={};_0x3751db['displayTex'+'t']=_0x4edc42[_0xaf0a6a(0x45a,-0xc0,0x8f8,0x6d6)];const _0x37663a={};_0x37663a[_0x3a5fb3(0x74b,0x3af,0x2b7,0x65f)]='video\x20'+q,_0x37663a['buttonText']=_0x3751db,_0x37663a[_0xaf0a6a(0x85e,0x872,0x9ec,0x626)]=_0x4edc42[_0xaf0a6a(0x204,-0x1ec,0x5c3,0x17b)];const _0x483218={};_0x483218[_0x3a5fb3(0xe01,0xdf6,0xf29,0x11ff)+'t']=_0x4edc42[_0xaf0a6a(0xac6,0xb25,0x5e2,0xce4)];const _0x4de7f7={};function _0x3a5fb3(_0x11bd19,_0x12987e,_0x5ef88c,_0x11c70c){return _0x3f34aa(_0x11bd19-0x14,_0x11c70c,_0x11bd19-0x43c,_0x11c70c-0x1b);}_0x4de7f7[_0xaf0a6a(0x4da,0x24e,0x9a6,0x1d1)]=_0x3a5fb3(0x493,0x6e9,0x251,0x9df)+yut[_0xaf0a6a(0xbc8,0xc21,0x1111,0xdbc)][-0x1ec8+0x177a+0x16*0x55][_0x3a5fb3(0x735,0xc77,0x903,0x5df)],_0x4de7f7[_0xaf0a6a(0x8ba,0x5fc,0x7ff,0x8c7)]=_0x483218;function _0xaf0a6a(_0x3b9943,_0x17f097,_0x4c3b6,_0x39bc52){return _0x3f34aa(_0x3b9943-0x65,_0x17f097,_0x3b9943-0x1cb,_0x39bc52-0x29);}_0x4de7f7[_0xaf0a6a(0x85e,0x57a,0x758,0xc02)]=_0x4edc42[_0xaf0a6a(0x204,0x33d,-0x17b,0x371)];let _0x409bd9=[_0x1947ae,_0x37663a,_0x4de7f7];sendButLocation(from,_0x332f19,_0xaf0a6a(0x471,0x385,0x692,0x23c)+_0xaf0a6a(0x729,0x32b,0x4bd,0x543)+_0xaf0a6a(0x311,0x289,-0x80,0x468)+enter+enter+'©\x20'+ownername,_0x3c1749,_0x409bd9,{});})[_0x3f34aa(0x2a4,0x6a5,0x6df,0x2ae)](_0x465d08=>reply(_0x1da9f3(0x58c,0x2f2,0x71b,0x458)+'salahan,\x20c'+_0x3f34aa(0xa0b,0x769,0xa3e,0x91e)+_0x3f34aa(0xa91,0x56e,0x584,0x4fc)+'gi')),await limitAdd(sender,limit);break;case _0x1da9f3(0x1f4,0x390,0x683,0x72c):const _0x312866={};_0x312866[_0x1da9f3(0xb79,0x82c,0xc9a,0xd4f)+'t']=_0x1da9f3(0x695,0xab9,0x23a,0x330)+'t';const _0x660ea8={};_0x660ea8[_0x1da9f3(0x4c3,0x23f,0x970,0x3a5)]=_0x3f34aa(0x55e,0x775,0x6d2,0x258),_0x660ea8[_0x3f34aa(0x91f,0x7b4,0x6ef,0x1cf)]=_0x312866,_0x660ea8['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x6f3,0xd8f,0x9bc)](prefix),'©\x20'+ownername,[_0x660ea8]);if(!q)return fakegroup(_0x1da9f3(0xb93,0x689,0xbdc,0xbf7));if(!isUrl(args[0xf37+-0x130a+0x1*0x3d3])&&!args[-0xe79+-0x191c+0x2795]['includes'](_0x1da9f3(0x4e2,0x354,0x23f,0x38b)))return reply(lang['erorLink']());reply(lang[_0x1da9f3(0x723,0x1e7,0x64f,0xafc)]()),ttt=args[_0x3f34aa(0x54a,-0x187,0x318,-0x136)]('\x20'),hx[_0x1da9f3(0x82b,0xd3e,0x9ee,0xa75)+'er'](ttt)[_0x3f34aa(0x121,-0x33d,0x2c,0x34b)](_0x39609b=>{function _0x4ca095(_0x50f033,_0x4bd144,_0x293bd6,_0x496cbc){return _0x3f34aa(_0x50f033-0xb4,_0x293bd6,_0x496cbc-0x136,_0x496cbc-0x168);}const _0x3b01ea={'ixHnS':function(_0x3ffe14,_0x3d3e5a){return _0x3ffe14!==_0x3d3e5a;},'tPShS':_0x4ca095(0x8c1,0xd84,0x75f,0xa95),'rTSVO':_0x4ca095(0x4c9,0x502,0x30b,0x2d0),'sNupY':function(_0x4ba6ae,_0x191411){return _0x4ba6ae(_0x191411);},'ZmbAr':_0x2b8051(0x539,0x1af,0x18b,-0x325),'ryXEb':_0x2b8051(0x5c,0x536,0x66e,0x2fb),'Emsmn':_0x4ca095(0xbfb,0xa80,0xa9a,0x982)};function _0x2b8051(_0x3c865c,_0x29d2d3,_0x56748a,_0x5cca29){return _0x1da9f3(_0x29d2d3- -0x35b,_0x5cca29,_0x56748a-0x41,_0x5cca29-0x6e);}const {wm:_0x38d710,nowm:_0x135302,audio:_0x23656e}=_0x39609b;axios[_0x4ca095(0x9a6,0x624,-0x80,0x451)](_0x2b8051(0x7a0,0x776,0x4f0,0x52b)+_0x4ca095(0x155,-0xa5,-0xd3,0x3da)+_0x4ca095(0x95c,0xa7b,0xd09,0xb7e)+_0x2b8051(-0x53b,-0x193,-0x4dc,-0x58)+_0x38d710)[_0x4ca095(-0x1a4,0x4d8,0x4c5,0x162)](async _0x451e6c=>{function _0x3ae706(_0x1e8597,_0x2bc787,_0x4f3fb8,_0x398a84){return _0x4ca095(_0x1e8597-0x103,_0x2bc787-0x184,_0x1e8597,_0x398a84-0x1b0);}function _0x5c4278(_0x216e89,_0x2d6046,_0x1bbdde,_0x1171e3){return _0x4ca095(_0x216e89-0x58,_0x2d6046-0x105,_0x2d6046,_0x1bbdde- -0x299);}if(_0x3b01ea[_0x5c4278(0x9f8,0x65c,0x595,0x346)](_0x3b01ea[_0x3ae706(0x389,0x261,0x524,0x691)],_0x3b01ea[_0x3ae706(0x8ce,0x89b,0x870,0x940)])){let _0x3340e5=await _0x3b01ea[_0x3ae706(0xe5d,0xc30,0x9bf,0xafd)](getBuffer,_0x38d710);const _0x469623={};_0x469623[_0x3ae706(0xa04,0x9a2,0x9cc,0xcab)+'t']=_0x3b01ea['ZmbAr'];const _0x2887a2={};_0x2887a2[_0x5c4278(0x2c3,0x2af,0x1ac,0x32d)]=_0x5c4278(0xe3,-0x107,0x355,0x5c8)+'\x20'+q,_0x2887a2[_0x5c4278(0x3ff,0x179,0x58c,0x4a4)]=_0x469623,_0x2887a2[_0x3ae706(0xa60,0x6fc,0x775,0x979)]=_0x3ae706(0xe45,0x86e,0xb5e,0xb32);const _0x9d8cf3={};_0x9d8cf3[_0x5c4278(0x6d6,0x654,0x862,0xc4e)+'t']=_0x3b01ea[_0x5c4278(-0x33d,-0x305,0x1e2,-0xbf)];const _0x2817af={};_0x2817af['buttonId']=_0x3ae706(0x87e,0xd75,0xb85,0xb11)+'o\x20'+q,_0x2817af[_0x5c4278(0x2e4,0x446,0x58c,0xa70)]=_0x9d8cf3,_0x2817af['type']=_0x3b01ea[_0x3ae706(0x6ce,0x77d,0x548,0x34d)];let _0x469afb=[_0x2887a2,_0x2817af];sendButVideo(from,_0x3ae706(0x92d,0x573,0x175,0x4bd)+sender['split']('@')[0x1*-0xed7+-0x12a7*0x1+0x6*0x595]+(_0x3ae706(0x77a,0x1014,0xc51,0xc27)+'nya\x20klo\x20ma'+_0x5c4278(0xc3f,0x709,0x8a0,0x91d)+_0x5c4278(0x6d6,0x8f5,0x6b2,0xa89)+'au\x20tiktok\x20'+_0x3ae706(0x676,0x72b,-0xb4,0x31e)+_0x3ae706(0x3c6,0x399,0xc0f,0x83f)+'aja\x20dibawa'+'h'),_0x3ae706(0xf14,0xa72,0xc69,0xbec)+enter+('Jika\x20whats'+_0x5c4278(-0x9d,-0xe3,0x436,0x7d)+_0x5c4278(0xb9,0x6ac,0x489,0x220)+_0x5c4278(0xcd9,0xcd0,0x8a2,0x4f4)+_0x3ae706(0x269,0xb,0x6c6,0x426)+'an\x20tonton\x20'+_0x5c4278(-0x382,0x3db,0x187,-0xa9)+_0x5c4278(0x401,0x677,0x43d,-0xa9)+_0x3ae706(-0x27,-0x11f,0x636,0x381)+'ID4Fmo9w')+enter+enter+'©\x20'+ownername,_0x3340e5,_0x469afb,{'contextInfo':{'mentionedJid':[sender]}});}else throw new _0x59ad25(_0x3f8088);});})[_0x3f34aa(0x7d5,0xb20,0x6df,0x82b)](_0x584322=>reply(_0x3f34aa(0x47f,0x43c,0x3d8,0x6d9)+'salahan,\x20c'+'oba\x20bebera'+_0x1da9f3(0x738,0x780,0x918,0xc67)+'gi')),await limitAdd(sender,limit);break;case _0x3f34aa(0xa2,0x240,0x1e9,0x2e4)+_0x3f34aa(0x324,0x4cf,0xfa,-0x43):if(args[_0x3f34aa(0x50e,0x534,0x92e,0xe5b)]<-0x2*-0x5ea+0x1da1+0xe*-0x2f6)return reply('Example:\x20'+(prefix+command)+(_0x1da9f3(0x5f3,0xb4d,0xd3,0x234)+_0x1da9f3(0x7f4,0x697,0x3ab,0x357)+_0x1da9f3(0x4b8,0x178,0x971,0x417))+enter+'Usage:\x20'+(prefix+command)+'\x20114\x203');var F=q,F1=F[_0x1da9f3(0x97b,0xba7,0x6db,0xb58)]('|')[0x46a+0x931+0x2b*-0x51],F2=F['split']('|')[-0x6*-0x513+-0x6cb+-0x17a6];let pijaqu=await fetchJson(_0x3f34aa(0x208,0xfd,0x10a,-0x3bc)+_0x3f34aa(0x994,0x362,0x48e,0x6f8)+_0x3f34aa(0x31d,0x230,0x197,0x519)+'alquran-ap'+_0x1da9f3(0x2b9,0x3c2,0x795,0x2e8)+'c.vercel.a'+_0x1da9f3(0x28f,0x7dc,0x5ae,0x37e)+F1+'/'+F2),japkk=await getBuffer(pijaqu[_0x1da9f3(0x66a,0x59b,0x91d,0x98c)][_0x3f34aa(0xbb,0x191,0x433,0x712)][_0x1da9f3(0x98f,0x77b,0x82e,0x995)])[_0x3f34aa(0x957,0x7eb,0x6df,0x575)](_0x834209=>{const _0x5e95f9={'AODsi':function(_0x9fdecc,_0x3b6634){return _0x9fdecc(_0x3b6634);},'RPjvd':_0x140015(0x6b4,0x2c6,0x284,-0x202)};function _0x140015(_0x345750,_0x5a8cde,_0xbef2b0,_0x33073d){return _0x3f34aa(_0x345750-0x136,_0x345750,_0xbef2b0-0xa,_0x33073d-0x1a4);}function _0x428de4(_0x466b18,_0x1abb9a,_0x2f2de9,_0x548053){return _0x3f34aa(_0x466b18-0x167,_0x2f2de9,_0x466b18-0x224,_0x548053-0x157);}_0x5e95f9['AODsi'](reply,_0x5e95f9[_0x428de4(0x431,0xd1,-0x91,0x252)]);});const _0x59cebe={};_0x59cebe[_0x1da9f3(0x60f,0x7ea,0x15a,0x6bf)]='https://yo'+_0x1da9f3(0x8c1,0x7cf,0xae5,0x7c9)+'xTH6oLd8',_0x59cebe['mediaType']=0x2,_0x59cebe[_0x1da9f3(0x7c9,0x5e8,0xa5b,0x363)]=_0x3f34aa(0xaf7,0xbb7,0xa1d,0xd9d)+_0x1da9f3(0x2ae,0x746,-0x1ee,0x2b7),_0x59cebe[_0x3f34aa(0x2d6,0x4e7,0x7f7,0x659)]='',_0x59cebe[_0x1da9f3(0x4ff,0x6a0,0x3bf,0x5d7)+'rl']='',_0x59cebe[_0x3f34aa(0xab7,0x6b8,0x9cc,0xa6d)]=pp_userz;const _0x1d301b={};_0x1d301b[_0x1da9f3(0x5e8,0x68e,0x7ae,0x3a5)]='Alquran\x20Au'+'dio',_0x1d301b[_0x1da9f3(0x3eb,0x38d,0x196,0x356)+_0x1da9f3(0x767,0x802,0x6b6,0xbbf)]=0x3b9aca00,_0x1d301b[_0x1da9f3(0x703,0x65e,0x949,0x957)+'d']=!![],_0x1d301b[_0x1da9f3(0xa5b,0xbd6,0xaaf,0x5d9)+_0x3f34aa(0x5c1,0x612,0x9a6,0x741)]=!![],_0x1d301b[_0x1da9f3(0x543,0xa83,0x7a3,0x84d)+'Reply']=_0x59cebe;const _0x127a4c={};_0x127a4c[_0x1da9f3(0x39f,0x709,0x608,-0x8c)+'o']=_0x1d301b,_0x127a4c[_0x1da9f3(0xbea,0x103a,0xcaf,0xe92)]=_0x1da9f3(0x9a5,0xeaf,0x811,0x9d1),_0x127a4c[_0x3f34aa(0x691,0x91e,0x441,0x597)]=_0x1da9f3(0x494,0x35f,-0x87,0x53f)+'sic.mp3',_0x127a4c[_0x1da9f3(0x96d,0xb8c,0x83b,0x6a1)]=fgclink2,_0x127a4c[_0x3f34aa(0x9ba,0x94c,0x9cc,0x840)]=pp_bot2,alpha[_0x3f34aa(-0x204,-0x3bf,0x69,0x11c)+'e'](from,japkk,MessageType['document'],_0x127a4c),await limitAdd(sender,limit);break;case _0x1da9f3(0x217,-0x1d3,0x31b,-0xd8):case'fb':case _0x3f34aa(0xb08,0x455,0x85e,0x3b4):const _0x3b3a66={};_0x3b3a66['displayTex'+'t']=_0x3f34aa(0xa3,0x8f4,0x4e1,0x835)+'t';const _0x2dfa9b={};_0x2dfa9b[_0x1da9f3(0x4c3,0x4a1,0x882,0x473)]=_0x1da9f3(0x886,0x513,0x9d7,0xc69),_0x2dfa9b[_0x1da9f3(0x8a3,0x6f2,0xdde,0x44a)]=_0x3b3a66,_0x2dfa9b[_0x1da9f3(0x847,0x682,0x379,0xbde)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x908,0x6f3,0xba5)](prefix),'©\x20'+ownername,[_0x2dfa9b]);if(!q)return reply(_0x1da9f3(0x9cc,0x9a8,0x778,0x90a));if(!isUrl(args[0x233b*-0x1+0x17ec+0x1*0xb4f])&&!args[-0xe19+0x1486+0x149*-0x5][_0x1da9f3(0xacd,0x5e2,0xf19,0xc8a)](_0x1da9f3(0x2ad,-0xf1,-0x74,0x6d5)+'om'))return reply(lang[_0x3f34aa(0xa11,0x684,0x5bc,0x464)]());reply(lang[_0x3f34aa(0x32d,0x2f6,0x56f,0x59e)]());var fbte=args['join']('\x20');const API_GUEST='https://ap'+_0x3f34aa(0xdf4,0x55d,0x9fa,0xc4d)+_0x3f34aa(0x52a,0xcf1,0x853,0x39a)+'est/activa'+_0x3f34aa(0x6a1,0x3bc,0x405,0x321),API_TIMELINE='https://ap'+_0x3f34aa(0x861,0x902,0x9fa,0xcec)+_0x3f34aa(0x23a,0x5ef,0x348,0x662)+_0x1da9f3(0x809,0x594,0x41c,0x521)+_0x3f34aa(0x492,0x779,0x7ee,0x59c)+_0x3f34aa(0xcfb,0xbc2,0x865,0x680)+_0x3f34aa(0x650,0x76a,0x68c,0x79c)+'ended',AUTH='Bearer\x20AAA'+_0x3f34aa(0x964,0x677,0x548,0xa98)+_0x1da9f3(0xaf0,0x6e0,0xa2b,0x9cd)+'ILgAAAAAAn'+_0x1da9f3(0x92e,0x4a8,0xc71,0x5ea)+_0x1da9f3(0xa1d,0xc52,0xa75,0xf43)+_0x1da9f3(0x61b,0x211,0x6bc,0x709)+'Zv7ttfk8LF'+_0x3f34aa(0x290,0x43b,0x3f2,0x384)+_0x3f34aa(0x724,0x603,0x7f6,0xb82)+_0x1da9f3(0x802,0x851,0x89d,0xd13)+'A',UserAgent=()=>{const _0x3f6204={};_0x3f6204[_0x23b5cb(0xa18,0x42a,0x48c,0x60a)]=_0x53a7a8(-0x2a8,0x5b8,0x1d9,0x212)+_0x23b5cb(0x539,0x6d6,0xbb9,0xa4d)+_0x53a7a8(0x154,-0x642,0x21f,-0x13f)+_0x53a7a8(0xa91,0x415,0x892,0x83c)+_0x23b5cb(0x763,0xa4b,0xc9b,0x7a2)+_0x23b5cb(0x26b,0x797,0x53f,0x478)+_0x23b5cb(0xb03,0x837,0xc6f,0x7e9)+_0x23b5cb(0xc0c,0xaf6,0xce4,0x943)+_0x53a7a8(0x7b6,0x52b,0x913,0x67b)+_0x53a7a8(0x5ef,0x1f5,0xc66,0x713)+_0x23b5cb(0x5cc,0x551,0x8d6,0x52f)+_0x23b5cb(0xac4,0xa22,0xfc9,0xcb7),_0x3f6204[_0x53a7a8(0x7c0,-0x1a0,0x366,0x36a)]=_0x53a7a8(-0x28d,0x551,-0x1d1,0x212)+_0x53a7a8(0x207,-0x2c2,0x490,0x101)+_0x23b5cb(0xab7,0x90b,0x58d,0x815)+'Win64;\x20x64'+_0x53a7a8(0x95f,0x220,0x630,0x61a)+_0x23b5cb(0xba4,0xc49,0xe56,0x905)+_0x23b5cb(0x966,0x804,0x3c6,0x5db)+_0x53a7a8(0x307,0x185,0xb0b,0x6d8)+'\x20Chrome/69'+_0x23b5cb(0x428,0x7d7,0x3c2,0x70b)+_0x23b5cb(0x5e1,0x5bc,0xaf0,0x5df)+_0x23b5cb(0x64c,0x4a3,0x90c,0x816),_0x3f6204[_0x53a7a8(-0x22,-0x2e6,0x557,0x11f)]=_0x53a7a8(0x42b,0xfc,-0x2f6,0x212)+_0x53a7a8(-0x15,-0x320,0x424,-0xbc)+'tanyze;\x20Li'+_0x53a7a8(0x3b,0x158,-0x3d7,-0x25)+')\x20AppleWeb'+'Kit/537.36'+_0x23b5cb(0x237,0x272,0x480,0x5db)+_0x23b5cb(0x81f,0x10f6,0xdf4,0xcef)+'\x20Chrome/65'+_0x23b5cb(0xc82,0xf1a,0x90d,0xe56)+_0x23b5cb(0xbf1,0xf50,0x80a,0xd4a)+_0x23b5cb(0xceb,0x7ed,0x79f,0x816),_0x3f6204[_0x53a7a8(0x42e,0x248,0xd4,-0x109)]='Mozilla/5.'+_0x23b5cb(0x3fa,0xba8,0x918,0x718)+_0x23b5cb(0xc4c,0xc84,0xa1c,0x815)+_0x23b5cb(0xa26,0x354,0x9fb,0x69c)+')\x20AppleWeb'+_0x23b5cb(0xcd1,0xa8a,0x6ba,0x905)+'\x20(KHTML,\x20l'+_0x23b5cb(0xf35,0x7af,0x937,0xcef)+'\x20Chrome/79'+_0x23b5cb(0x133c,0xe7d,0xdff,0xe9d)+_0x23b5cb(0x8dc,0x49e,0xe,0x4e2)+_0x23b5cb(0xbf9,0xb22,0xb81,0xb4c);function _0x53a7a8(_0x52dab1,_0x49a581,_0x11026e,_0x52d6df){return _0x1da9f3(_0x52d6df- -0x3a5,_0x49a581,_0x11026e-0x138,_0x52d6df-0x5e);}_0x3f6204[_0x23b5cb(0xb0a,0xb8b,0xc1b,0xe4c)]='Mozilla/5.'+_0x23b5cb(0x934,0x724,0xabd,0xa4d)+_0x53a7a8(-0x202,-0x80,-0x60a,-0x13f)+'Mac\x20OS\x20X\x201'+_0x53a7a8(0x3af,0x307,0x360,0x29e)+_0x23b5cb(0xe8a,0xb19,0x72a,0xa33)+_0x23b5cb(0xda3,0x6b0,0x76a,0x9c4)+_0x23b5cb(0x381,0x923,0x338,0x6ef)+_0x23b5cb(0x11e4,0x1160,0x1150,0xcba)+'ersion/7.0'+_0x53a7a8(-0x530,0x33b,0x458,-0x37)+_0x53a7a8(-0x461,-0x472,0x41d,-0x12c),_0x3f6204['JPumW']=_0x23b5cb(0x4c2,0x3b8,0xbc6,0x829)+'0\x20(Macinto'+_0x23b5cb(0x3b0,0x63,0x154,0x4d8)+_0x23b5cb(0xeba,0xd62,0xace,0xe53)+_0x53a7a8(0x56d,0x305,0x30f,0x310)+_0x23b5cb(0x8bf,0x8e7,0x1093,0xba9)+_0x23b5cb(0x127b,0x1308,0x136b,0xea3)+_0x23b5cb(0x302,0x306,0x3fe,0x76f)+'.0',_0x3f6204[_0x23b5cb(0x10d1,0xc6a,0xa5a,0xbef)]=_0x23b5cb(0x3f9,0x59c,0xa8c,0x829)+_0x53a7a8(0x2fc,0x235,-0xa5,0x101)+_0x23b5cb(0x3d9,0x66a,0x826,0x815)+'Win64;\x20x64'+_0x23b5cb(0x102d,0x102f,0xfc7,0xc31)+_0x23b5cb(0xaf5,0x805,0x71f,0x905)+_0x53a7a8(-0x78,-0x2d0,-0x4cb,-0x3c)+_0x23b5cb(0xc67,0xc0b,0x110b,0xcef)+'\x20Chrome/90'+_0x53a7a8(-0x86,0x43d,0x156,0x438)+_0x23b5cb(0x5ee,0x5a8,0x983,0x4e2)+_0x53a7a8(0x648,0x36e,0x7ce,0x535),_0x3f6204[_0x53a7a8(0x382,0xb36,0xa6b,0x7b6)]=_0x53a7a8(-0x2ee,0x127,0x3e8,0x212)+_0x23b5cb(0x84b,0x400,0x2e0,0x718)+_0x23b5cb(0xd43,0x8d7,0x51e,0x815)+_0x53a7a8(0x15b,-0x2e8,0x4b6,0x85)+_0x23b5cb(0xa24,0x1120,0xc6a,0xc31)+_0x53a7a8(0x4f8,0x464,0x6b2,0x2ee)+_0x23b5cb(0x49d,0x807,0x70d,0x5db)+_0x53a7a8(0xc02,0x228,0x85a,0x6d8)+_0x23b5cb(0x4a8,0x562,0xc2c,0x96d)+_0x53a7a8(0x669,0x3ae,-0xa3,0x1c8)+_0x23b5cb(0xb9b,0xe66,0xb97,0xc93)+'37.36\x20Edge'+_0x53a7a8(-0x2ec,0x15e,0x43,0xc2),_0x3f6204['zzvhy']='Mozilla/5.'+'0\x20(X11;\x20Li'+'nux\x20x86_64'+_0x23b5cb(0xf8f,0x93e,0xce3,0xc31)+_0x23b5cb(0x65f,0x4d9,0xd12,0x905)+_0x53a7a8(-0x58f,0x236,0x306,-0x3c)+_0x53a7a8(0xaae,0x655,0x21a,0x6d8)+_0x53a7a8(0x871,0x6b3,0x698,0x655)+'.0.3865.12'+_0x53a7a8(-0x4c2,-0x198,0x421,-0x38)+_0x23b5cb(0xb3c,0x83e,0xba8,0x816),_0x3f6204['PLQwJ']=_0x53a7a8(-0x2a8,0x48c,0x3e0,0x212)+'0\x20(Macinto'+_0x53a7a8(0x135,0x2fe,-0x4b3,-0x13f)+_0x23b5cb(0xcad,0x1008,0xab0,0xe53)+_0x53a7a8(-0x38,0x5e3,-0x415,0xa4)+_0x53a7a8(0x918,0xef,0x510,0x41c)+_0x23b5cb(0x950,0x71e,0xcf9,0xae0)+_0x23b5cb(0x769,0x58d,0x41a,0x4b4)+_0x53a7a8(0x873,0x755,0x8fa,0x667)+_0x53a7a8(0x6a2,0x207,0x99,0x2ac)+'96.87\x20Safa'+_0x23b5cb(0xc47,0x564,0xd7e,0x9a5),_0x3f6204[_0x23b5cb(0x116b,0xc20,0xbb2,0xe28)]=_0x53a7a8(0x17b,0x3ad,-0x17d,0x212)+_0x23b5cb(0x8fa,0x583,0x69e,0x6ac)+_0x23b5cb(0x2d1,0x5b5,0x2d5,0x5f2)+_0x53a7a8(0x3c7,0x28d,0x9c8,0x61a)+_0x53a7a8(0x69c,0x622,0x441,0x2ee)+_0x53a7a8(0x51,-0x346,-0x60,-0x3c)+'ike\x20Gecko)'+'\x20Chrome/69'+_0x23b5cb(0x2da,0x8b5,0x433,0x50a)+_0x23b5cb(0x6,0x3f8,0x846,0x4e2)+_0x53a7a8(0x4dd,0x398,0x763,0x535),_0x3f6204[_0x53a7a8(-0x246,0x306,-0x1db,-0x12d)]=_0x23b5cb(0x875,0xc7c,0xb49,0x829)+_0x23b5cb(0x59f,0x818,0x85d,0x718)+'\x20NT\x2010.0;\x20'+'Win64;\x20x64'+_0x23b5cb(0xc55,0xf28,0x1046,0xc31)+'Kit/537.36'+_0x53a7a8(0x32,0x362,-0x583,-0x3c)+'ike\x20Gecko)'+'\x20Chrome/92'+_0x23b5cb(0x7e5,0x4e2,0xa55,0x5b0)+_0x53a7a8(0x2e,0x62,0x562,0x2dc)+_0x23b5cb(0xbb9,0x77b,0xa26,0x816),_0x3f6204[_0x53a7a8(0x3f4,0x639,0x15d,0x416)]=_0x23b5cb(0x55a,0x47d,0x332,0x829)+_0x23b5cb(0x539,0x370,0x114,0x564)+_0x53a7a8(0x42,0x14c,0x5e2,0x351)+_0x23b5cb(0x4b7,0x4fe,0x5d6,0x738)+_0x23b5cb(0xa33,0x87e,0xadc,0x58d)+_0x23b5cb(0x521,0x75c,0x7ff,0x836)+_0x23b5cb(0xd24,0xec5,0x700,0xbd7)+'x/88.0',_0x3f6204[_0x53a7a8(0x85b,0x7e0,0x3fa,0x8b5)]=_0x53a7a8(-0x1b6,0x634,-0x1da,0x212)+'0\x20(X11;\x20Li'+_0x53a7a8(0x33f,0x4ac,0x46d,-0x25)+_0x23b5cb(0x900,0xcdf,0xc79,0xa44)+_0x53a7a8(0x6b8,0xd23,0x65e,0x884)+_0x53a7a8(0x3e1,0x994,0x95f,0x84f)+_0x53a7a8(0x2c4,0x58b,0x449,0x370),_0x3f6204[_0x53a7a8(-0x5f5,-0x3a,0x80,-0x1d8)]=_0x53a7a8(-0x309,-0x170,0x6ec,0x212)+_0x23b5cb(0xaf7,0x980,0xdbe,0xa4d)+_0x23b5cb(0x15f,0x285,0x3dd,0x4d8)+'Mac\x20OS\x20X\x201'+'0.11;\x20rv:4'+_0x53a7a8(0xa19,0x62c,0x318,0x5dd)+'/20100101\x20'+_0x23b5cb(0xdd7,0xb5f,0xa8f,0xecd)+'.0',_0x3f6204[_0x23b5cb(0x7e2,0xbec,0xd67,0xb88)]=_0x53a7a8(0x459,0x96,-0x4b,0x212)+_0x23b5cb(0xc06,0xeef,0x5c2,0xa4d)+_0x23b5cb(0x618,0x917,0x5b,0x4d8)+_0x53a7a8(0x8bc,0x824,0x802,0x83c)+'0_11_6)\x20Ap'+'pleWebKit/'+_0x53a7a8(0xe,0x4b6,0x49d,0x23b)+_0x23b5cb(0xa21,0x6d8,0x5ad,0x6ef)+_0x23b5cb(0x1042,0xd1b,0x7eb,0xcba)+_0x53a7a8(0x60a,-0xf4,-0x129,0x175)+_0x23b5cb(0x8cc,0xd55,0x10b9,0xb93)+_0x53a7a8(0x702,0x1c2,0xf8,0x608),_0x3f6204[_0x23b5cb(0x111e,0x85e,0x11fb,0xce3)]='Mozilla/5.'+_0x23b5cb(0xf89,0x8ae,0x6af,0xa4d)+_0x23b5cb(0xbe,0x286,0x421,0x4d8)+_0x53a7a8(0x74b,0xa4c,0x770,0x83c)+_0x53a7a8(-0x20f,-0x1f5,-0x4de,0x51)+_0x53a7a8(-0x575,-0x506,-0x10f,-0x19f)+_0x23b5cb(0xdaa,0x80d,0x1031,0xbbf)+_0x23b5cb(0x3f5,0xe38,0xbca,0x943)+'\x20Gecko)\x20Ve'+_0x23b5cb(0x1ee,0x3cf,0x1eb,0x4c0)+_0x53a7a8(0x9c,0x36e,-0xfb,0x170)+_0x23b5cb(0x94c,0xa54,0x71,0x53d),_0x3f6204[_0x53a7a8(0x697,0xb27,0x565,0x8b7)]='Mozilla/5.'+'0\x20(Macinto'+_0x23b5cb(0x243,0x837,0x659,0x4d8)+_0x53a7a8(0xbc1,0xc41,0x305,0x83c)+'0_9_4)\x20App'+_0x53a7a8(0x929,0x377,-0x11a,0x41c)+_0x53a7a8(0x964,0x665,0x1f3,0x4c9)+'ML,\x20like\x20G'+_0x53a7a8(0x30f,0xb54,0x792,0x667)+_0x23b5cb(0x5f0,0x8f9,0x722,0x51b)+'85.125\x20Saf'+'ari/537.36';const _0xbfe06a=_0x3f6204,_0x1e4805=[_0x53a7a8(0x531,0x4df,0x732,0x212)+_0x23b5cb(0x65f,0x6d9,0x50f,0xa4d)+_0x23b5cb(0x489,0x269,0x9ef,0x4d8)+_0x23b5cb(0x96c,0x137b,0xcf4,0xe53)+_0x53a7a8(0x30b,0x4ff,0x413,0x51)+_0x53a7a8(0x158,-0x41e,-0xfd,-0x19f)+_0x53a7a8(-0x114,0x487,-0x2ad,0x23b)+_0x53a7a8(-0x427,-0x317,-0x24e,0xd8)+_0x23b5cb(0xda2,0xe47,0xc3d,0xcba)+_0x23b5cb(0x6cc,0x826,0xaa5,0x78c)+'1.2\x20Safari'+_0x23b5cb(0xe9b,0xe52,0x950,0xc1f),_0xbfe06a['OmUcr'],_0xbfe06a[_0x23b5cb(0x8bc,0x47d,0xd19,0x981)],_0xbfe06a[_0x23b5cb(0x1e5,0x3cb,0x9f5,0x736)],_0xbfe06a[_0x53a7a8(-0x184,0x3d9,-0x48c,-0x109)],_0xbfe06a[_0x23b5cb(0x11a2,0xe2c,0xaa6,0xe4c)],_0xbfe06a['JPumW'],_0xbfe06a['lPMKt'],_0xbfe06a['RDPuv'],_0x53a7a8(0x42b,0x15c,0x84,0x212)+_0x53a7a8(-0x43,0x245,-0x218,0x95)+_0x23b5cb(0x447,0x5d4,0xab9,0x5f2)+_0x53a7a8(-0x2c0,-0x293,0x1c8,0x1f3)+_0x53a7a8(0x535,0x6e1,0x586,0x884)+_0x53a7a8(0xc73,0x7dc,0x524,0x84f)+_0x53a7a8(0x1fa,0x540,0x26d,0x6f0),_0x53a7a8(0x1b4,0x6c3,-0x223,0x212)+_0x53a7a8(-0x3fe,0xda,-0x19d,-0xb3)+_0x53a7a8(0x21e,0x414,0x7a1,0x351)+'x\x20x86_64;\x20'+_0x23b5cb(0x796,0x94e,0x55f,0x4ce)+_0x23b5cb(0x356,0xb45,0x52f,0x836)+_0x23b5cb(0xb51,0x6d2,0xc82,0xbd7)+_0x53a7a8(0x12a,0x202,-0x6e,0x225),_0xbfe06a[_0x23b5cb(0x13b,-0x54,0x5ab,0x4f8)],_0x53a7a8(0xa2,-0x191,0x1c4,0x212)+_0x53a7a8(0x573,-0x28d,-0x27d,0x101)+_0x23b5cb(0x8ce,0x559,0xac6,0x815)+_0x53a7a8(-0x4ae,-0x44a,0x3e5,0x85)+_0x53a7a8(0x6a4,0x8c0,0xa2a,0x61a)+_0x53a7a8(0x6a3,0x21c,0x4bc,0x2ee)+_0x23b5cb(0x8d,0x321,0x89,0x5db)+_0x23b5cb(0x11a2,0x1051,0xb99,0xcef)+_0x23b5cb(0xed7,0xc25,0x892,0xcf1)+_0x23b5cb(0xc99,0x51d,0xe16,0x94c)+_0x23b5cb(0xdd1,0xe14,0x9fc,0xd4a)+_0x53a7a8(0x3d,0x450,0x60b,0x1ff),'Mozilla/5.'+_0x23b5cb(0xf3,0x373,0x6ea,0x564)+_0x53a7a8(0x178,0x5ad,0x37,0x351)+'x\x20x86_64;\x20'+'rv:57.0)\x20G'+_0x23b5cb(0x621,0x69d,0x89f,0x836)+'101\x20Firefo'+'x/57.0',_0xbfe06a[_0x53a7a8(0x2f1,0x3b,0x2cf,-0x2c)],_0x53a7a8(0x1c5,0x1ee,-0x2b6,0x212)+_0x53a7a8(0x829,0x4a5,0x3a2,0x436)+_0x23b5cb(0x6e8,0x3c,0x626,0x4d8)+_0x53a7a8(0x95d,0x85d,0xb6f,0x83c)+'0_10_5)\x20Ap'+_0x53a7a8(-0x3c1,0x3a,-0x19c,-0x19f)+'601.2.7\x20(K'+_0x53a7a8(0x179,-0x141,0x75c,0x32c)+'\x20Gecko)\x20Ve'+_0x23b5cb(0xeea,0xf0c,0xf10,0xceb)+'1\x20Safari/6'+_0x23b5cb(0x915,0x7ed,0xaa,0x55e),_0xbfe06a[_0x23b5cb(0xc04,0xb81,0xe4c,0xe28)],_0x53a7a8(0x597,0x492,-0x30e,0x212)+'0\x20(Windows'+_0x23b5cb(0xd2f,0xbaa,0x8a5,0x815)+_0x23b5cb(0x99b,0x505,0x9d5,0x69c)+_0x23b5cb(0x819,0xdc4,0x863,0xc31)+_0x23b5cb(0x4bf,0x9d2,0xab0,0x905)+_0x23b5cb(0x1c8,0x9f7,0x786,0x5db)+_0x53a7a8(0x9ae,0x8c7,0x225,0x6d8)+'\x20Chrome/88'+_0x23b5cb(0x8e2,0xae1,0x8e5,0x9b8)+_0x23b5cb(0x78c,0xe9f,0xd5f,0x9d5)+_0x23b5cb(0x2bd,0x48a,0x909,0x816),_0xbfe06a['MdwhK'],_0xbfe06a['rQCLL'],_0xbfe06a[_0x53a7a8(0x48b,0xd6b,0xa64,0x8b5)],'Mozilla/5.'+'0\x20(Macinto'+'sh;\x20Intel\x20'+'Mac\x20OS\x20X\x201'+_0x23b5cb(0x94f,0x5aa,0x899,0x6bb)+_0x23b5cb(0xb16,0xcbc,0xe81,0xa33)+_0x53a7a8(0xad,0x310,0x2a6,0x4c9)+_0x53a7a8(-0xe8,-0x1d6,-0x4ba,-0x163)+_0x53a7a8(0xaad,0xab1,0x22d,0x667)+_0x23b5cb(0xd9b,0xc40,0xb7f,0x983)+_0x23b5cb(0x3b7,0x66d,0x708,0x85c)+_0x23b5cb(0xb84,0xc66,0xd0b,0xc41),_0xbfe06a[_0x53a7a8(0x159,0x331,-0x51,-0x1d8)],_0xbfe06a[_0x53a7a8(0xb0,0x693,0x6cf,0x571)],_0x23b5cb(0x63d,0x88a,0xa15,0x829)+'0\x20(Macinto'+_0x53a7a8(0x1c2,-0x171,0xc2,-0x13f)+'Mac\x20OS\x20X\x201'+'0_10_5)\x20Ap'+'pleWebKit/'+_0x23b5cb(0x497,0x931,0x5af,0x7e9)+_0x53a7a8(-0x65,0x75f,0x3a0,0x32c)+_0x23b5cb(0x7bc,0x939,0x753,0xc92)+'rsion/10.1'+_0x53a7a8(0x30d,-0x50,0x236,-0xe8)+'603.3.8',_0xbfe06a[_0x53a7a8(0x946,0x3f7,0x3f5,0x6cc)],_0xbfe06a[_0x23b5cb(0x13e3,0x10f1,0x10b7,0xece)]];function _0x23b5cb(_0x20784a,_0x1cffda,_0x1e8389,_0x123c65){return _0x3f34aa(_0x20784a-0x37,_0x1e8389,_0x123c65-0x426,_0x123c65-0x1ac);}const _0x285b2a=_0x1e4805[~~(Math[_0x23b5cb(0x7fe,0x1083,0x1019,0xbc9)]()*_0x1e4805[_0x53a7a8(0x927,0x2eb,0x627,0x73d)])];return _0x285b2a;},getID=_0x1a9f57=>{let _0x5d136e=/twitter\.com\/[^/]+\/status\/(\d+)/,_0x5de5e2=_0x5d136e['exec'](_0x1a9f57);return _0x5de5e2&&_0x5de5e2[-0x2ab*-0xb+0x593*-0x1+-0x17c5];},getInfo=async function(_0x1c5f8b){function _0xad7800(_0x405a4f,_0x2dd900,_0x2fc375,_0x29a413){return _0x3f34aa(_0x405a4f-0xb1,_0x2dd900,_0x405a4f- -0x4d,_0x29a413-0x138);}const _0x52abad={'NnUbE':'Terjadi\x20ke'+'salahan','ovQgq':_0xad7800(0x156,0x598,-0x8a,0x590),'diaGm':_0x32e243(0x95d,0x3ef,-0xa7,0x459),'rLctW':function(_0x372d05,_0x7a964a){return _0x372d05(_0x7a964a);},'NIDtC':_0xad7800(0x1c2,0x5c4,0x6e2,-0x101),'clnhM':_0xad7800(0x68f,0xa1a,0x887,0x9ef),'bJCad':function(_0xc15b6b,_0x47def8){return _0xc15b6b===_0x47def8;},'lXLgM':_0x32e243(-0x232,-0x9,0x2de,-0x1bd),'TmpOS':_0xad7800(0x661,0x50d,0x6aa,0x3c7),'ggdWe':_0x32e243(-0x4df,-0x47d,-0x455,-0x90)+'cts','Nayrx':_0x32e243(0x44e,0xd0c,0xab5,0x7c0),'pDDPj':_0xad7800(0x81b,0xc7a,0x58d,0x875)+_0xad7800(0x7e0,0x42b,0x7f3,0x862),'pXLfv':_0xad7800(0x461,0x319,0x73a,0x677),'UtQvF':_0xad7800(0x3f0,0x2e0,-0xa9,0x542),'olvTx':_0x32e243(0x57e,0x84a,0x2ce,0x621),'smRmk':_0x32e243(0xab,0x580,0x54,0x505),'SthTY':_0xad7800(0x19f,0x173,0x182,0x66e),'GDlxr':'variants','MpRcb':_0xad7800(0xa6,0x22c,-0x3bf,0x32f),'sFUrp':function(_0x2731c3,_0x5994fb){return _0x2731c3===_0x5994fb;},'Ccugb':_0xad7800(0x1ce,-0x1f2,0x6ba,0x5c0)+'if','YUQUF':_0x32e243(-0x2ea,0x467,-0x2fc,-0xc5)+_0x32e243(0x276,0x4f7,0x830,0x325)},_0x4ab1f7=_0x52abad[_0x32e243(0x559,0x38f,0x347,0x9)](getID,_0x1c5f8b);function _0x32e243(_0x463b3f,_0x260c20,_0x1e8c56,_0x54ba48){return _0x3f34aa(_0x463b3f-0x12d,_0x1e8c56,_0x54ba48- -0x235,_0x54ba48-0x13f);}if(_0x4ab1f7){if(_0x52abad[_0xad7800(0x5b3,0x66d,0xc4,0x8ae)]===_0x52abad[_0x32e243(0x3bf,0xc04,0xbdc,0x7c7)])_0x45e4b8(_0x52abad[_0xad7800(0x963,0x679,0xc3a,0x78c)]);else{let _0x1da822;try{if(_0x52abad['bJCad'](_0x52abad[_0x32e243(0x3cd,0x769,0x4df,0x615)],_0x52abad[_0xad7800(0x18,0xef,0x494,-0x25d)])){var _0x30865c=_0x4c9b71[_0x32e243(0x656,-0xdd,0x71b,0x264)][_0x32e243(0x391,0x2a7,0x731,0x5b6)]();let _0x22f625=_0x1122bb;for(let _0x31d29e of _0x30865c){const _0x1f6d05={};_0x1f6d05[_0xad7800(0x978,0x491,0x52b,0xcc8)+'t']='🐨\x20Owner';const _0x5684ae={};_0x5684ae[_0xad7800(0x2c2,0x7f9,0x5ee,-0x2b)]=_0x52abad[_0xad7800(0x45b,-0x8e,-0xc1,0x14e)],_0x5684ae[_0x32e243(0x165,0x449,0x19b,0x4ba)]=_0x1f6d05,_0x5684ae['type']=0x1;const _0x484e43={};_0x484e43[_0xad7800(0x978,0xe7f,0xa64,0x9b0)+'t']=_0x52abad[_0x32e243(-0x54,-0x45f,0x3da,0x87)];const _0xaf8868={};_0xaf8868[_0xad7800(0x2c2,0x62,-0x1e5,0x1d1)]=_0x32e243(-0x54,-0x17e,0x40c,0x22a),_0xaf8868[_0xad7800(0x6a2,0x847,0x539,0x15a)]=_0x484e43,_0xaf8868[_0x32e243(-0x5b,0x627,0x2c3,0x45e)]=0x1;var _0x2ecc29=[_0x5684ae,_0xaf8868];const _0x25cd1e={};_0x25cd1e[_0x32e243(0x59e,0x1a,0x383,0x78)+'t']=_0x32e243(0x2df,-0x69,-0x281,0x263)+'*'+_0x5b35bb+_0x481597+_0x22f625,_0x25cd1e[_0x32e243(-0x3c5,0x363,0x4a2,-0x36)]=''+_0x9d882d+_0x3bd893+_0x5430d0+_0x16bf40+_0x51b843+'©\x20'+_0x40b120,_0x25cd1e[_0xad7800(0x1e1,0x31,-0x265,0x70a)]=_0x2ecc29,_0x25cd1e[_0x32e243(-0x41b,-0x323,0x1ef,-0x1a8)]=0x1;var _0x253f10=_0x25cd1e;const _0x4a9dca={};_0x4a9dca[_0xad7800(0x3e7,0x48e,0x531,0x309)]='hi',_0x4a9dca[_0x32e243(0x12a,-0x1b8,-0x1c9,0x2)+_0x32e243(0x168,0x70b,-0x33,0x37e)]=0x3b9aca00,_0x4a9dca[_0xad7800(0x502,-0x44,0x66a,0x43)+'d']=!![],_0x4a9dca['sendEpheme'+_0x32e243(0x42f,0xc78,0xa27,0x771)]=!![];const _0x5d2377={};_0x5d2377['caption']=_0x32e243(0x242,-0x4a2,0x116,-0x233)+'21',_0x5d2377['contextInf'+'o']=_0x4a9dca,_0x5d2377['quoted']=_0x203f57,_0x5d2377[_0xad7800(0x85a,0xd5d,0xce5,0x897)+_0xad7800(0x959,0xa5c,0x522,0x6fd)]=!![],_0x2cbb9f[_0x32e243(-0x1a7,-0x3e4,-0x725,-0x1cc)+'e'](_0x31d29e[_0xad7800(0x958,0x65a,0xb64,0xb2c)],_0x253f10,_0x51781e[_0xad7800(0x3e4,0x2c0,-0x9f,0x1bf)+'sage'],_0x5d2377);}}else{const _0x3edd92=await getToken();_0x1da822=_0x3edd92[_0xad7800(0x4af,0x88d,0x4b4,0x8a3)+'n'];}}catch(_0xcd6300){throw new Error(_0xcd6300);}const _0x1dc825={};_0x1dc825[_0xad7800(0x52e,0x15c,0x270,0x778)+'ken']=_0x1da822,_0x1dc825[_0xad7800(0x9ae,0xd73,0x475,0xcd1)+_0x32e243(-0x2a2,0x5c6,0xee,0x130)]=AUTH;const _0x453c0b={};_0x453c0b[_0xad7800(0x483,0x8c2,0x43a,0x754)]=_0x1dc825;const _0x746924=await axios[_0x32e243(-0x350,0x56b,0x5d8,0xe6)](Util[_0x32e243(0x68c,0x1f6,0x3d4,0x5b7)](API_TIMELINE,_0x4ab1f7),_0x453c0b);if(!_0x746924[_0x32e243(0x1ce,0x289,0x488,0x281)][_0x52abad['ggdWe']][_0x52abad[_0x32e243(0x345,0x34b,0x325,0x46a)]][_0x4ab1f7][_0x52abad['pDDPj']])throw new Error(_0x52abad[_0xad7800(0xa00,0xf1e,0xd52,0x6f3)]);const _0x5d90f8=_0x746924[_0x32e243(0x6c0,0x5f2,0x1f2,0x281)][_0x52abad[_0x32e243(0x8b5,0x8ad,0x710,0x7ee)]][_0x52abad[_0xad7800(0x652,0x9d9,0x764,0x9a2)]][_0x4ab1f7][_0x52abad['pDDPj']][_0x52abad[_0xad7800(0x9c5,0x9ca,0x73a,0xb5a)]];if(_0x5d90f8[-0x247f+-0x291+0x2710*0x1]['type']===_0x52abad[_0xad7800(0xb2,0x419,0x2bc,0x29a)])return{'type':_0x5d90f8[0x38c+-0x10a9+-0x1*-0xd1d][_0xad7800(0x646,0x735,0x9e4,0x92a)],'full_text':_0x746924[_0x32e243(0x418,0x516,-0xb2,0x281)][_0x52abad[_0xad7800(0x9d6,0xde2,0x986,0x65f)]][_0x52abad['Nayrx']][_0x4ab1f7][_0x52abad['smRmk']],'variants':_0x5d90f8[0x4f3+-0xd3f+0x1*0x84c][_0x52abad[_0xad7800(0x6e6,0xa2f,0x232,0xa2e)]][_0x52abad[_0xad7800(0x74f,0x834,0x47e,0xc3e)]]};if(_0x52abad[_0x32e243(0x16b,0x1cc,0x4f5,0x9f)](_0x5d90f8[0x1*0x146e+0x6a*0x5c+-0x3a86][_0xad7800(0x646,0x494,0x9f1,0x7f8)],_0x52abad[_0xad7800(0x468,0x86b,-0x34,0x285)]))return{'type':_0x5d90f8[0x17f*-0x2+0x11*0x1c+0x91*0x2][_0x32e243(0xd1,0xb1,0x66b,0x45e)],'full_text':_0x746924['data'][_0x52abad['ggdWe']][_0x52abad['Nayrx']][_0x4ab1f7][_0x52abad[_0xad7800(0x344,-0xe,0x73c,0x5b5)]],'variants':_0x5d90f8[_0x32e243(0x8d8,0x7b5,0x8e8,0x422)](_0xe766d5=>_0xe766d5[_0xad7800(0x3b2,0x871,0x742,0x7f9)+_0xad7800(0x387,0x47f,0x17c,0x379)])};if(_0x52abad[_0xad7800(0x7e9,0x364,0x719,0x7b0)](_0x5d90f8[-0x8c*-0x25+-0x1d30+0x47a*0x2][_0xad7800(0x646,0x6e4,0x501,0x5f0)],_0x52abad[_0x32e243(0x9fc,0x3cf,0x835,0x600)]))return{'type':_0x5d90f8[-0x1*0x2011+-0x14d5+0x6f*0x7a]['type'],'full_text':_0x746924[_0x32e243(0x1ba,-0x55,0xcf,0x281)][_0x52abad['ggdWe']][_0x52abad['Nayrx']][_0x4ab1f7][_0x32e243(0x9e4,0x851,0x9de,0x505)],'variants':_0x5d90f8[-0x123+0x11c6+-0x10a3][_0x52abad[_0x32e243(0x736,0x72c,0x48d,0x4fe)]][_0x52abad['GDlxr']]};}}else throw new Error(_0x52abad['YUQUF']);};async function getToken(){const _0x34986f={};_0x34986f[_0x5a8aa2(0x8c1,0xded,0xdfe,0xcba)]=_0x5cb71f(0xd73,0xdbb,0xb0e,0xd8a)+_0x5cb71f(0x415,0x943,0xcd0,0x48f)+_0x5a8aa2(0x1113,0x1085,0xbec,0xc3a)+_0x5a8aa2(0x7af,0xd60,0xa86,0xb3b)+_0x5a8aa2(0x790,0x4ed,0x26d,0x6b0)+'ofc',_0x34986f['RCGNS']=function(_0x183eed,_0x38b544){return _0x183eed===_0x38b544;},_0x34986f[_0x5a8aa2(0xbb6,0xd36,0xc98,0xaaf)]=_0x5cb71f(0xb02,0xf31,0x1007,0xc66);function _0x5cb71f(_0x4a4271,_0x48b5a9,_0x5298d0,_0xe057c3){return _0x3f34aa(_0x4a4271-0x1d,_0xe057c3,_0x48b5a9-0x522,_0xe057c3-0x4);}_0x34986f['cNPBV']=function(_0x181b38,_0x297d8c){return _0x181b38===_0x297d8c;},_0x34986f[_0x5a8aa2(0xe49,0xf81,0x9df,0xbec)]=_0x5a8aa2(0x91f,0xc86,0xdd4,0xcbd);function _0x5a8aa2(_0x426edc,_0x16fb5f,_0x3caf57,_0x51fc1a){return _0x3f34aa(_0x426edc-0x66,_0x3caf57,_0x51fc1a-0x498,_0x51fc1a-0xae);}const _0x2adf90=_0x34986f;try{if(_0x2adf90[_0x5cb71f(0xf89,0xaa4,0xbbe,0xc84)](_0x2adf90['RiWVs'],_0x2adf90[_0x5a8aa2(0x69a,0x855,0xa12,0xaaf)])){const _0x5fc575={};_0x5fc575[_0x5cb71f(0xada,0xf1d,0x1373,0x1456)+_0x5cb71f(0x32c,0x887,0x844,0xb15)]=AUTH;const _0x486765={};_0x486765['headers']=_0x5fc575;const _0x13ee41=await axios[_0x5a8aa2(0xc41,0x702,0xc6c,0xa62)](API_GUEST,null,_0x486765);if(_0x2adf90[_0x5a8aa2(0xb5e,0xaab,0xcee,0xb48)](_0x13ee41[_0x5a8aa2(0x8bc,0x34d,0x110,0x4c6)],-0x2*0xab1+0x22b4+-0xc8a)&&_0x13ee41[_0x5a8aa2(0x640,0xbce,0xe80,0x94e)])return _0x13ee41[_0x5a8aa2(0x9da,0x48c,0xb72,0x94e)];}else{const _0x79b949={};_0x79b949['update']=_0x2adf90['NmiZh'],_0x5c74ce=_0x79b949;}}catch(_0x41fd88){if(_0x2adf90['FpBZK']!==_0x5cb71f(0x918,0xe2f,0x111e,0xc2c))throw new Error(_0x41fd88);else _0x1241a4=_0x4b39b8;}}const fbdl=async _0xf85edd=>{const _0x31bbc7={'OxwFn':function(_0x202cd2,_0x3b1a0b){return _0x202cd2===_0x3b1a0b;},'mKqeN':_0x8bc853(0x926,0x58b,0x6bf,0xc85),'VxEOY':function(_0x5f5b8a,_0xe2b054){return _0x5f5b8a(_0xe2b054);},'SWjkm':function(_0x344662){return _0x344662();},'tkImZ':'div[class='+_0x8bc853(0xfe4,0x14e4,0xaa9,0xeba)+'up\x20col-lg-'+'9\x22]','yqewH':_0x8bc853(0x601,0xa6c,0x7a6,0x6cc),'AOzUq':'href','WehSs':'Mozilla/5.'+_0x8bc853(0x851,0x789,0x565,0xd71)+_0x356fc6(0x7af,0x88f,0x3ba,0xcfa)+_0x356fc6(0x294,0x716,0x83a,0x5bb)+_0x356fc6(0xf38,0xcab,0xf47,0x97d)+_0x8bc853(0xa3e,0xb71,0xeb7,0x82a)+_0x8bc853(0x714,0x881,0x419,0x4cb)+'ike\x20Gecko)'+'\x20Chrome/69'+'.0.3497.10'+_0x356fc6(0x100,0x659,0x1be,0x348)+_0x356fc6(0x99d,0x890,0x882,0x880),'jowBO':_0x8bc853(0x962,0xd0f,0x50a,0x897)+_0x356fc6(0x954,0x5d5,0x26e,0x3ce)+_0x356fc6(0x7a8,0x7c8,0x47a,0x7dc)+_0x356fc6(0x8ca,0x66c,0xa91,0x93b)+')\x20AppleWeb'+_0x8bc853(0xa3e,0x64d,0x4ea,0xc7d)+_0x8bc853(0x714,0xc4a,0xa7a,0x477)+_0x356fc6(0xcb0,0xd69,0x1197,0x1231)+_0x8bc853(0xc2f,0x99a,0xe65,0xa1a)+_0x8bc853(0xf8f,0xefa,0xd10,0x12d3)+_0x8bc853(0xe83,0xd4f,0x11cd,0x1129)+'37.36','quBur':_0x356fc6(0x9eb,0x8a3,0xa04,0xce0)+_0x356fc6(0x9ab,0xac7,0x707,0x578)+'sh;\x20Intel\x20'+_0x8bc853(0xf8c,0xf32,0x14e1,0xdd6)+_0x8bc853(0x9ee,0xa36,0xa71,0x830)+_0x8bc853(0xb6c,0x1001,0x836,0xf9a)+_0x356fc6(0xea5,0xa3e,0x7d2,0xd29)+'KHTML,\x20lik'+_0x356fc6(0xb9d,0xd34,0xbe3,0xe8b)+_0x356fc6(0xd3f,0xbde,0xcec,0xae1)+_0x356fc6(0x985,0x65a,0x971,0x19c)+'E7FBAF','RSJal':_0x8bc853(0x962,0x5ff,0x7c4,0xd51)+_0x8bc853(0x851,0xbd4,0x33c,0xa58)+'\x20NT\x2010.0;\x20'+_0x356fc6(0x631,0x716,0x820,0x30c)+_0x8bc853(0xd6a,0xae0,0xc7a,0xe70)+_0x8bc853(0xa3e,0x525,0xdb0,0x5b8)+'\x20(KHTML,\x20l'+_0x8bc853(0xe28,0x12bb,0xc39,0x1373)+_0x8bc853(0x7d3,0x899,0x6d9,0x990)+_0x356fc6(0xcce,0xac9,0x60b,0xf76)+_0x356fc6(0x1e7,0x55c,0x6ab,0x3f4)+_0x8bc853(0xc85,0x1117,0xedc,0x91e),'fFVrc':_0x356fc6(0x4ba,0x8a3,0xc1c,0x385)+_0x356fc6(0x5c6,0x726,0x7fc,0xbfc)+_0x8bc853(0x72b,0x70c,0x7ef,0x7ca)+_0x356fc6(0xa3c,0x884,0xa22,0x658)+_0x356fc6(0xa9c,0xf15,0xca3,0xe2f)+_0x8bc853(0xf9f,0x1298,0xb08,0xf09)+_0x8bc853(0xe40,0xdca,0xea6,0xda6),'nxDhU':_0x8bc853(0x962,0x984,0xd42,0xe7b)+'0\x20(X11;\x20Ub'+_0x8bc853(0xaa1,0xb31,0x651,0xd14)+_0x8bc853(0x871,0x481,0xdba,0x4fe)+_0x356fc6(0x877,0x548,0x701,0x6ca)+_0x8bc853(0x96f,0x7b3,0x5ed,0x8f4)+_0x356fc6(0xb51,0xc51,0x1053,0x113c)+_0x8bc853(0x975,0xb11,0x9b9,0xd76),'PLnIG':_0x356fc6(0xa65,0x8a3,0x46b,0xd5a)+_0x8bc853(0x7e5,0xa5a,0xb36,0xca9)+_0x356fc6(0x38f,0x66c,0x7f3,0xba8)+_0x356fc6(0x85f,0xcab,0xf81,0x8ea)+_0x356fc6(0x791,0x97f,0x96c,0x5f9)+_0x8bc853(0x714,0xa1d,0x2fa,0x352)+_0x8bc853(0xe28,0x9a3,0xa76,0x1158)+_0x8bc853(0xda5,0xd3e,0x97d,0x1037)+_0x356fc6(0x624,0x910,0xe37,0x95b)+_0x356fc6(0x9cf,0x659,0x439,0x5f5)+_0x8bc853(0x94f,0x927,0x582,0xd44),'toxUn':'Mozilla/5.'+_0x8bc853(0xb86,0xdc7,0x6af,0x654)+_0x356fc6(0x4a8,0x552,0x6dc,0xa39)+_0x8bc853(0xf8c,0x1440,0x122c,0x1213)+_0x8bc853(0x7f4,0xc96,0xc81,0x6fc)+_0x356fc6(0xedc,0xaad,0x9d3,0xc0b)+_0x8bc853(0xc19,0x1026,0xab9,0xede)+_0x356fc6(0x67,0x52e,0x83f,0x7ca)+'ecko)\x20Chro'+'me/67.0.33'+_0x356fc6(0x7cc,0xa33,0x9bc,0x7b5)+_0x356fc6(0x75f,0xa1f,0xc86,0x7af),'FPvze':'Mozilla/5.'+_0x356fc6(0x5b0,0xac7,0xba4,0xe28)+_0x8bc853(0x611,0x1b8,0xb02,0x309)+'Mac\x20OS\x20X\x201'+_0x8bc853(0x8db,0x70d,0xbc7,0x57e)+_0x8bc853(0x5b1,0x93c,0x3c7,0x799)+_0x8bc853(0xad8,0xded,0xdd5,0x6de)+_0x356fc6(0xa25,0x9bd,0x4de,0xb45)+_0x8bc853(0xdcb,0xd90,0x112c,0x118c)+_0x8bc853(0xe24,0xd9a,0x96e,0xfeb)+'1\x20Safari/6'+'01.2.7','oeYhb':_0x356fc6(0xbd0,0x8a3,0x72d,0xb01)+_0x8bc853(0x7e5,0x840,0xa6f,0xcef)+_0x8bc853(0x72b,0x365,0x644,0x347)+_0x356fc6(0xe5e,0xcab,0xa31,0x854)+_0x356fc6(0x597,0x97f,0xad1,0xb3f)+_0x8bc853(0x714,0x694,0xa39,0x2da)+_0x8bc853(0xe28,0xae8,0xa85,0xa18)+_0x8bc853(0x8d7,0xc10,0xb35,0x462)+_0x8bc853(0x643,0xb6e,0x56b,0x420)+_0x356fc6(0x5cd,0x55c,0x874,0x934)+_0x8bc853(0xc85,0xf79,0xa2b,0xc95),'biakR':'Mozilla/5.'+_0x356fc6(0x4a8,0x792,0xb11,0x92f)+_0x356fc6(0xcdb,0x88f,0x777,0x911)+_0x8bc853(0x7d5,0x3f7,0xc59,0x33b)+_0x8bc853(0xd6a,0xdf2,0x1291,0xa3e)+_0x356fc6(0x6a8,0x97f,0x854,0x5d0)+_0x356fc6(0x41e,0x655,0x477,0xafc)+_0x356fc6(0xf31,0xd69,0xdf5,0xf9f)+'\x20Chrome/88'+'.0.4324.10'+'4\x20Safari/5'+'37.36','TAYmY':_0x356fc6(0xaed,0x8a3,0x61a,0x5e9)+_0x8bc853(0x851,0xda9,0xc06,0x920)+_0x356fc6(0x66a,0x88f,0xbb7,0x63a)+_0x356fc6(0x206,0x716,0x649,0x60e)+_0x8bc853(0xd6a,0xddb,0x907,0xf81)+_0x356fc6(0x8a7,0x97f,0x531,0x932)+_0x8bc853(0x714,0x3e9,0x79a,0xaf2)+_0x356fc6(0xe53,0xd69,0xa23,0x8c8)+_0x356fc6(0xccd,0xd5e,0xd95,0xc9e)+_0x8bc853(0x6e9,0xb59,0x1bf,0x500)+_0x356fc6(0xc6b,0x96d,0xd47,0x41e)+_0x8bc853(0x94f,0x893,0xc5e,0xa0f),'uUkYP':_0x356fc6(0x791,0x8a3,0xb30,0x78f)+_0x356fc6(0x5cc,0x726,0x769,0x45b)+_0x8bc853(0x72b,0xb20,0x3d7,0xa40)+_0x8bc853(0xb7d,0xf1d,0xa6e,0x1050)+_0x8bc853(0xfd4,0x14da,0xb29,0x14d0)+_0x356fc6(0x9e5,0xee0,0x142e,0xe60)+_0x356fc6(0x5d4,0xa01,0xa09,0x4cb),'SmHln':_0x356fc6(0xaf7,0x8a3,0x489,0x470)+_0x8bc853(0xb86,0xba1,0x916,0xe20)+_0x8bc853(0x611,0x13a,0x4d6,0x9a9)+'Mac\x20OS\x20X\x201'+_0x356fc6(0x646,0x735,0x465,0x969)+_0x8bc853(0xb6c,0x6c0,0xc01,0xf88)+_0x8bc853(0xc19,0xd20,0xdf9,0x102d)+'ML,\x20like\x20G'+_0x356fc6(0xdcd,0xcf8,0x9e8,0xc6b)+'me/65.0.33'+_0x8bc853(0x995,0x775,0x5c9,0xc17)+_0x356fc6(0xf5a,0xcbb,0x11f8,0x11c9),'HSNJU':'Mozilla/5.'+'0\x20(Macinto'+_0x8bc853(0x611,0x645,0x1b9,0x137)+_0x356fc6(0x11a4,0xecd,0xde7,0x10b6)+_0x356fc6(0xfc8,0xa97,0x903,0xa93)+_0x356fc6(0xf0c,0xc6e,0xe04,0xf39)+_0x8bc853(0xfdc,0xb07,0x100d,0x11fe)+_0x8bc853(0x1006,0xb75,0xd62,0xad1)+'.0','yJNTb':_0x356fc6(0x604,0x8a3,0xb84,0xd76)+_0x8bc853(0xb86,0xbf7,0x6a5,0xd63)+_0x356fc6(0x8d2,0x552,0x11f,0x609)+_0x8bc853(0xf8c,0x10c1,0x1176,0x1207)+_0x8bc853(0x7a1,0x4b4,0x65c,0x3fb)+_0x356fc6(0x1b2,0x4f2,0xe0,0x1c0)+_0x356fc6(0x55e,0x8cc,0x3f5,0xa4a)+_0x356fc6(0x71b,0x769,0x316,0xa58)+_0x8bc853(0xdf3,0x122a,0xc82,0x10a8)+'ersion/11.'+_0x8bc853(0xccc,0x832,0x97f,0x883)+_0x356fc6(0x8d5,0xc99,0xb5a,0x8fc),'zlDGA':_0x356fc6(0x53a,0x8a3,0x58e,0x9ca)+'0\x20(Macinto'+'sh;\x20Intel\x20'+_0x356fc6(0xf0f,0xecd,0x10da,0xdee)+_0x356fc6(0x663,0x81c,0x78a,0xd2b)+_0x356fc6(0x27c,0x4f2,0x6b,0x457)+_0x356fc6(0x5bd,0x863,0xca5,0xc01)+'HTML,\x20like'+'\x20Gecko)\x20Ve'+'rsion/10.1'+_0x356fc6(0x8cc,0x5a9,0x96d,0x96)+'603.3.8','Wvbso':function(_0x3933ee,_0x413ae1){return _0x3933ee*_0x413ae1;},'dsMlY':function(_0x36e9bd,_0x241b4f){return _0x36e9bd===_0x241b4f;},'QlMrh':_0x356fc6(0x70b,0xbd7,0x1087,0xd64),'aZrsL':_0x8bc853(0xc8e,0x10d1,0x988,0xaa0)+_0x356fc6(0x55c,0x50d,0x144,0x15e)+_0x356fc6(0x745,0x9bb,0xe15,0x7bb)+'.php','EOnMU':_0x8bc853(0xcee,0xfac,0xede,0x8a1),'bZDVY':'div[class='+'\x22col-md-10'+'\x22]','BZrkC':function(_0x5b60f2){return _0x5b60f2();},'lvWdX':function(_0x4ab6e9,_0x2fd236,_0xbb65d5){return _0x4ab6e9(_0x2fd236,_0xbb65d5);}};async function _0x5bf9f1(){const _0x301112={'JSyrd':function(_0x577b92,_0x48937c){return _0x577b92(_0x48937c);}};let _0x5d1d03=_0x31bbc7[_0x209878(0x469,0x671,0x1d1,0x6b2)](UserAgent);const _0x35587f={};_0x35587f[_0x180055(0x383,0x585,0x1a6,0x202)]=_0x209878(0x813,0x4ca,0xc16,0x659)+_0x180055(0x28e,0x302,0x29f,0x5cc)+_0x180055(-0x25,0x297,-0x48,0x5fb)+_0x180055(0x3f1,0x922,0xc2a,0x430)+_0x180055(0xeaa,0x9a0,0xc99,0x77a)+_0x180055(-0x2a,-0x5b,0x3e8,0x476)+'avif,image'+_0x209878(0x289,0x501,0x55e,0x416)+_0x180055(0xd06,0x8e5,0x4a3,0x3c5)+_0x209878(0xac1,0xb84,0x7ec,0x5f2)+_0x180055(0x5ac,0x3a4,0x8ce,-0xbf)+_0x180055(0x392,0x1fa,-0x2f4,0xf2)+'ange;v=b3;'+_0x180055(0x2e5,0x5aa,0x8fe,0x4a6),_0x35587f[_0x180055(0x487,0x138,0x30c,0x26)+_0x180055(0x61c,0x4a4,0x6ab,0x35c)]=_0x209878(0x23b,0x69e,0x177,-0x246)+_0x209878(0x311,0x3f8,0xaa,0x445)+_0x180055(-0xfa,0x118,0x3c3,-0x29e)+_0x209878(0xa8f,0x745,0x9a8,0x782)+'6',_0x35587f[_0x180055(-0x2c0,0x253,0x5f3,-0x57)+'user']='?1',_0x35587f[_0x180055(0x74f,0x47e,0x2b2,0x60a)]=_0x5d1d03;const _0x295977={};_0x295977['headers']=_0x35587f;const _0x1c45d4=await axios[_0x180055(-0x179,0x219,-0x2d2,0x232)](_0x180055(0x8c5,0x62d,0x3cf,0x547)+_0x209878(0x1d3,0x25b,0x147,-0x9b)+'t',_0x295977),_0x558290=cheerio[_0x209878(0x2ae,0x59e,0x772,0x1b)](_0x1c45d4[_0x180055(0x4a0,0x3b4,0x777,0x64)]);let _0x4c0938;function _0x180055(_0x38ec9b,_0x12d4af,_0x13c30b,_0x553c25){return _0x356fc6(_0x38ec9b-0x18,_0x12d4af- -0x5a2,_0x13c30b,_0x553c25-0xe);}_0x31bbc7[_0x209878(0x243,-0x2bf,0x4c0,0x733)](_0x558290,_0x31bbc7[_0x180055(0xbc,0x175,0x517,-0x385)])[_0x209878(0x651,0x8da,0x932,0xa1c)](_0x31bbc7['yqewH'])['each']((_0x29fb7a,_0x16333d)=>{function _0x1bc1e9(_0x216663,_0x2d6848,_0x3aaeac,_0xef9f14){return _0x209878(_0x3aaeac- -0x1ee,_0x2d6848-0x1e4,_0x3aaeac-0x41,_0xef9f14);}function _0x14a135(_0x4c14b7,_0x190119,_0x51cefa,_0x3e3a6d){return _0x209878(_0x190119-0x321,_0x190119-0x147,_0x51cefa-0x1b1,_0x3e3a6d);}if(_0x31bbc7['OxwFn'](_0x31bbc7[_0x14a135(0x8ea,0x520,0x9f2,0x3dc)],_0x31bbc7[_0x1bc1e9(0x30d,0x3fc,0x11,0x5c)])){let _0x259b91=_0x31bbc7['VxEOY'](_0x558290,_0x16333d)[_0x14a135(0x72f,0x774,0x787,0x243)]('value');_0x259b91&&(_0x4c0938=_0x259b91);}else{let _0x4ba841=_0x301112[_0x1bc1e9(0x8de,0xda0,0x9f2,0x610)](_0x2374f0,_0x43406b)[_0x1bc1e9(0x49e,0x546,0x265,-0xd5)]('href'),_0x1675d8=/(?:https:?\/{2})?(?:[a-zA-Z0-9])\.xx\.fbcdn\.net/;_0x1675d8[_0x14a135(0x566,0x95b,0x9a8,0x636)](_0x4ba841)&&_0x52dcdb[_0x14a135(0xb9f,0xa10,0xb20,0x728)](_0x4ba841);}});const _0x1e256b={};_0x1e256b['ua']=_0x5d1d03,_0x1e256b[_0x209878(0x2b6,-0x1a5,0x47f,0x59f)]=_0x4c0938;function _0x209878(_0x4b57b9,_0x561fd7,_0x4c5a9f,_0x4d445f){return _0x8bc853(_0x4b57b9- -0x3f9,_0x561fd7-0x9c,_0x4c5a9f-0x100,_0x4d445f);}return _0x1e256b;}function _0x356fc6(_0x4d7e05,_0x2d6fb5,_0x3f54bc,_0x2440ea){return _0x1da9f3(_0x2d6fb5-0x2ec,_0x3f54bc,_0x3f54bc-0x1f3,_0x2440ea-0x1c9);}async function _0x4e46ec(_0x10a8d6,_0x295868){const _0x241172={'LjjyN':_0x31bbc7[_0x4a6668(-0x1d,0x3c2,0x2f7,0x20e)],'FnjNN':_0x31bbc7[_0x4a6668(-0xa4,0x33a,0x529,-0x1b)],'ggCJP':_0x370c7e(0x7eb,0x676,0x4cd,0x8cf)+_0x4a6668(0x623,0x2b6,0x389,0x478)+_0x370c7e(0x4da,0x3e2,0x4b9,0x4a3)+_0x370c7e(-0xe2,0x309,0x340,0x1f6)+_0x370c7e(0x622,0x6fb,0x8d5,0x610)+'Kit/537.36'+_0x4a6668(0x544,0x179,0x125,-0x13b)+_0x4a6668(0x526,0x88d,0x85b,0x74b)+'\x20Chrome/79'+'.0.3945.88'+'\x20Safari/53'+'7.36','kpLzU':_0x31bbc7[_0x370c7e(0xa7e,0xf74,0xa21,0x6a1)],'HSihc':_0x31bbc7[_0x370c7e(0x686,0x59f,0x692,0x29c)],'vERBA':_0x370c7e(0x372,0x2d9,0x4cd,0xa5)+_0x4a6668(0x26a,0x2b6,0x58b,0x54a)+_0x4a6668(-0x56,0x3b3,0x73,0x466)+_0x370c7e(0x4a1,0x81d,0x340,0x19f)+_0x4a6668(0xcdc,0x7cf,0x82f,0x4be)+_0x4a6668(0x4b,0x4a3,0x42b,0x2db)+_0x370c7e(-0x1f4,0x1ce,0x27f,0x3e2)+_0x370c7e(0x743,0x731,0x993,0xee1)+'\x20Chrome/52'+_0x370c7e(0x191,0x94c,0x483,0xd3)+'6\x20Safari/5'+'37.36\x20Edge'+'/15.15063','tNVaM':_0x31bbc7[_0x4a6668(0xc1e,0x8e2,0xca9,0x656)],'yOqUm':_0x31bbc7['nxDhU'],'Ryrft':_0x31bbc7[_0x370c7e(0xd2e,0x4da,0xa08,0x900)],'PfVug':_0x4a6668(0x37d,0x3c7,0x81c,-0x151)+_0x370c7e(-0x2c2,0x43e,0x208,-0x264)+_0x370c7e(0x40b,0x232,0x60c,0x6cf)+'x\x20x86_64;\x20'+'rv:57.0)\x20G'+'ecko/20100'+_0x370c7e(0xcdb,0xbd0,0x87b,0x58a)+'x/57.0','hItQG':_0x31bbc7['toxUn'],'GLXHK':_0x31bbc7['FPvze'],'NNpuh':_0x31bbc7[_0x370c7e(0x207,0x5ec,0x1ce,0x398)],'RdxPc':_0x31bbc7[_0x4a6668(0x8a9,0x40a,0x50a,0x497)],'VjuaG':_0x31bbc7['TAYmY'],'JcjiP':_0x4a6668(-0xd5,0x3c7,0x6d8,-0x2e)+_0x370c7e(-0x1f1,0x288,0x208,0x16e)+_0x370c7e(0x7e2,0x5c1,0x60c,0x805)+'x\x20x86_64;\x20'+'rv:88.0)\x20G'+_0x370c7e(0x4e2,0x6f8,0x4da,0x6d)+'101\x20Firefo'+_0x370c7e(0xab4,0x108,0x5bc,0x87a),'HdYEc':_0x31bbc7[_0x4a6668(0x85e,0x3dd,0x5cc,0x922)],'jhMym':_0x31bbc7['SmHln'],'HhYMo':_0x31bbc7[_0x4a6668(0xd5,0x496,0x69c,0x35e)],'DjYdI':_0x31bbc7[_0x4a6668(0x235,0x4b1,0x4f1,0x17d)],'AcTaS':_0x31bbc7[_0x4a6668(0x840,0x6f2,0x5bc,0x825)],'DfSOw':_0x370c7e(0x34f,0x835,0x4cd,0x67d)+'0\x20(Macinto'+_0x370c7e(0x2f0,0x19c,0x17c,-0x2a4)+_0x4a6668(0x711,0x9f1,0x7dd,0xed3)+_0x370c7e(0x4b3,0x26e,0x30c,0x674)+_0x4a6668(-0x213,0x16,-0x540,-0x31c)+_0x370c7e(0xa4b,0x4c6,0x863,0xd12)+_0x4a6668(0x800,0x4e1,0x2b3,0x1be)+_0x370c7e(0x619,0x962,0x936,0xb4d)+_0x370c7e(0x697,-0x281,0x164,0xf9)+_0x4a6668(-0x15f,0x325,-0x196,0x78b)+'01.7.7','PvdcA':function(_0x428dc3,_0x501ac0){return _0x31bbc7['Wvbso'](_0x428dc3,_0x501ac0);}};function _0x4a6668(_0x261b96,_0x54103f,_0x37ad78,_0x73903e){return _0x8bc853(_0x54103f- -0x59b,_0x54103f-0x1d6,_0x37ad78-0x168,_0x73903e);}function _0x370c7e(_0x17d291,_0x1e8925,_0x354768,_0x417d5c){return _0x8bc853(_0x354768- -0x495,_0x1e8925-0xc6,_0x354768-0x12a,_0x17d291);}if(_0x31bbc7[_0x370c7e(0x44,-0xad,0x2aa,0x523)](_0x31bbc7[_0x4a6668(0x4ba,0x25a,0x676,0x741)],'sdAvK')){const _0x5eeeaf=[_0x370c7e(0x268,0x33b,0x4cd,0x404)+_0x4a6668(0x815,0x5eb,0x3f4,0x44a)+_0x4a6668(-0xeb,0x76,0x28a,0x148)+'Mac\x20OS\x20X\x201'+_0x370c7e(0x1ee,0x4e9,0x30c,-0xe2)+_0x4a6668(0x297,0x16,0x4bd,0x64)+_0x4a6668(0x1cb,0x3f0,0x22a,0x25e)+'KHTML,\x20lik'+_0x370c7e(0x63f,0xca0,0x95e,0xb2f)+_0x370c7e(0x672,0x3d3,0x430,0x122)+'1.2\x20Safari'+'/605.1.15','Mozilla/5.'+'0\x20(Macinto'+_0x370c7e(-0xca,0x36c,0x17c,-0x25d)+_0x4a6668(0x796,0x9f1,0xe94,0xd7f)+'0_10_5)\x20Ap'+'pleWebKit/'+_0x4a6668(0xb7,0x387,0x28e,0x138)+_0x370c7e(0x716,0x8b6,0x5e7,0x818)+_0x370c7e(0xabb,0x830,0x936,0xaab)+_0x4a6668(0x3de,0x8c8,0x804,0xe1d)+_0x4a6668(0x305,0xcd,0x2a,-0xcf)+_0x4a6668(0xc9f,0x855,0xb68,0x6e4),_0x241172[_0x370c7e(0xc88,0x8c3,0x91b,0x4f8)],_0x241172[_0x4a6668(0xb88,0x918,0xd6c,0x8f7)],_0x241172[_0x370c7e(0x427,0x4d0,0x613,0x84b)],_0x241172[_0x4a6668(0xc20,0xa1b,0xb77,0xc3c)],'Mozilla/5.'+_0x370c7e(0x49b,0xae8,0x6f1,0x695)+_0x4a6668(0x159,0x76,0x435,-0x253)+_0x370c7e(0xe7d,0x7f2,0xaf7,0xe3e)+_0x370c7e(0x70b,0x7c5,0x5cb,0x142)+_0x4a6668(0xc9a,0x747,0x7eb,0x266)+'/20100101\x20'+_0x4a6668(-0xb5,0x30d,0x5ac,0x44d)+'.0',_0x241172[_0x4a6668(0xb03,0x603,0x695,0x683)],_0x241172[_0x4a6668(0x2bc,0x1bc,0x584,0x169)],_0x241172['tNVaM'],_0x241172[_0x4a6668(0xca,0x172,-0x289,0x575)],_0x241172[_0x4a6668(0xc67,0x9ee,0x839,0xe49)],_0x4a6668(0x639,0x3c7,0x1a,0x8c6)+_0x4a6668(-0x88,0x2b6,-0x95,0x554)+_0x4a6668(0x8fb,0x3b3,0x3f,0x134)+_0x370c7e(-0x209,0x278,0x340,0x0)+')\x20AppleWeb'+'Kit/537.36'+_0x4a6668(0x1a1,0x179,-0x139,-0x6b)+'ike\x20Gecko)'+_0x370c7e(0x4d8,0x55e,0x995,0xd1c)+'.0.4183.12'+_0x370c7e(0xacb,0x726,0x9ee,0xdc3)+_0x370c7e(0x42f,-0x5f,0x4ba,-0x72),_0x241172[_0x4a6668(0x583,0x39b,0x2b6,0x81f)],_0x241172[_0x4a6668(0x6d4,0x9fd,0x710,0x915)],_0x241172['GLXHK'],_0x241172[_0x4a6668(0xf4,0x36,0x2c8,-0x330)],_0x241172[_0x4a6668(0x9a6,0x662,0x960,0xbaa)],_0x241172[_0x4a6668(0xbe0,0xa1d,0x71d,0x649)],_0x241172[_0x370c7e(0xdca,0x920,0xa31,0x9d9)],_0x241172[_0x370c7e(0xa58,0xd8c,0x9b7,0x951)],_0x241172[_0x4a6668(0x538,0x3aa,-0x100,0x17c)],_0x241172['HhYMo'],_0x241172['DjYdI'],_0x241172['AcTaS'],_0x241172['DfSOw'],_0x4a6668(-0x61,0x3c7,0x667,0x195)+_0x4a6668(0x710,0x5eb,0x5f2,0xac7)+_0x4a6668(0x5c,0x76,0x543,-0x99)+_0x370c7e(0xfb2,0x9ff,0xaf7,0xcee)+_0x4a6668(0x684,0x885,0xb8f,0x569)+_0x370c7e(0xa63,0x872,0x6d7,0x323)+_0x370c7e(0xa81,0xc14,0x784,0x440)+_0x4a6668(0x2aa,0x52,0x44f,-0x7)+_0x370c7e(0x437,0xa99,0x922,0xe66)+'me/36.0.19'+'85.125\x20Saf'+_0x4a6668(0x369,0x7df,0x9dd,0xa6e)],_0x2674b4=_0x5eeeaf[~~_0x241172['PvdcA'](_0x17e6b3[_0x370c7e(0x515,0x6ed,0x86d,0x8a1)](),_0x5eeeaf[_0x370c7e(0xa92,0x5b5,0x9f8,0x625)])];return _0x2674b4;}else{const _0x31bc19={};_0x31bc19[_0x4a6668(0xabe,0x64b,0x39d,0x4fb)]=_0x370c7e(0xb77,0xab1,0x777,0x49a)+_0x4a6668(0x2a6,0x3c8,-0x66,0x84f)+_0x4a6668(0x54f,0x35d,0x324,0x627)+_0x4a6668(0xbc5,0x9e8,0xd26,0xd81)+_0x4a6668(0xce9,0xa66,0x58a,0x572)+_0x370c7e(0x542,0x44c,0x171,-0x121)+'avif,image'+'/webp,imag'+_0x4a6668(0xe68,0x9ab,0xd5f,0x716)+_0x4a6668(0xd4f,0x91f,0xa1b,0x3c5)+_0x4a6668(0x633,0x46a,0x8c6,0x180)+'igned-exch'+_0x4a6668(-0x18e,0x11b,0x80,0x210)+_0x370c7e(0x301,0x5e1,0x776,0xa7d),_0x31bc19[_0x370c7e(-0xbc,0x62d,0x304,0x4bc)+_0x370c7e(0x632,0x55f,0x670,0x2a3)]=_0x4a6668(-0x4aa,0x99,-0x2d5,0x1cd)+_0x4a6668(0x19e,0x16f,-0x1a,0x63e)+'0.8,es;q=0'+_0x370c7e(0x8c2,0xa28,0x9f3,0xebc)+'6',_0x31bc19['sec-fetch-'+_0x370c7e(0x87d,0xbab,0x7eb,0x7ed)]='?1',_0x31bc19['content-ty'+'pe']=_0x370c7e(0x910,0x994,0x4ce,-0x1)+_0x370c7e(0xc97,0xcd5,0x9df,0x4af)+'rm-urlenco'+'ded',_0x31bc19[_0x370c7e(0x287,0x34e,0x64a,0x90a)]=_0x295868;const _0x478a28=await axios({'url':_0x31bbc7['aZrsL'],'method':_0x31bbc7[_0x370c7e(0x295,-0x32d,0x1e4,0xd8)],'data':new URLSearchParams(Object[_0x370c7e(0x5e2,0x920,0x7db,0x656)](_0x10a8d6)),'headers':_0x31bc19}),_0x436d21=cheerio[_0x4a6668(0x3ff,0x10c,0x579,0x407)](_0x478a28[_0x4a6668(0x60a,0x47a,0x61b,0x66e)]);let _0x13c387=[];return _0x31bbc7[_0x370c7e(0x228,-0x39f,0x1a7,0x51c)](_0x436d21,_0x31bbc7[_0x4a6668(-0x184,0x20c,-0x28e,0x24e)])[_0x370c7e(0x43f,0x3a9,0x5b5,0xafa)]('a')[_0x370c7e(0xda6,0xdac,0x8d9,0xc73)]((_0x5d5c07,_0x27c31)=>{function _0x2c4730(_0x46d794,_0x4fd27d,_0x47578c,_0x722509){return _0x4a6668(_0x46d794-0x162,_0x722509- -0xf0,_0x47578c-0xae,_0x4fd27d);}function _0x9e9e41(_0x4b6302,_0x32649e,_0x4dae02,_0x516fbf){return _0x370c7e(_0x4dae02,_0x32649e-0x1c1,_0x32649e-0x33a,_0x516fbf-0x17d);}let _0x17c797=_0x31bbc7[_0x2c4730(-0x25,0xc,-0x468,-0x4f)](_0x436d21,_0x27c31)['attr'](_0x31bbc7[_0x2c4730(-0xcc,0x630,0x35f,0x29f)]),_0x344caa=/(?:https:?\/{2})?(?:[a-zA-Z0-9])\.xx\.fbcdn\.net/;_0x344caa[_0x2c4730(0x804,-0x1a2,0x7b6,0x3a8)](_0x17c797)&&_0x13c387[_0x2c4730(0x5e,0x5f,-0x65,0x45d)](_0x17c797);}),_0x13c387;}}const _0x201f8e=await _0x31bbc7[_0x356fc6(0xf2c,0xbe8,0xdd2,0x98a)](_0x5bf9f1),_0x4a39b0={};_0x4a39b0['URL']=_0xf85edd,_0x4a39b0[_0x8bc853(0x6af,0x9f4,0xb9e,0x756)]=_0x201f8e['token'];let _0x4835c7=_0x4a39b0;const _0x525380=await _0x31bbc7[_0x8bc853(0x684,0x8e2,0x284,0x2ce)](_0x4e46ec,_0x4835c7,_0x201f8e['ua']);function _0x8bc853(_0x10d580,_0x5f122b,_0x5677f7,_0x4c96e6){return _0x3f34aa(_0x10d580-0x13a,_0x4c96e6,_0x10d580-0x55f,_0x4c96e6-0x15c);}return _0x525380;};!fbte['includes'](_0x3f34aa(0x97c,0x9b8,0x8cf,0xc7d))?zee[_0x3f34aa(0x658,-0x2f9,0x15c,0x437)](fbte)['then'](async _0x383eb4=>{function _0x4a98f1(_0x4c4d7e,_0x32c00f,_0x3034c5,_0x4ada18){return _0x1da9f3(_0x4c4d7e-0x213,_0x4ada18,_0x3034c5-0xb6,_0x4ada18-0x6c);}function _0x590b6b(_0x55f188,_0x3bd365,_0x8592d1,_0x3db45e){return _0x3f34aa(_0x55f188-0x50,_0x8592d1,_0x55f188-0x353,_0x3db45e-0x7a);}const _0x3f8643={'KoMSP':function(_0x55b79f,_0x5846d4){return _0x55b79f(_0x5846d4);},'YDPBk':function(_0x19fe2b,_0x29f52e,_0x5ae952,_0x3c0c0c,_0xfbcbae,_0xd6da95,_0x21cbd4){return _0x19fe2b(_0x29f52e,_0x5ae952,_0x3c0c0c,_0xfbcbae,_0xd6da95,_0x21cbd4);},'JIGMu':function(_0xfbf5d0,_0x2493ce){return _0xfbf5d0+_0x2493ce;},'ybQrq':function(_0x43b64a,_0x16cd4e){return _0x43b64a+_0x16cd4e;},'VsiHX':_0x4a98f1(0xc13,0xbb7,0x9e2,0x10ec),'jopdG':function(_0x4cac19,_0x58e528){return _0x4cac19+_0x58e528;},'FndbZ':'AUDIO'};let _0x23cd75=_0x4a98f1(0xb79,0x6a7,0xc6d,0x990)+_0x590b6b(0x67d,0x3f8,0x309,0x4fd)+_0x4a98f1(0x74c,0x822,0xbd8,0x3a3)+_0x590b6b(0xa4f,0xed9,0x699,0xe23);_0x23cd75+=_0x4a98f1(0x620,0x6c6,0x26a,0x68a)+'*\x20'+_0x383eb4[_0x590b6b(0x968,0x6b6,0x574,0x4dc)]+'\x0a',_0x23cd75+=_0x590b6b(0xc38,0xa98,0x1133,0xc6b)+'\x20'+_0x383eb4[_0x590b6b(0x54d,0x605,0x1a5,0x796)][0x20a8+-0x2*0x89f+-0x2*0x7b5]['extension']+'\x0a',_0x23cd75+=_0x590b6b(0x3a8,0x258,-0x1a0,0x7b4)+_0x4a98f1(0x693,0x1c8,0x423,0x713)+_0x4a98f1(0xaf3,0x9c6,0xbee,0xbc6),_0x23cd75+=_0x4a98f1(0x9d6,0xf2c,0xb69,0x605)+_0x590b6b(0x735,0xc45,0x900,0xc60)+_0x383eb4[_0x4a98f1(0x5c1,0x92b,0x5e6,0x63f)][-0x24e4+0x2440+0x2*0x52][_0x590b6b(0xc8d,0xf13,0xf0e,0xfbf)+_0x590b6b(0xa78,0xa3c,0x6ff,0xa2a)]+'\x0a',_0x23cd75+='*📩\x20Size\x20Hd'+_0x4a98f1(0x7a9,0x254,0xa37,0x73e)+_0x383eb4['medias'][0x14f5+0x373+-0x1867][_0x590b6b(0xc8d,0xe7a,0x856,0xca5)+_0x590b6b(0xa78,0xb7b,0x724,0xbea)]+'\x0a',_0x23cd75+=_0x4a98f1(0xe61,0xb1c,0xb3f,0xd00)+_0x590b6b(0x6b3,0x5a1,0x282,0x8fa)+_0x383eb4['medias'][-0x29*-0xe3+0xa36*-0x1+-0x8b7*0x3]['formattedS'+'ize']+'\x0a',_0x23cd75+=_0x590b6b(0xccd,0x10a2,0x1036,0xdc8)+_0x383eb4[_0x590b6b(0x64c,0x146,0x2a5,0x6a1)];let _0x3ea89c=await _0x3f8643[_0x4a98f1(0xd42,0xca9,0xbeb,0x842)](getBuffer,_0x383eb4['thumbnail']);_0x3f8643[_0x590b6b(0x3fd,0x7c1,0x873,0x2b4)](sendButLocation,from,_0x23cd75,_0x590b6b(0xd08,0xc7c,0xc4c,0x9df)+_0x4a98f1(0xb21,0xb49,0x825,0xfe3)+_0x590b6b(0x553,0x4b9,0x9e7,0x726)+'wnload\x20'+enter+enter+'©\x20'+ownername,_0x3ea89c,[{'buttonId':'fbhd\x20'+fbte,'buttonText':{'displayText':_0x3f8643[_0x4a98f1(0xaaf,0x804,0x997,0xa2a)](_0x3f8643[_0x590b6b(0x95c,0x54b,0xdd0,0xe5e)]('HD','\x20'),_0x383eb4[_0x4a98f1(0x5c1,0x681,0x27a,0x360)][-0xbaa+-0x76a+-0x3d1*-0x5]['formattedS'+'ize'])},'type':_0x3f8643['VsiHX']},{'buttonId':_0x590b6b(0x8c4,0x8b2,0x555,0xa53)+fbte,'buttonText':{'displayText':'SD'+'\x20'+_0x383eb4['medias'][0x2c1+0x1a88+-0x1d49][_0x4a98f1(0xd01,0x10d8,0xf46,0xf8d)+_0x4a98f1(0xaec,0x707,0x1023,0x5fd)]},'type':_0x3f8643['VsiHX']},{'buttonId':_0x4a98f1(0x8f1,0x4c8,0xc2d,0xafc)+fbte,'buttonText':{'displayText':_0x3f8643['jopdG'](_0x3f8643[_0x590b6b(0xd06,0xa43,0x125a,0x11be)](_0x3f8643[_0x4a98f1(0x8d9,0xc22,0xd59,0xdc4)],'\x20'),_0x383eb4[_0x4a98f1(0x5c1,0x69a,0x236,0xb03)][-0x1c6b+0x1*0x14d5+-0x796*-0x1][_0x590b6b(0xc8d,0x104e,0x81c,0x919)+_0x4a98f1(0xaec,0xf3d,0x70b,0x8ca)])},'type':_0x3f8643['VsiHX']}],{});}):fbdl(fbte)['then'](_0x381521=>{function _0x143383(_0x4cc03e,_0x3bccbf,_0x19eade,_0x4636d2){return _0x3f34aa(_0x4cc03e-0xe6,_0x4636d2,_0x19eade-0x52c,_0x4636d2-0x139);}function _0xcd8f22(_0x57a346,_0x2d8673,_0x36cc32,_0x14a5c8){return _0x3f34aa(_0x57a346-0x16d,_0x36cc32,_0x2d8673-0x44a,_0x14a5c8-0x3d);}const _0xb28367={'fYvhn':function(_0x174c8d,_0x3fa333,_0x160b8c,_0x525a50,_0x4db20d){return _0x174c8d(_0x3fa333,_0x160b8c,_0x525a50,_0x4db20d);}};_0xb28367[_0x143383(0xaa5,0x1035,0xcef,0x103b)](sendFileFromUrl,from,_0x381521[0x2*-0x12a2+-0xece+0x3412],lang[_0xcd8f22(0xd56,0xa62,0x71b,0x90d)](),mek);});break;case _0x3f34aa(-0x265,-0x29d,0x255,0x14d):case _0x1da9f3(0x587,0x10a,0x5e1,0x6af):const _0x122fe1={};_0x122fe1[_0x3f34aa(0x64d,0xe22,0x9c5,0xdce)+'t']=_0x3f34aa(0x729,0x77b,0xa78,0xebe)+_0x1da9f3(0x332,0x1f7,0x617,0x88b);const _0x896558={};_0x896558['buttonId']=_0x1da9f3(0x5a8,0x7dc,0x12d,0x58e),_0x896558[_0x3f34aa(0x504,0x395,0x6ef,0xb35)]=_0x122fe1,_0x896558[_0x3f34aa(0x2c4,0x3e6,0x693,0x4ec)]=0x1;const _0x2a01d5={};_0x2a01d5['quoted']=mek;if(!isPremium)return sendButMessage(from,_0x3f34aa(0x379,0x5a1,0x3c5,-0x6)+_0x1da9f3(0x88e,0xc2e,0xa19,0xae3)+_0x1da9f3(0x5e1,0x1ab,0x859,0x5de)+_0x1da9f3(0x630,0x54b,0x2f6,0x942)+_0x1da9f3(0x478,0xf8,-0x14,-0x7f)+_0x3f34aa(0x7aa,0xc86,0x9d4,0x91d)+_0x3f34aa(-0x63,0x6e,0x37b,0x3d3)+_0x1da9f3(0x1c1,-0x228,0x49,0x2fd)+'ngan\x20cara\x20'+_0x3f34aa(0x419,0x640,0x10c,-0x77)+prefix+_0x1da9f3(0x5a8,0x7f9,0x26c,0x6e3),_0x1da9f3(0x737,0x76c,0x632,0x522)+_0x3f34aa(0x6ef,0x946,0x680,0x6ad),[_0x896558],_0x2a01d5);if(!q)return reply(_0x1da9f3(0xb93,0x7b4,0xf6e,0xa88));if(!isUrl(args[-0x1fe1+0x132*-0xd+0xc7*0x3d])&&!args[-0x1*-0x2366+0x6d*-0x13+-0x1b4f][_0x1da9f3(0xacd,0xba6,0xb50,0x9d7)](_0x1da9f3(0x2ad,-0x203,0x16a,0x233)+'om'))return reply(lang[_0x3f34aa(0x53d,0x3d2,0x5bc,0x59d)]());reply(lang[_0x1da9f3(0x723,0x82a,0x780,0x5be)]());var fbte=args[_0x1da9f3(0x4cc,0x4a7,0x520,0x65a)]('\x20');zee[_0x1da9f3(0x310,0x160,0x41,0x17b)](fbte)[_0x3f34aa(0x90,-0x413,0x2c,0x58)](async _0x49eece=>{const _0x3d3dfc={'TzaAA':function(_0xf7210d,_0x5598a8,_0x4a6f0e,_0x4e6f95,_0x26a7ee){return _0xf7210d(_0x5598a8,_0x4a6f0e,_0x4e6f95,_0x26a7ee);}};function _0x28cc19(_0x6cd43c,_0x25b799,_0x32ed93,_0x3fd22c){return _0x1da9f3(_0x3fd22c-0x45,_0x25b799,_0x32ed93-0x99,_0x3fd22c-0xd4);}function _0x5ac5c7(_0x73aee5,_0x24819c,_0x222525,_0xe6299c){return _0x1da9f3(_0x222525- -0x1ff,_0x24819c,_0x222525-0x1a0,_0xe6299c-0x197);}_0x3d3dfc['TzaAA'](sendFileFromUrl,from,_0x49eece[_0x5ac5c7(0x67,0x194,0x1af,-0xf8)][0x1b*0x133+-0xa9d+-0x15c3][_0x28cc19(0x6c4,0x69d,0x4c8,0x4f2)],lang[_0x5ac5c7(0x2ad,0x5c7,0x5cd,0x7d4)](),mek);})[_0x3f34aa(0x562,0x6c0,0x6df,0x690)](_0x34a583=>{function _0x318611(_0x39860a,_0x2ada21,_0x84536a,_0x24673d){return _0x1da9f3(_0x84536a-0x323,_0x2ada21,_0x84536a-0x11b,_0x24673d-0x47);}function _0x1e4c4d(_0x381c7d,_0x28b22b,_0x20779a,_0x470d6b){return _0x3f34aa(_0x381c7d-0x2d,_0x381c7d,_0x28b22b-0x227,_0x470d6b-0x160);}const _0xf5f0bd={'dMaGN':function(_0x2639ef,_0x1bf5ba){return _0x2639ef(_0x1bf5ba);}};_0xf5f0bd[_0x318611(0xcd0,0xa17,0x7d7,0xa2e)](reply,lang[_0x318611(0xa2e,0x1070,0xb92,0xbb3)]());});break;case _0x3f34aa(0x3ae,0x158,0x46a,0x663):case _0x3f34aa(0x3e3,0x29f,0x5c9,0x210):if(!q)return reply(_0x3f34aa(0x592,0xa0e,0x9df,0xbc8));if(!isUrl(args[0x3e*0x1+0x1*0x15bb+-0x3*0x753])&&!args[0x1ff*0x1+-0x1*-0x2592+-0x2791]['includes']('facebook.c'+'om'))return reply(lang[_0x1da9f3(0x770,0xba3,0x285,0x468)]());reply(lang[_0x1da9f3(0x723,0xba2,0x2f8,0x5b6)]());var fbte=args[_0x1da9f3(0x4cc,0x690,0x363,0x1e5)]('\x20');zee[_0x3f34aa(0x690,-0x28d,0x15c,-0x70)](fbte)[_0x1da9f3(0x1e0,-0x164,0x66c,0x73)](async _0x122528=>{function _0xc6ae97(_0x211b02,_0xefaf3a,_0x4bd4f2,_0x456afc){return _0x3f34aa(_0x211b02-0xef,_0x211b02,_0x456afc-0x22c,_0x456afc-0x167);}const _0x10126f={'orMGs':function(_0x2e23c5,_0x5ee1a9,_0x1c5e75,_0x2a0772,_0x417621){return _0x2e23c5(_0x5ee1a9,_0x1c5e75,_0x2a0772,_0x417621);}};function _0x196068(_0x48004b,_0x244de3,_0x5f3dfc,_0x2019d0){return _0x3f34aa(_0x48004b-0x62,_0x244de3,_0x5f3dfc-0x190,_0x2019d0-0x14a);}_0x10126f[_0xc6ae97(0x932,0x734,0x544,0x64a)](sendFileFromUrl,from,_0x122528['medias'][0x1b14+0x2012+-0x1d93*0x2][_0x196068(0x5c8,0x22a,0x489,-0x7)],lang['success'](),mek);})['catch'](_0x247e20=>{function _0x413ac1(_0x3919c4,_0x18a8b7,_0x4e89a8,_0xd0b3fa){return _0x1da9f3(_0xd0b3fa- -0x2e6,_0x18a8b7,_0x4e89a8-0x136,_0xd0b3fa-0x8);}const _0x3541bb={'yZpdU':function(_0x36b70a,_0x5d858c){return _0x36b70a(_0x5d858c);}};function _0xa0002f(_0x2d0370,_0x1801c7,_0x47a802,_0x3b5a6a){return _0x3f34aa(_0x2d0370-0x1c3,_0x47a802,_0x2d0370-0x57e,_0x3b5a6a-0x14b);}_0x3541bb[_0x413ac1(0x447,0x198,0x516,0x624)](reply,lang[_0xa0002f(0xc39,0x6fd,0xbf1,0xa8e)]());});break;case _0x1da9f3(0x3e7,0x6c2,0x5b2,0x760):case'facebookau'+_0x3f34aa(-0x92,0x2c8,0xfa,0x4c6):const _0x349831={};_0x349831[_0x1da9f3(0xb79,0xfe9,0xe6c,0xe09)+'t']=_0x1da9f3(0x695,0x3ab,0x16b,0x590)+'t';const _0x1c0f67={};_0x1c0f67['buttonId']=_0x1da9f3(0x886,0xbf5,0x379,0xd82),_0x1c0f67[_0x1da9f3(0x8a3,0x823,0xab5,0xb54)]=_0x349831,_0x1c0f67[_0x1da9f3(0x847,0x4f8,0xc90,0xd65)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x219,0xbfa,0x6d2,0x92c)](prefix),'©\x20'+ownername,[_0x1c0f67]);if(!q)return reply('Linknya?');if(!isUrl(args[-0x907*0x4+0x1cc6+0x756])&&!args[0x176b+-0xec7+-0x8a4][_0x1da9f3(0xacd,0xae1,0xb27,0x6fe)](_0x1da9f3(0x2ad,0x63e,0x3be,0x806)+'om'))return reply(lang[_0x1da9f3(0x770,0xbd6,0x73b,0x30c)]());reply(lang[_0x3f34aa(0x496,0x2b1,0x56f,0xa7c)]());var fbte=args[_0x3f34aa(-0x1eb,0x5fc,0x318,0x35)]('\x20');zee[_0x3f34aa(0x50,0x2c2,0x15c,-0x1f1)](fbte)[_0x3f34aa(-0x1db,-0x36b,0x2c,-0x2c9)](async _0x337c1f=>{const _0x26013f={'vzVGU':function(_0x3b7d7a,_0x4de855){return _0x3b7d7a(_0x4de855);},'tgnhC':_0x32f303(0x8f7,0x390,0x62e,0x13e)+_0x32f303(0xcde,0xad6,0x79b,0x5d4)+'xTH6oLd8','avBrZ':_0x7159d(0x774,0x9bd,0x840,0x914)};let _0x13e422=await _0x26013f[_0x7159d(0x380,0xec,0x13f,0x54b)](getBuffer,_0x337c1f[_0x7159d(0x17d,-0x341,0x625,0xb0)][-0xea2*-0x1+0xd7f+-0x17b*0x13][_0x7159d(0x27c,-0xd6,0x1d3,0x23)]);const _0x254638={};_0x254638[_0x7159d(0x3de,0x281,0x353,0x59e)]=_0x26013f['tgnhC'],_0x254638[_0x7159d(0x934,0xbc7,0x520,0xc2a)]=0x2,_0x254638[_0x32f303(0x456,0x58f,0x6a3,0x610)]='Tiktok\x20Mus'+'ic',_0x254638[_0x7159d(0x77a,0x343,0x3c9,0x32d)]='',_0x254638[_0x32f303(0x5fc,-0xc2,0x3d9,0x816)+'rl']='',_0x254638['thumbnail']=pp_userz;const _0x36ba3c={};_0x36ba3c[_0x7159d(0x3b7,0x630,0x715,0x845)]=_0x7159d(-0x43,-0x3a9,-0x1eb,0x66)+_0x7159d(0x619,0x9b1,0x8b8,0x1a5)+_0x32f303(0xb93,0xa42,0xb2b,0x6d2),_0x36ba3c[_0x32f303(0x568,0x26,0x2c5,0x4f8)+'Score']=0x3b9aca00,_0x36ba3c['isForwarde'+'d']=!![];function _0x7159d(_0x31d97e,_0x323a17,_0x4fc8ab,_0x1520d7){return _0x3f34aa(_0x31d97e-0x55,_0x1520d7,_0x31d97e- -0x7d,_0x1520d7-0x97);}function _0x32f303(_0x40d566,_0x11042b,_0x4f5216,_0x4d12de){return _0x1da9f3(_0x4f5216- -0x126,_0x4d12de,_0x4f5216-0x6c,_0x4d12de-0x111);}_0x36ba3c[_0x32f303(0x9fb,0x798,0x935,0x503)+_0x7159d(0x929,0xa5e,0x6c2,0xa18)]=!![],_0x36ba3c[_0x7159d(0x312,0x222,0x7c2,0x3e6)+_0x32f303(0x26c,0x362,0x3c4,0x3d3)]=_0x254638;const _0x35e0ae={};_0x35e0ae[_0x7159d(0x16e,0x4cf,0x5f6,0x3da)+'o']=_0x36ba3c,_0x35e0ae['mimetype']=_0x26013f['avBrZ'],_0x35e0ae[_0x7159d(0x3c4,0x2e0,0x478,-0x53)]=_0x32f303(0xe2,-0x310,0xc8,0x452)+'usic.mp3',_0x35e0ae[_0x7159d(0x73c,0x728,0x6bd,0xbf6)]=fgclink2,_0x35e0ae[_0x7159d(0x94f,0xbf0,0x6e7,0x627)]=pp_bot2,alpha[_0x32f303(0x605,-0x19,0xf7,-0xf4)+'e'](from,_0x13e422,MessageType[_0x7159d(0x24b,0x13e,0xfd,-0x7f)],_0x35e0ae);})[_0x3f34aa(0x43e,0xa91,0x6df,0x90a)](_0x2a4d4f=>{function _0x210233(_0xdac7de,_0x271a7d,_0x11b984,_0x1822a4){return _0x1da9f3(_0x271a7d- -0x393,_0xdac7de,_0x11b984-0x137,_0x1822a4-0x134);}function _0x1c2307(_0x113392,_0x5bf402,_0x27c335,_0x2379c7){return _0x1da9f3(_0x2379c7-0x37c,_0x5bf402,_0x27c335-0x12d,_0x2379c7-0x126);}const _0x84d98d={'QPeua':function(_0x1397c2,_0x29a8c1){return _0x1397c2(_0x29a8c1);}};_0x84d98d[_0x210233(0x5b8,0x35c,0x533,0x690)](reply,lang[_0x210233(0x861,0x4dc,0x48b,0x683)]());});break;case _0x1da9f3(0x9df,0x9f4,0xed8,0x964)+'o':case _0x3f34aa(0xbfd,0x8c5,0xa4b,0xe74)+'c':case _0x3f34aa(0x255,-0x3c,0x3b7,0x20f):const _0x4f79d6={};_0x4f79d6[_0x3f34aa(0xdce,0xa13,0x9c5,0xd9b)+'t']='Check\x20Limi'+'t';const _0x192f1b={};_0x192f1b[_0x1da9f3(0x4c3,0x1bb,0x17e,0x53c)]=_0x3f34aa(0x557,0xaef,0x6d2,0x98e),_0x192f1b[_0x1da9f3(0x8a3,0x86c,0x967,0xb0e)]=_0x4f79d6,_0x192f1b[_0x3f34aa(0x45a,0x24f,0x693,0x502)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x494,0x2a0,0x6d2,0x8e3)](prefix),'©\x20'+ownername,[_0x192f1b]);if(!q)return reply(_0x1da9f3(0xb93,0xde1,0xbc8,0x695));if(!isUrl(args[0x1*0x68b+-0x68e*-0x5+-0x131*0x21])&&!args[-0x14*0xaa+0x234b+-0x1603][_0x1da9f3(0xacd,0xfa2,0xf14,0xf05)]('tiktok.com'))return reply(lang[_0x1da9f3(0x770,0xa19,0xbd5,0x585)]());reply(lang[_0x3f34aa(0x97b,0x334,0x56f,0xf9)]()),hx[_0x3f34aa(0xbab,0x20d,0x677,0x6c9)+'er'](''+args[0xe03+0x1e95+0x164c*-0x2])['then'](_0x5878a4=>{const _0x46690e={};_0x46690e[_0x5d4cc9(0x503,0xc56,0xa28,0x923)]=_0x5d4cc9(0x6d1,0x9be,0x8f5,0xda2)+_0x2073c7(0xa8a,0x304,0x79b,0x62e)+'xTH6oLd8',_0x46690e['zVVYq']=_0x2073c7(0x4d8,0x898,0x87f,0x5a1);const _0x26e635=_0x46690e,{wm:_0x44ce0c,nowm:_0x2edff0,audio:_0x3b034d}=_0x5878a4;function _0x5d4cc9(_0x13127c,_0x464acc,_0x21fdae,_0x133d79){return _0x3f34aa(_0x13127c-0x19e,_0x133d79,_0x21fdae-0x355,_0x133d79-0xf6);}function _0x2073c7(_0xbec543,_0xa60067,_0x5b9714,_0x169be5){return _0x1da9f3(_0x5b9714- -0x126,_0x169be5,_0x5b9714-0xae,_0x169be5-0x1dc);}axios[_0x5d4cc9(0x60f,0x907,0x670,0x405)](_0x5d4cc9(0xe5d,0xc8b,0xc72,0xa2c)+_0x2073c7(0x4ef,0x7c4,0x332,0xa4)+_0x2073c7(0xc8e,0x875,0xad6,0xe48)+_0x2073c7(0xd4,-0x487,0xa2,-0xe8)+_0x2edff0)[_0x2073c7(0xdb,0x4aa,0xba,-0x435)](async _0x472f56=>{me='*Link*\x20:\x20'+_0x472f56[_0x8c4466(0xb5c,0x8f8,0x915,0x4cd)],nowmm=await getBuffer(_0x3b034d);function _0x8c4466(_0x59ee8a,_0x352ed0,_0x2cd8b6,_0x2fb74b){return _0x5d4cc9(_0x59ee8a-0x9c,_0x352ed0-0x17b,_0x352ed0-0xed,_0x2fb74b);}const _0x1c01d4={};function _0x5725b3(_0x2784a2,_0x3da902,_0x4f54d8,_0x197698){return _0x2073c7(_0x2784a2-0x79,_0x3da902-0x18a,_0x4f54d8-0x159,_0x3da902);}_0x1c01d4['mediaUrl']=_0x26e635[_0x5725b3(0xb75,0x500,0x8ba,0x5cb)],_0x1c01d4[_0x5725b3(0x902,0x9e9,0xb98,0xe1a)]=0x2,_0x1c01d4['title']='Tiktok\x20Mus'+'ic',_0x1c01d4['body']='',_0x1c01d4[_0x5725b3(0x71,0x86f,0x532,0xa88)+'rl']='',_0x1c01d4[_0x8c4466(0x1104,0xe0e,0x946,0xe50)]=pp_userz;const _0x339e64={};_0x339e64[_0x5725b3(0x4da,0x983,0x61b,0x1ea)]=_0x8c4466(0x559,0x655,0x9a5,0xb39)+_0x8c4466(0x68d,0x81e,0xbb2,0x404)+_0x8c4466(0xa57,0x658,0x144,0xb76),_0x339e64[_0x5725b3(0x383,0xff,0x41e,0x722)+_0x5725b3(0x6ff,0x241,0x79a,0x4f6)]=0x3b9aca00,_0x339e64[_0x8c4466(0xc2a,0x991,0xd25,0xb6b)+'d']=!![],_0x339e64[_0x5725b3(0xed2,0x5f0,0xa8e,0x7f2)+_0x5725b3(0x687,0x1075,0xb8d,0xcb5)]=!![],_0x339e64[_0x8c4466(0x6a3,0x7d1,0x63f,0xc68)+_0x8c4466(0x440,0x778,0x935,0x991)]=_0x1c01d4;const _0x57823b={};_0x57823b[_0x8c4466(0xa37,0x62d,0x395,0x9c8)+'o']=_0x339e64,_0x57823b[_0x8c4466(0x1283,0xe78,0xcc5,0x11e2)]=_0x26e635[_0x5725b3(0x109,0x687,0x2ca,0x19e)],_0x57823b[_0x8c4466(0x43a,0x883,0x8b9,0x689)]=_0x5725b3(0x34a,0x748,0x3fa,0x372)+_0x8c4466(0x97d,0x441,0x442,0x237),_0x57823b[_0x5725b3(0x8a9,0xc18,0x9a0,0x975)]=fgclink2,_0x57823b['thumbnail']=pp_bot2,alpha['sendMessag'+'e'](from,nowmm,MessageType[_0x5725b3(0x417,-0x2b,0x4af,0x32e)],_0x57823b);});}),await limitAdd(sender,limit);break;case _0x1da9f3(0x66c,0xaf2,0x6be,0x835):case _0x3f34aa(0x82e,0x4d5,0x412,0x3b2):case _0x1da9f3(0x2c4,0x240,0x732,0x92):const _0xff9767={};_0xff9767[_0x3f34aa(0x77d,0xaaf,0x9c5,0xef2)+'t']=_0x3f34aa(0x609,0x1f3,0x4e1,0x4d2)+'t';const _0x58c4c0={};_0x58c4c0[_0x1da9f3(0x4c3,0x72e,0x820,0x12b)]=_0x1da9f3(0x886,0x406,0x54a,0x547),_0x58c4c0['buttonText']=_0xff9767,_0x58c4c0[_0x3f34aa(0x499,0x732,0x693,0x865)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x58c4c0]);if(!q)return reply(_0x3f34aa(0x618,0x6b9,0x9df,0x932));if(!isUrl(args[0x1e5b*0x1+0x285*0xa+-0x378d])&&!args[-0x22f3+-0x1224+0x3517*0x1]['includes'](_0x3f34aa(0x531,-0x1cd,0x32e,0xf8)))return reply(lang['erorLink']());reply(lang[_0x1da9f3(0x723,0x5f6,0x9c2,0x408)]()),hx[_0x1da9f3(0x82b,0xba9,0x8ff,0x6ed)+'er'](''+args[0x1265+-0x21bc+-0xbb*-0x15])[_0x3f34aa(-0x18a,0xca,0x2c,-0x185)](_0x13be58=>{const _0x28a7b9={'YYgAd':function(_0x24a537,_0x3484c6){return _0x24a537===_0x3484c6;},'JwISK':_0x375bf7(0x3a5,0x384,0x1e8,0x2a1),'EYsCt':function(_0x6e47e7,_0x5dd307){return _0x6e47e7(_0x5dd307);},'LJwez':_0x375bf7(0x3df,0x43f,0x311,0x73f)};function _0x375bf7(_0x46a7df,_0x203ec4,_0x55c7b3,_0x2b15d9){return _0x1da9f3(_0x46a7df- -0x278,_0x2b15d9,_0x55c7b3-0x10e,_0x2b15d9-0xcf);}function _0x3fac50(_0x326fe7,_0x108bc2,_0x37a114,_0x3f76a0){return _0x3f34aa(_0x326fe7-0x1c8,_0x37a114,_0x326fe7-0x189,_0x3f76a0-0xcf);}const {wm:_0x437d3e,nowm:_0x53e734,audio:_0x1fbeb2}=_0x13be58;axios[_0x375bf7(0x257,0x475,0x614,0x205)]('https://ti'+_0x375bf7(0x1e0,-0x2ba,0x56d,0x333)+'api-create'+'.php?url='+_0x53e734)[_0x375bf7(-0x98,-0x1d7,-0x3ce,-0x133)](async _0x3890e6=>{function _0x24eb51(_0xbbee14,_0xdb8959,_0x4323e2,_0x26e3c2){return _0x375bf7(_0xbbee14-0x19d,_0xdb8959-0xe0,_0x4323e2-0x145,_0x4323e2);}function _0x465235(_0x4ba811,_0x273bc0,_0x7db3e2,_0x7aed1){return _0x3fac50(_0x7db3e2- -0xc9,_0x273bc0-0xf2,_0x4ba811,_0x7aed1-0x94);}if(_0x28a7b9['YYgAd'](_0x28a7b9[_0x465235(-0x171,-0x168,0x11c,0x212)],_0x28a7b9[_0x465235(-0x35a,-0xc,0x11c,0x21c)])){me=_0x24eb51(0x7ad,0xb25,0x77a,0x289)+_0x3890e6[_0x24eb51(0x58f,0x8da,0x5e7,0x8cc)],nowmm=await _0x28a7b9[_0x465235(0x50,0x287,0x43e,0x755)](getBuffer,_0x53e734);const _0x285482={};_0x285482[_0x24eb51(0x534,0x42d,0x29f,0x125)]='https://yo'+'utu.be/1r_'+_0x465235(0x8ed,0xf8d,0xac6,0x9ce),_0x285482['mediaType']=0x2,_0x285482[_0x465235(0x42e,0x212,0x6d5,0xa26)]=_0x465235(0x681,0x9ce,0x630,0x372)+_0x24eb51(0x916,0xb09,0xde4,0x3fc),_0x285482[_0x465235(0x459,0x3d5,0x8b7,0xdc2)]='',_0x285482[_0x465235(0x228,0x72e,0x40b,0x2b7)+'rl']='',_0x285482['thumbnail']=pp_userz;const _0x4512de={};_0x4512de['forwarding'+'Score']=0x3b9aca00,_0x4512de[_0x24eb51(0x628,0xae8,0x376,0x84a)+'d']=!![],_0x4512de[_0x465235(0x71a,0xac0,0x967,0xbe3)+_0x465235(0xb7f,0x5ea,0xa66,0x649)]=!![],_0x4512de['externalAd'+_0x24eb51(0x40f,0x66b,0x6db,-0xf4)]=_0x285482;const _0x298f40={};_0x298f40['contextInf'+'o']=_0x4512de,_0x298f40[_0x465235(0x850,0x82b,0xaf6,0xce5)]=_0x28a7b9['LJwez'],_0x298f40['filename']=_0x24eb51(0x649,0x6dc,0xb48,0x16d)+_0x24eb51(0x889,0x76b,0x89c,0xc2d),_0x298f40[_0x24eb51(0x892,0xa2a,0x73d,0xd33)]=fgclink2,_0x298f40[_0x24eb51(0xaa5,0xc94,0xe1a,0xc74)]=pp_bot2,alpha[_0x465235(0x13f,0xe2,0x129,0x454)+'e'](from,nowmm,MessageType['document'],_0x298f40);}else throw new _0x4a1d60(_0x504d02);});}),await limitAdd(sender,limit);break;case _0x3f34aa(-0x196,-0x156,0x24e,0x7a6):const _0x2a9038={};_0x2a9038['displayTex'+'t']=_0x3f34aa(0x442,0x53f,0x4e1,0x278)+'t';const _0x1d148a={};_0x1d148a[_0x1da9f3(0x4c3,0x55f,0x5fb,0xa4)]='limit',_0x1d148a[_0x3f34aa(0xaa6,0xc02,0x6ef,0xb1b)]=_0x2a9038,_0x1d148a[_0x1da9f3(0x847,0x4c4,0x705,0x972)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0xba9,0x3ad,0x6d2,0xaf9)](prefix),'©\x20'+ownername,[_0x1d148a]);if(!isUrl(args[0x2*0x1241+0xc42+-0x30c4])&&!args[-0xf8d*0x2+0xacf+0x5*0x40f][_0x3f34aa(0xb17,0xe33,0x919,0xc9b)](_0x1da9f3(0x4e2,0x9b6,0xe,0xa10)))return reply(lang['erorLink']());if(!q)return fakegroup(_0x3f34aa(0x559,0xecb,0x9df,0xce1));reply(lang[_0x1da9f3(0x723,0x632,0x50e,0x28b)]()),hx[_0x1da9f3(0x82b,0x644,0x5f8,0x558)+'er'](''+args[-0x171f+0xcc8+-0xa57*-0x1])[_0x1da9f3(0x1e0,0x4af,-0xc,0x634)](_0x341873=>{function _0x334c6a(_0xc9cd5,_0x50c655,_0x3248d1,_0x17a509){return _0x1da9f3(_0xc9cd5- -0x399,_0x3248d1,_0x3248d1-0x165,_0x17a509-0x5e);}function _0x22eb56(_0x154ea9,_0x3a2ca9,_0x714bb4,_0x4398b8){return _0x3f34aa(_0x154ea9-0x23,_0x4398b8,_0x154ea9- -0x208,_0x4398b8-0x122);}const _0x3014ef={'btoDg':_0x334c6a(0x6f,0x550,-0x124,-0x245),'qnACD':function(_0x5ed130,_0x2a8876){return _0x5ed130>=_0x2a8876;},'UJCvO':function(_0x37b33c,_0x340735){return _0x37b33c(_0x340735);},'ceIex':function(_0x794385,_0x41576b,_0x599683,_0xa4d2e8){return _0x794385(_0x41576b,_0x599683,_0xa4d2e8);},'pdDqX':'hfNqm','qZlxK':_0x22eb56(0x585,0x5c2,0x7e8,0xa93),'PigZx':function(_0x4c0aa,_0x4a5965){return _0x4c0aa(_0x4a5965);},'YqOKp':_0x334c6a(0x3bb,-0x16,0x466,0x63d)+_0x334c6a(0x528,0x3ca,0x460,0x9a6)+_0x334c6a(0x821,0xb01,0x61f,0x73d),'loycf':'video/mp4'},{wm:_0x633a87,nowm:_0x2abad5,audio:_0x421abd}=_0x341873;axios[_0x334c6a(0x136,-0x2fe,0x15,0x333)](_0x22eb56(0x715,0xbc7,0x645,0x597)+_0x22eb56(0x9c,-0x1d6,-0x407,-0x253)+_0x334c6a(0x863,0xd3f,0xb6f,0xd3a)+'.php?url='+_0x633a87)['then'](async _0x39c3f3=>{function _0x1c2101(_0x4ccb1e,_0x4ec28b,_0x463abc,_0x4d97fb){return _0x22eb56(_0x4ec28b- -0x1a,_0x4ec28b-0x1e1,_0x463abc-0xb1,_0x4d97fb);}function _0x2e85c7(_0x37b230,_0x5d9657,_0x38a8a2,_0x41e14d){return _0x22eb56(_0x38a8a2-0x32c,_0x5d9657-0x54,_0x38a8a2-0x10a,_0x37b230);}const _0x2704dd={'ealdC':_0x3014ef[_0x1c2101(0x21c,0x736,0xa37,0xbbf)],'RoevS':function(_0x20b928,_0x5f00c6){function _0x480731(_0x18a572,_0x236d6d,_0x35463b,_0x3fe355){return _0x1c2101(_0x18a572-0x88,_0x3fe355-0x60a,_0x35463b-0x1d,_0x236d6d);}return _0x3014ef[_0x480731(0x3f5,0x402,0x7a4,0x513)](_0x20b928,_0x5f00c6);},'hnkRQ':function(_0x244a6e,_0x35c134){function _0x522cca(_0x287f40,_0x4c4143,_0x2ff112,_0x3c3463){return _0x1c2101(_0x287f40-0x1cc,_0x3c3463-0x491,_0x2ff112-0x11d,_0x4c4143);}return _0x3014ef[_0x522cca(0xe0,-0x1c2,0xe,0x345)](_0x244a6e,_0x35c134);},'xPfxR':function(_0x44f258,_0x5e5e09,_0x349fd8,_0x305a77){function _0x6c704e(_0xffb44d,_0xaa7bd0,_0x43b539,_0x1a811d){return _0x1c2101(_0xffb44d-0x58,_0xaa7bd0-0x5aa,_0x43b539-0xc3,_0x1a811d);}return _0x3014ef[_0x6c704e(0x6c9,0xa4c,0xe0e,0xf11)](_0x44f258,_0x5e5e09,_0x349fd8,_0x305a77);}};if(_0x3014ef[_0x1c2101(-0x1f5,0x10,-0xad,-0x156)]!==_0x3014ef['qZlxK']){me='*Link*\x20:\x20'+_0x39c3f3[_0x1c2101(0x1b7,0x294,0xd2,0x79f)],wmm=await _0x3014ef[_0x2e85c7(0x607,0x584,0x9cd,0x511)](getBuffer,_0x633a87);const _0x99764f={};_0x99764f[_0x2e85c7(0xa60,0x46a,0x57f,0x692)]=_0x3014ef[_0x2e85c7(0xcab,0x68f,0x9f7,0xe4f)],_0x99764f[_0x1c2101(0x71a,0x78f,0xb91,0x585)]=0x2,_0x99764f['title']=_0x2e85c7(0xf65,0xe9c,0xa7d,0xf48)+'h\x20Watermar'+'k',_0x99764f['body']='',_0x99764f[_0x2e85c7(0x326,0x4,0x46f,0xb7)+'rl']='',_0x99764f['thumbnail']=pp_userz;const _0x20d6ca={};_0x20d6ca[_0x2e85c7(-0xa6,-0x193,0x35b,0x5f1)+_0x2e85c7(0x886,0x9b6,0x6d7,0x8d2)]=0x3b9aca00,_0x20d6ca[_0x2e85c7(0x3a4,0x72b,0x673,0x623)+'d']=!![],_0x20d6ca[_0x1c2101(0x43d,0x685,0xb9c,0x674)+_0x1c2101(0x3ed,0x784,0x722,0x4bc)]=!![],_0x20d6ca[_0x2e85c7(0x874,0x98e,0x4b3,0x749)+'Reply']=_0x99764f;const _0x5efe97={};_0x5efe97[_0x2e85c7(-0x11e,0x1ad,0x30f,0x4f1)+'o']=_0x20d6ca,_0x5efe97['mimetype']=_0x3014ef['loycf'],_0x5efe97[_0x2e85c7(0x823,0x4a6,0x565,0x68a)]=_0x2e85c7(0x26c,0xaea,0x619,0x9c7)+_0x2e85c7(0x941,0x5e1,0x89a,0xcde),_0x5efe97[_0x1c2101(0x886,0x597,0xdc,0x5cf)]=fgclink2,_0x5efe97[_0x1c2101(0x786,0x7aa,0xba9,0x959)]=pp_bot2,alpha[_0x2e85c7(-0x93,0x38,0x18d,-0x162)+'e'](from,wmm,MessageType[_0x1c2101(0x5cf,0xa6,-0x192,-0x49d)],_0x5efe97);}else{const _0x31c78b={};_0x31c78b[_0x2e85c7(0x717,0xf35,0xaa3,0x7c9)]=_0x2704dd[_0x2e85c7(0x6a2,0x46c,0x74e,0x8a5)];const _0x22268b=_0x31c78b;if(_0x2704dd[_0x1c2101(0x46d,0x2e2,0x1dc,-0xd5)](_0x2704dd[_0x2e85c7(0x424,0x2fb,0x165,0x1fe)](_0x58bc70,_0x1e11c4),-0xc592+0x4c7b+0xee47))return _0x2704dd['xPfxR'](_0x157bbe,_0x4fb871,_0x1bbf65,_0x2e85c7(0x226,0x8c4,0x61a,0x5a4)+_0x1c2101(-0x165,0x2d1,0xa6,-0x259)+_0x2e85c7(0x28d,0x35,0x3e9,0x788)+_0x1cbbe0+(_0x2e85c7(-0x123,0x72b,0x29d,-0x12a)+'P3\x0a🆔\x20ID\x20:\x20')+_0x25924b[_0x2e85c7(0x862,0x945,0xb21,0xb9c)][0x2011+0x2b7+-0x22c8][_0x2e85c7(0x296,0x490,0x759,0x548)]+(_0x1c2101(-0x61,0x48f,0x581,0x701)+':\x20')+_0x276cfb['videos'][-0x360*-0x6+-0xb25+-0x91b]['ago']+('\x20\x0a🎃\x20Size\x20:'+'\x20')+_0x13a7e0+'\x0a🍃\x20Views:\x20'+_0x2d56b2[_0x1c2101(0xc01,0x7db,0xbb5,0x90f)][0xf19+0x31d*-0x6+-0x7*-0x83][_0x1c2101(0x755,0x545,0x228,0x69e)]+(_0x2e85c7(0x46c,0x703,0x319,0x6c6)+'on\x20:\x20')+_0x7019fe[_0x1c2101(0x674,0x7db,0x5da,0x5bf)][-0x1ce8+-0x1*0xfc1+0x2ca9][_0x1c2101(-0x27a,-0xa8,-0x3c5,-0x29f)]+_0x1c2101(0x939,0x5fe,0x69b,0x42a)+_0x394560[_0x2e85c7(0xd1e,0xea5,0xb21,0xbbd)][-0xec*-0x11+0xf15+-0x1ec1][_0x1c2101(-0x453,0xd7,0x1e,-0x29e)]+(_0x2e85c7(0xae9,0x655,0xa20,0xc1a)+_0x2e85c7(0xd24,0xd6e,0xac1,0xf8a)+_0x2e85c7(0x6d5,0x57a,0x185,-0x26)+_0x1c2101(0x2ec,0xd5,0x2a0,-0x3d)+'it\x20is\x20pres'+_0x1c2101(-0x2c0,-0x156,-0x6f,-0x199)+'he\x20form\x20of'+_0x1c2101(0x493,0x404,0x60e,0x5df)));_0x133db4(_0x3a505f,_0x5f557c,'')[_0x2e85c7(0x49b,0xbde,0x803,0x473)](_0x1a773e=>{function _0x3faa65(_0x2caf0b,_0x1f6fb5,_0x210ac3,_0x49f4a3){return _0x2e85c7(_0x1f6fb5,_0x1f6fb5-0x1a1,_0x2caf0b-0x3b3,_0x49f4a3-0x44);}_0x3ce473(_0x22268b[_0x3faa65(0xe56,0x1305,0x902,0x1011)]);});}});})['catch'](_0x1f33a9=>console[_0x3f34aa(0x4b9,0x180,0x51e,0x385)](_0x1f33a9)),await limitAdd(sender,limit);break;case _0x3f34aa(0x29e,0x5ad,0x541,0x61a):const _0x3258dd={};_0x3258dd[_0x1da9f3(0xb79,0x627,0xd85,0xb07)+'t']=_0x1da9f3(0xc2c,0xdc5,0x1068,0x1049)+_0x3f34aa(0x591,0x40f,0x17e,0x6d0);const _0x6d342d={};_0x6d342d[_0x1da9f3(0x4c3,0x788,0x157,0x2d0)]=_0x1da9f3(0x5a8,0x61a,0x5d6,0x639),_0x6d342d['buttonText']=_0x3258dd,_0x6d342d[_0x1da9f3(0x847,0x44b,0xa33,0x445)]=0x1;const _0x4d45b9={};_0x4d45b9[_0x1da9f3(0x96d,0x6cd,0xe18,0xec2)]=mek;if(!isPremium)return sendButMessage(from,_0x1da9f3(0x579,0x950,0x8b7,0x6b)+_0x1da9f3(0x88e,0xb24,0x4c4,0x7f6)+_0x3f34aa(0x572,0x723,0x42d,0x5a)+_0x1da9f3(0x630,0x8ee,0x232,0x4f7)+_0x1da9f3(0x478,0x6e,0x759,0x4ed)+_0x3f34aa(0xa19,0x774,0x9d4,0xb35)+_0x3f34aa(0x278,0x29a,0x37b,0x895)+'ekarang\x20de'+'ngan\x20cara\x20'+_0x1da9f3(0x2c0,0x786,0x6,0x610)+prefix+_0x1da9f3(0x5a8,0x2ca,0x163,0x1b5),_0x3f34aa(0x136,0x405,0x583,0x29)+_0x3f34aa(0x2da,0x42c,0x680,0x45f),[_0x6d342d],_0x4d45b9);if(args[_0x1da9f3(0xae2,0x8b0,0x89b,0x816)]<-0x1909+0x4*0x6b9+0x9e*-0x3)return reply(_0x1da9f3(0x1f1,0x4c4,0xf8,0x6e8)+'ana?\x20');if(!isUrl(args[-0x1*0x1f1b+0x2045+-0x12a])&&!args[-0x2190*-0x1+0x1816+0x5e*-0x9d][_0x3f34aa(0x895,0xe3b,0x919,0x606)](_0x1da9f3(0x6f5,0xb72,0x330,0x556)))return reply(mess['error']['Iv']);reply(lang[_0x3f34aa(0x35c,0x3e5,0x56f,0x506)]()),teks=args['join']('\x20'),res=await mediafireDl(teks),result='*MEDIAFIRE'+_0x3f34aa(0xd7d,0xd24,0x882,0xc60)+_0x1da9f3(0x531,0x4d4,0x5c,0x291)+petik+'々\x20Judul\x20:\x20'+petik+'\x20'+res[0x6*-0x647+0x2333+0x277][_0x1da9f3(0x756,0xbbc,0xb79,0xc6a)]+'\x0a'+petik+'々\x20Link\x20:\x20'+petik+'\x20'+res[-0x1786+0x18f9*0x1+-0x173]['link']+'\x0a'+petik+_0x3f34aa(0x5ac,0x1f4,0x3a5,0x123)+petik+'\x20'+res[0x102b+-0x2635+0x160a]['size']+(_0x1da9f3(0xc2f,0x101f,0xe5a,0xad6)+_0x3f34aa(0x8cf,0x139,0x51a,0xa59)+_0x1da9f3(0xab3,0xc94,0xd78,0x8f3)),await reply(result),mdf=await getBuffer(res[-0xe70+-0x23b7+0x1*0x3227][_0x1da9f3(0x1e9,0x55e,0x4d6,0x424)]);const _0x46e085={};_0x46e085[_0x1da9f3(0x60f,0xac2,0xbb,0x112)]=_0x3f34aa(0xa55,0x1e6,0x5a0,0x9c5)+_0x3f34aa(0x760,0xb1f,0x70d,0x5a8)+_0x1da9f3(0xbba,0xa63,0xcae,0xc55),_0x46e085[_0x1da9f3(0xb65,0x6a0,0x946,0x9b3)]=0x2,_0x46e085['title']=_0x1da9f3(0x397,-0x170,0x275,-0x1a6)+_0x1da9f3(0x1f0,-0xce,-0x346,0xa7),_0x46e085['body']='',_0x46e085['thumbnailU'+'rl']='',_0x46e085[_0x1da9f3(0xb80,0xeed,0xf2b,0xc3a)]=pp_userz;const _0x3f46c2={};_0x3f46c2[_0x3f34aa(-0x177,0x46d,0x237,0x387)+_0x3f34aa(0x634,0x17a,0x5b3,0x54c)]=0x3b9aca00,_0x3f46c2['isForwarde'+'d']=!![],_0x3f46c2['sendEpheme'+'ral']=!![],_0x3f46c2['externalAd'+_0x1da9f3(0x4ea,0x572,-0x3d,0x155)]=_0x46e085;const _0x2fb732={};_0x2fb732[_0x1da9f3(0x39f,0x474,0x17,0x17d)+'o']=_0x3f46c2,_0x2fb732[_0x1da9f3(0xbea,0xde5,0x80d,0x856)]=res[0x12a+0x4e*0x76+-0x251e]['mime'],_0x2fb732[_0x3f34aa(0xc4,0x265,0x441,0x728)]=res[0x1e67+-0xb17*0x3+0x2de][_0x3f34aa(0x9d4,0x4f2,0x5a2,0x389)],_0x2fb732[_0x1da9f3(0x96d,0xa34,0xe45,0xb91)]=fgclink,_0x2fb732['thumbnail']=pp_bot2,alpha[_0x1da9f3(0x21d,-0x31,0x3b4,0x141)+'e'](from,mdf,MessageType[_0x1da9f3(0x47c,0x107,-0x89,0x843)],_0x2fb732);break;case _0x1da9f3(0xa0a,0x9fe,0x69f,0xb43):const _0x37f2f2={};_0x37f2f2[_0x1da9f3(0xb79,0xd18,0xa62,0xff0)+'t']='Check\x20Limi'+'t';const _0x69ec70={};_0x69ec70[_0x3f34aa(0x691,-0x148,0x30f,0x79c)]='limit',_0x69ec70[_0x1da9f3(0x8a3,0x95b,0x472,0x873)]=_0x37f2f2,_0x69ec70['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x69ec70]);if(args[_0x3f34aa(0xb22,0x9eb,0x92e,0x61a)]===0x24b8+0x18b*0x11+0x125*-0x37)return reply('Kirim\x20peri'+_0x1da9f3(0x639,0xb07,0x793,0x2c9)+prefix+('video*\x20_Ju'+_0x1da9f3(0x23b,0x2ab,0x2f1,0x744)+_0x3f34aa(0x7b5,0x90e,0x66b,0x417)+_0x3f34aa(0x1f6,-0x164,0x224,0x458)));var srch=args['join']('');aramas=await yts(srch),server=_0x3f34aa(-0x126,0x793,0x35c,0x576),aramat=aramas[_0x1da9f3(0x99f,0xa10,0x6e4,0xe71)];var mulaikah=aramat[-0x1fb7+0x1530+0xa87]['url'];try{ytv2(mulaikah,server)['then'](_0x73b2d2=>{function _0x2ba666(_0x3d35c0,_0xe1ff38,_0x383f17,_0x16d1f1){return _0x3f34aa(_0x3d35c0-0x19b,_0x16d1f1,_0xe1ff38-0x29d,_0x16d1f1-0x1f1);}function _0x254de5(_0x3f205a,_0x5e79c7,_0x473927,_0x2eed35){return _0x1da9f3(_0x473927- -0x145,_0x3f205a,_0x473927-0xca,_0x2eed35-0x9b);}const _0x1cfb5e={'ruwnX':function(_0x3cf61d,_0x205e26){return _0x3cf61d(_0x205e26);},'xoWnE':function(_0x2a0c09,_0x2e897e,_0x498c84,_0x3408ce){return _0x2a0c09(_0x2e897e,_0x498c84,_0x3408ce);},'ssoiV':function(_0x551f93,_0x2be749,_0x5a982f){return _0x551f93(_0x2be749,_0x5a982f);}},{dl_link:_0x19dcf5,thumb:_0x5f145f,title:_0x4ecd3d,filesizeF:_0x25f686,filesize:_0x587198}=_0x73b2d2;axios[_0x2ba666(0x550,0x5b8,0x5c5,0x1fb)](_0x2ba666(0xbc3,0xbba,0xddc,0xde4)+_0x254de5(-0xf7,0x7be,0x313,-0x5b)+_0x254de5(0x8ac,0xa2e,0xab7,0xfb6)+_0x2ba666(0x3aa,0x2b1,0x73e,-0x17c)+_0x19dcf5)['then'](async _0x2eaddb=>{function _0x26cfc1(_0xb8f74,_0x393f5f,_0x1ddd79,_0x4573b2){return _0x2ba666(_0xb8f74-0xc7,_0xb8f74- -0x285,_0x1ddd79-0x157,_0x4573b2);}if(_0x1cfb5e['ruwnX'](Number,_0x587198)>=-0x28381+-0x24ecf+0x658f0)return sendMediaURL(from,_0x5f145f,_0x26cfc1(0xa06,0xd49,0xcb8,0x737)+'-PLAY\x20MP4*'+enter+enter+(_0x26cfc1(0x7d2,0x481,0x6b5,0x58e)+':\x20')+_0x4ecd3d+enter+(_0x26cfc1(0x2f,0x43b,0x277,0x57c)+_0x26cfc1(0x7c6,0x8e8,0x545,0x421)+'s')+enter+(_0x162be7(0x6a4,0x6eb,0x5e6,0x352)+_0x26cfc1(0x320,0x310,-0x11,0x5b8))+enter+(_0x26cfc1(0x3c8,0x27,0x756,0x4bc)+'e*\x20:\x20')+_0x25f686+enter+('🌀\x20*Link*\x20:'+'\x20')+_0x2eaddb[_0x26cfc1(0x4ce,0x9fb,0x4a9,0x234)]+enter+enter+(_0x26cfc1(0x88f,0xa3a,0x769,0x4d8)+_0x162be7(0xc6b,0x5ad,0x853,0xd95)+_0x26cfc1(0x39b,0x885,-0x3b,0x60a)+_0x26cfc1(0x139,-0xac,0x3b6,-0x22b)+_0x26cfc1(0x2b9,0x48a,0x65c,0x2c5)+_0x162be7(0x70f,0x6da,0x43d,0x50d)));function _0x162be7(_0x3b3c1d,_0x3ee4e5,_0x5b0a81,_0x3d3c75){return _0x254de5(_0x3d3c75,_0x3ee4e5-0x54,_0x5b0a81-0x370,_0x3d3c75-0x1d6);}const _0x35b4ca='*----「\x20YOU'+'TUBE\x20VIDEO'+_0x26cfc1(0x61c,0x497,0x556,0x14d)+enter+enter+(_0x26cfc1(0x7d2,0x947,0x5df,0x8c8)+':\x20')+_0x4ecd3d+enter+(_0x26cfc1(0x2f,0x4c4,-0x15,0x2de)+_0x162be7(0xf12,0x83d,0xb8d,0x638)+'s')+enter+(_0x162be7(0xe77,0x10f9,0xde0,0xb03)+_0x162be7(0xa2d,0x3d6,0x6e7,0x88a))+enter+(_0x162be7(0x896,0x99f,0x78f,0xc9b)+_0x26cfc1(0x8b7,0x7b8,0x780,0xc95))+_0x25f686+enter+(_0x162be7(0xd84,0x47f,0x854,0x75c)+'\x20')+_0x2eaddb[_0x26cfc1(0x4ce,0x7cc,0x44b,0x41f)]+enter+enter+(_0x26cfc1(0x3a2,0xaf,0x8a6,0x535)+_0x162be7(0xa99,0x6a0,0x87f,0xd85)+_0x26cfc1(0x4d6,0x820,0x6f4,-0x42)+'dang\x20dikir'+_0x162be7(0xe50,0xa5c,0xa7b,0xf57)+_0x162be7(0xfa1,0xed4,0xb6d,0x90b)+_0x162be7(0x800,0x12,0x3ee,0x70)+'t_');_0x1cfb5e['xoWnE'](sendMediaURL,from,_0x5f145f,_0x35b4ca),await _0x1cfb5e[_0x162be7(0xfa6,0x115b,0xd6b,0xa23)](sendMediaURL,from,_0x19dcf5)[_0x26cfc1(0x6f7,0x629,0x782,0x3e5)](()=>reply(_0x26cfc1(0x347,0x1d0,0x4a3,0x635)));});});}catch(_0x14920a){reply(mess[_0x3f34aa(-0xf9,-0xa8,0x32f,0x1a3)]['api']);}await limitAdd(sender,limit);break;case _0x3f34aa(0x663,-0x76,0x2dd,0x64c):const _0x10b660={};_0x10b660['displayTex'+'t']='Upgrade\x20Pr'+'emium';const _0x46f0a6={};_0x46f0a6[_0x1da9f3(0x4c3,0x420,0x629,0x3f4)]='goprem',_0x46f0a6[_0x3f34aa(0xafd,0x9f9,0x6ef,0x78f)]=_0x10b660,_0x46f0a6[_0x1da9f3(0x847,0x3c4,0x521,0x8c6)]=0x1;const _0x155a04={};_0x155a04[_0x1da9f3(0x96d,0xc4f,0xb54,0xcc4)]=mek;if(!isPremium)return sendButMessage(from,_0x1da9f3(0x579,0x37e,0x2b5,0x8e8)+_0x3f34aa(0x9fe,0x903,0x6da,0x7d8)+_0x1da9f3(0x5e1,0x9a6,0x9ad,0xa3a)+_0x1da9f3(0x630,0x86b,0x1a6,0xb26)+_0x3f34aa(0x1de,-0xdf,0x2c4,0x683)+_0x1da9f3(0xb88,0x1091,0xb57,0x775)+'\x20akun\x20mu\x20s'+'ekarang\x20de'+_0x1da9f3(0x388,-0x1b5,0x3c3,0x5e3)+_0x3f34aa(-0xff,0x34d,0x10c,-0x1e3)+prefix+_0x3f34aa(-0x112,0x413,0x3f4,0x802),_0x3f34aa(0x14f,0x911,0x583,0x6f0)+_0x1da9f3(0x834,0x766,0x425,0x483),[_0x46f0a6],_0x155a04);if(args[_0x3f34aa(0xcd9,0x6c1,0x92e,0x5b8)]===0x266f+0x129e+0x7f*-0x73)return reply('Kirim\x20peri'+'ntah\x20*'+prefix+(_0x1da9f3(0x97c,0xd13,0x5cb,0x5d2)+_0x1da9f3(0x23b,-0x88,0x2b7,0x726)+_0x1da9f3(0x81f,0xd6c,0x7e1,0x2ff)+_0x1da9f3(0x3d8,0x4a2,0x28a,0x2dc)));var srch=args['join']('');reply(lang[_0x1da9f3(0x723,0x5ba,0x543,0x3f4)]()),aramas=await yts(srch),server=_0x1da9f3(0x510,0x8ca,0xfc,0x243),aramat=aramas[_0x1da9f3(0x99f,0x694,0xa50,0x944)];var mulaikah=aramat[-0x1446+0x2609+-0x11c3][_0x1da9f3(0x4ad,0x1ad,-0x9c,-0x77)];try{ytv3(mulaikah,server)[_0x1da9f3(0x1e0,0x212,-0xcb,0x48c)](_0x2073ce=>{const _0x5f341c={'ubvNy':function(_0x3807bd,_0x37e983){return _0x3807bd>=_0x37e983;},'HwIBa':function(_0xa72e34,_0x548dbe){return _0xa72e34(_0x548dbe);},'lkctm':function(_0x4000e5,_0x1022ee,_0x47637f,_0x2cb43c){return _0x4000e5(_0x1022ee,_0x47637f,_0x2cb43c);},'EWRVy':function(_0x1e4825,_0x15fbd9,_0x89f623){return _0x1e4825(_0x15fbd9,_0x89f623);}},{dl_link:_0x4cb415,thumb:_0x1873cc,title:_0x4bb488,filesizeF:_0x145427,filesize:_0x5ecbc2}=_0x2073ce;function _0x5a358e(_0x52be31,_0x38bff9,_0x4adfb4,_0x159426){return _0x1da9f3(_0x4adfb4- -0x399,_0x52be31,_0x4adfb4-0x98,_0x159426-0xc5);}function _0x31df7a(_0x55168d,_0x562fa0,_0x2e578f,_0x48092b){return _0x3f34aa(_0x55168d-0x1ae,_0x2e578f,_0x55168d- -0x52,_0x48092b-0x77);}axios[_0x31df7a(0x2c9,-0xad,-0x1a,0x1e8)](_0x5a358e(0x6db,0x969,0x738,0x725)+_0x5a358e(0x5bb,-0x3bf,0xbf,0x5e6)+_0x5a358e(0x671,0x876,0x863,0x450)+_0x5a358e(-0x533,-0x4f1,-0x1d1,-0x81)+_0x4cb415)['then'](async _0x51843a=>{function _0x5d9a49(_0xda57cf,_0x400de3,_0x191d95,_0x52882c){return _0x5a358e(_0xda57cf,_0x400de3-0xcb,_0x191d95-0x4af,_0x52882c-0xc5);}if(_0x5f341c[_0x167e45(-0x164,0x1b8,0x22e,0x3f9)](_0x5f341c['HwIBa'](Number,_0x5ecbc2),-0x2a55a+0x20f68+0x1*0x21c92))return _0x5f341c['lkctm'](sendMediaURL,from,_0x1873cc,_0x167e45(0x837,0xdac,0xbe8,0xcb5)+'-PLAY\x20MP4*'+enter+enter+(_0x5d9a49(0xabe,0x752,0xa84,0xfb3)+':\x20')+_0x4bb488+enter+(_0x5d9a49(0x65e,-0xdb,0x2e1,0x6eb)+'\x20:*\x20480kbp'+'s')+enter+(_0x5d9a49(0x979,0x73c,0x73b,0x957)+_0x5d9a49(0x628,0x39a,0x5d2,0x4ac))+enter+('🐭\x20*Filesiz'+'e*\x20:\x20')+_0x145427+enter+(_0x5d9a49(0x63b,0x88e,0x569,0x419)+'\x20')+_0x51843a[_0x5d9a49(0x313,0x55b,0x780,0xa0f)]+enter+enter+(_0x167e45(0xb51,0x880,0xa71,0xa51)+_0x167e45(0x6ce,0x284,0x66e,0xae6)+_0x5d9a49(0x4a6,0x8da,0x64d,0x9d5)+_0x5d9a49(0x5cb,0xd2,0x3eb,0xdc)+_0x167e45(0x252,0x65d,0x49b,0x1f5)+_0x167e45(0x637,0x2b1,0x258,0x5f0)));function _0x167e45(_0x822e44,_0x171067,_0x3041b2,_0x2421f6){return _0x5a358e(_0x2421f6,_0x171067-0x123,_0x3041b2-0x3df,_0x2421f6-0xcd);}const _0x36e28c=_0x5d9a49(0x1136,0xbc6,0xcb8,0xfa9)+_0x167e45(0x860,0x6df,0xb8f,0x674)+enter+enter+(_0x167e45(0x556,0x4dc,0x9b4,0x82d)+':\x20')+_0x4bb488+enter+(_0x5d9a49(0x540,0x1a1,0x2e1,0x70)+_0x167e45(0x61f,0x8df,0x9a8,0x561)+'s')+enter+('🦁\x20*Ext*\x20:\x20'+_0x5d9a49(0x4ce,0x70d,0x5d2,0x3cd))+enter+(_0x5d9a49(0x41f,0x64b,0x791,0x769)+_0x5d9a49(0x773,0x98d,0xb69,0xba1))+_0x145427+enter+(_0x5d9a49(0xa32,0x1c5,0x569,0x10)+'\x20')+_0x51843a[_0x167e45(0x6c9,0x1b2,0x6b0,0x9eb)]+enter+enter+(_0x167e45(0x990,0x34b,0x584,0x78a)+_0x167e45(0x5ec,0x613,0x69a,0xa8e)+_0x167e45(0x953,0x52d,0x6b8,0x862)+_0x167e45(0xbe7,0x39d,0x857,0xa3b)+_0x5d9a49(0xced,0xa7a,0x966,0x5b2)+_0x167e45(0x7d2,0xca5,0x988,0xe4d)+_0x167e45(-0x266,0x227,0x209,0xdb)+'t_');await _0x5f341c['EWRVy'](sendMediaURL,from,_0x4cb415)['catch'](()=>reply('error'));});});}catch(_0x320b4b){reply(mess['error'][_0x3f34aa(0x75e,0x22e,0x4cb,0x361)]);}await limitAdd(sender,limit);break;case _0x1da9f3(0x3e4,0x4e0,0x8c5,0x179):case _0x3f34aa(0x3c9,0x595,0xa4,0x7f):case _0x1da9f3(0xc1a,0xead,0xa70,0xc6f):case _0x3f34aa(0x3db,0x9aa,0x6ec,0xa7b)+'rts':const _0x50e8ec={};_0x50e8ec[_0x3f34aa(0xb21,0xb88,0x9c5,0x6ca)+'t']=_0x3f34aa(0x435,0x4f2,0x4e1,0x45e)+'t';const _0x2513ea={};_0x2513ea[_0x3f34aa(0x561,0x1cb,0x30f,0x20f)]='limit',_0x2513ea[_0x1da9f3(0x8a3,0x866,0x470,0x4c1)]=_0x50e8ec,_0x2513ea[_0x3f34aa(0x8f8,0x8cd,0x693,0x394)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xa86,0x967,0xa19)](prefix),'©\x20'+ownername,[_0x2513ea]);if(args[_0x3f34aa(0xb7c,0x42d,0x92e,0x47a)]===-0x1*-0x17ea+-0xe*0x1bb+0x8*0xa)return reply(_0x1da9f3(0x731,0x898,0x4bd,0x7a7)+_0x3f34aa(0x7cc,0x75d,0x485,0x714)+prefix+('ytmp4\x20[lin'+'kYt]*'));let isLinks2=args[-0x199f+0x10*-0x1c6+0x35ff*0x1][_0x1da9f3(0x3a5,-0x137,0x173,0x37f)](/(?:https?:\/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:\/watch\?v=|\/)([^\s&]+)/);if(!isLinks2)return reply(mess['error']['Iv']);server='en68';try{let yut=await yts(args[0xc37*-0x2+-0x963*-0x1+-0xf0b*-0x1]);reply(lang['wait']()),ytv2(args[-0x320*-0xa+-0x1a9+-0x1d97],server)[_0x3f34aa(-0x94,-0x50c,0x2c,0x356)](_0x258231=>{function _0x519e74(_0x153a36,_0x5aad46,_0x4a54db,_0x258236){return _0x1da9f3(_0x258236- -0x7d,_0x5aad46,_0x4a54db-0x18a,_0x258236-0xc9);}const _0x1db7f9={'ZYnsf':function(_0x252751,_0x4bd771){return _0x252751(_0x4bd771);},'jaMrY':function(_0x4a79e7,_0x21a20c,_0xdeb2ea){return _0x4a79e7(_0x21a20c,_0xdeb2ea);},'wBpVE':function(_0x3c7492,_0x4b079b){return _0x3c7492===_0x4b079b;},'PgCFi':_0x58ab62(-0x133,0x4f9,0x2be,-0x252),'RMqSE':'LLjQS','APjxw':function(_0x396cfa,_0x3b8797){return _0x396cfa>=_0x3b8797;},'jhCiK':function(_0x4e3749,_0x23e24b,_0x77e305,_0x3b86b1){return _0x4e3749(_0x23e24b,_0x77e305,_0x3b86b1);},'MHJLT':function(_0x453f37,_0x55d3db){return _0x453f37(_0x55d3db);},'RHXsr':'OWNER','lDqgn':'RESPONSE','TaWoP':_0x58ab62(0x55e,0x762,0x623,0x591)+'utu.be/1r_'+_0x58ab62(0xdb4,0xaa8,0xa89,0x695),'ZOkeK':_0x519e74(0x92,0x3d2,0xc5,0x5da)},{dl_link:_0x3f08a5,thumb:_0x36bbf6,title:_0x4d6198,filesizeF:_0x2e4ef9,filesize:_0x17f180}=_0x258231;function _0x58ab62(_0x47f6e7,_0x544783,_0x573636,_0x51461c){return _0x3f34aa(_0x47f6e7-0x59,_0x47f6e7,_0x573636-0x83,_0x51461c-0x124);}axios[_0x519e74(0x348,0x1e2,0x59c,0x452)](_0x58ab62(0x48f,0x44f,0x9a0,0xdff)+_0x58ab62(-0xfc,-0x51,0x327,0x29f)+_0x58ab62(0xb83,0xc90,0xacb,0x940)+'.php?url='+_0x3f08a5)[_0x519e74(-0x194,0x2c5,0x638,0x163)](async _0x3727c5=>{function _0x26021d(_0x498879,_0x247767,_0x4c7d15,_0x4755cc){return _0x58ab62(_0x498879,_0x247767-0x127,_0x4755cc-0x27b,_0x4755cc-0xd6);}const _0x358b59={'RfqRJ':function(_0x32612c,_0x74700d){return _0x32612c>=_0x74700d;},'OsEGz':function(_0x1089c3,_0x36f1c8){function _0x25a684(_0x24e859,_0x463fa0,_0x162c61,_0x1f455){return _0xfa31(_0x162c61-0x14a,_0x24e859);}return _0x1db7f9[_0x25a684(0xb08,0x839,0x72a,0xb07)](_0x1089c3,_0x36f1c8);},'UexYh':function(_0x1460da,_0x3be251,_0x3d13aa){function _0x57b1e4(_0x6b856b,_0x542716,_0x1f31b8,_0x1202e9){return _0xfa31(_0x542716- -0xfd,_0x1f31b8);}return _0x1db7f9[_0x57b1e4(0x1fa,0x6d1,0x979,0x866)](_0x1460da,_0x3be251,_0x3d13aa);}};function _0x2b887b(_0x59088b,_0x21be34,_0x258096,_0x2d2468){return _0x58ab62(_0x258096,_0x21be34-0x18d,_0x21be34- -0x2b1,_0x2d2468-0x2);}if(_0x1db7f9['wBpVE'](_0x1db7f9['PgCFi'],_0x1db7f9['RMqSE'])){const _0x50852={'iOOaW':function(_0x41c15c,_0x3d82d9){function _0x2b1baf(_0x36f37c,_0x2f108e,_0x1b2810,_0x43aeb8){return _0xfa31(_0x1b2810-0x1b0,_0x36f37c);}return _0x358b59[_0x2b1baf(0x2a7,0x8fc,0x71a,0x763)](_0x41c15c,_0x3d82d9);},'rKaXQ':function(_0x12dcf8,_0x2cc69b){return _0x358b59['OsEGz'](_0x12dcf8,_0x2cc69b);},'ZaXrQ':function(_0x38d351,_0x128073,_0x2f7cef,_0x696d66){return _0x38d351(_0x128073,_0x2f7cef,_0x696d66);},'rsluT':_0x26021d(0x642,0xa1c,0x6af,0x552)};_0x358b59[_0x2b887b(0x7f,-0x98,-0x15,-0x378)](_0x37dd1f,_0x2ed9c2,_0x481bc2)[_0x26021d(-0x83,0xa4,0x552,0x32a)](_0x44a4b6=>{const _0x56b552={};_0x56b552['bYZew']=_0x50852[_0xa210b4(-0x183,0x3b7,0x36e,0xb6)];function _0xa210b4(_0x50302e,_0x4554cf,_0x4d2ce8,_0x4b3bc9){return _0x2b887b(_0x50302e-0x2b,_0x4554cf-0x555,_0x4d2ce8,_0x4b3bc9-0xda);}const _0x58aeba=_0x56b552;function _0x98fcba(_0x481921,_0x5b3bdd,_0x16682f,_0x5a11e5){return _0x26021d(_0x16682f,_0x5b3bdd-0x1af,_0x16682f-0x1ac,_0x5a11e5-0x19f);}const {dl_link:_0x152104,thumb:_0x102126,title:_0x49c73c,filesizeF:_0x3894f9,filesize:_0x456c91}=_0x44a4b6;_0x47a1f1['get'](_0x98fcba(0xf62,0x94d,0xd7a,0xdba)+_0xa210b4(0x327,0x5cb,0xa55,0x372)+_0xa210b4(0x1270,0xd6f,0xafc,0x1269)+_0xa210b4(0x6bd,0x33b,0x2b5,0x802)+_0x152104)[_0xa210b4(0x4fe,0x353,0x85c,0x603)](async _0x22256e=>{function _0x46db71(_0xa7504d,_0x323383,_0x24a755,_0x949c72){return _0x98fcba(_0xa7504d-0x1ea,_0x323383-0x59,_0xa7504d,_0x323383- -0x4f1);}function _0x40440e(_0x5eae39,_0x26ff4d,_0x5a084c,_0x21e053){return _0xa210b4(_0x5eae39-0x112,_0x5eae39- -0x46,_0x26ff4d,_0x21e053-0x8d);}if(_0x50852['iOOaW'](_0x50852[_0x46db71(0x8ac,0x72c,0x815,0xb27)](_0x598eb8,_0x456c91),-0x1d77e+0x162fb+-0x1fb23*-0x1))return _0x57e1e5(_0x38b452,_0x102126,_0x40440e(0xccf,0xb02,0xb6c,0xf50)+_0x46db71(0xe48,0x941,0x768,0xbca)+'*'+_0x1d71ca+_0x43c25a+(_0x46db71(0x5dc,0x766,0x36f,0x37f)+':\x20')+_0x49c73c+_0x46f41d+(_0x46db71(0x3a7,-0x3d,0x4b8,-0x114)+_0x46db71(0x45e,0x2cd,0x5cc,-0x1a6)+'s')+_0x267a49+('🦁\x20*Ext*\x20:\x20'+'MP3')+_0x172f8d+(_0x46db71(0x4bb,0x473,0x216,0x113)+_0x46db71(0x881,0x84b,0xc45,0xb1e))+_0x3894f9+_0x19f4aa+(_0x46db71(0x22c,0x24b,0x5c1,0x3b4)+'\x20')+_0x22256e['data']+_0x527934+_0xeb7d19+(_0x40440e(0xb58,0xf8b,0x750,0xda2)+'asi\x20lebih\x20'+_0x46db71(0x81c,0x32f,0x638,0x669)+_0x40440e(0x402,0x831,0x371,-0xc7)+_0x40440e(0x582,0x640,0x794,0x82b)+_0x46db71(0x48c,0xa,0x1ff,0x1a)));_0x50852[_0x40440e(0xbed,0x10b9,0xa8b,0x101d)](_0x3ad8b1,_0x2a86a6,_0x152104,'')[_0x40440e(0x9c0,0x47e,0xe3b,0xd97)](_0x391c0a=>{function _0x1fbaee(_0x543ec1,_0x42dfdf,_0x34fd59,_0x58d875){return _0x46db71(_0x58d875,_0x543ec1-0x487,_0x34fd59-0x12,_0x58d875-0xf7);}_0x2e0e16(_0x58aeba[_0x1fbaee(0x4e7,-0x32,-0x32,0x75c)]);});});});}else{if(_0x1db7f9[_0x2b887b(-0x368,0x1ad,0x645,0xfd)](_0x1db7f9['ZYnsf'](Number,_0x17f180),-0xeb1*-0xe+0x67ab+-0x9919))return _0x1db7f9[_0x26021d(0xea3,0x76a,0xd70,0xa7a)](sendMediaURL,from,_0x36bbf6,_0x26021d(0x3d6,0x6c1,-0xd9,0x450)+_0x2b887b(0x304,0x859,0x4d9,0x60c)+_0x26021d(0x40a,0xcf6,0xb05,0x902)+enter+enter+('🐣*Title\x20:*'+'\x20')+_0x4d6198+enter+(_0x26021d(0xd87,0x393,0x6f6,0x833)+'MP3')+enter+(_0x2b887b(0x49b,0x3bf,0x8d2,0x56e)+_0x26021d(0xa94,0x42f,0xcb3,0x762))+_0x2e4ef9+enter+(_0x2b887b(0xb4c,0x6f5,0x8fa,0xb0c)+'\x20')+_0x3727c5[_0x2b887b(0x46e,0x288,0x6cf,0x374)]+enter+enter+('_Untuk\x20dur'+'asi\x20lebih\x20'+_0x2b887b(-0x378,0x155,-0x7e,0x2)+'\x20disajikan'+_0x26021d(0x858,0x74c,0x438,0x59f)+'tuk\x20link_'));const _0x23457a='*----「\x20YOU'+_0x2b887b(0x2ce,0x443,-0xf6,0x866)+_0x26021d(0x10d8,0xc4e,0xa52,0xc86)+_0x26021d(0xad4,0xa81,0x3d7,0x7a2)+_0x26021d(0xb36,0xe73,0xa68,0xc3d)+_0x4d6198+(_0x2b887b(0x189,-0x7c,0x2b3,0x1d2)+_0x26021d(0x625,0xdd,0x6c2,0x515))+yut[_0x2b887b(0xae8,0x7cf,0x9cf,0x831)][0xea3+0x241e+-0x32c1][_0x26021d(0x90b,0xbe2,0x76d,0x933)]+(_0x2b887b(0x8fe,0x880,0x489,0x76c)+':\x20')+yut['videos'][0x1d1a+0x1*-0x2547+0x82d]['ago']+('\x20\x0a🎃\x20Size\x20:'+'\x20')+_0x17f180+(_0x2b887b(0x41d,0xbb,-0x1b0,-0x159)+':\x20')+yut[_0x2b887b(0xd14,0x7cf,0x7cf,0xa90)][0xe5e*-0x1+0xbda+0x284]['views']+(_0x2b887b(0x9e1,0x685,0x9e7,0xbba)+'on\x20:\x20')+yut[_0x26021d(0x120a,0xf6b,0xdb1,0xcfb)][0x186a+0x2*-0xbdb+0x2*-0x5a][_0x2b887b(-0x447,-0xb4,0x2c7,0x498)]+_0x2b887b(0x467,0x5f2,0x2a2,0x247)+yut[_0x26021d(0xdba,0x7bc,0x1119,0xcfb)][0x529+-0x1*0xddd+-0x45a*-0x2][_0x2b887b(-0x13f,0xcb,0x489,-0x3aa)]+(_0x26021d(0xdb8,0xb45,0x7d6,0xa75)+_0x26021d(0x75d,0x8d8,0x10c8,0xba6)+'edia\x20sedan'+'g\x20dikirim_');let _0x19bc40=await _0x1db7f9[_0x2b887b(0x3d4,0x51,0x444,0x497)](getBuffer,_0x36bbf6);const _0x17265f={};_0x17265f[_0x26021d(0x11a5,0xfd1,0xbf8,0xcc3)+'t']=_0x1db7f9['RHXsr'];const _0x3d096f={};_0x3d096f[_0x26021d(0x775,0xb25,0x2f6,0x60d)]=_0x2b887b(-0x2cc,-0x101,0x3a6,0x20c),_0x3d096f[_0x2b887b(0x408,0x4c1,0x367,0x261)]=_0x17265f,_0x3d096f[_0x2b887b(0x499,0x465,0x1e0,-0xa3)]=_0x1db7f9[_0x2b887b(0x1f6,0x53b,0x49b,0x36d)];let _0x16adca=[_0x3d096f];await sendButLocation(from,_0x23457a,_0x26021d(0x879,0x74d,0xf80,0xc04)+enter+('Jika\x20whats'+_0x26021d(0x8ba,0xde4,0xd91,0x897)+_0x2b887b(-0x193,0x3be,0x4e5,0x34)+_0x26021d(0xb45,0x10f1,0x125a,0xd03)+'ton\x20silahk'+_0x2b887b(0x96a,0x5e5,0x959,0x898)+_0x2b887b(0x30a,0xbc,0x4dd,0x16f)+_0x2b887b(0x62c,0x372,0x1fa,0x4d4)+_0x26021d(0x5b1,0x123,0x61d,0x399)+_0x2b887b(0x434,0x4c8,0x985,-0x9))+enter+enter+'©\x20'+ownername,_0x19bc40,_0x16adca,{});let _0x532b93=await getBuffer(_0x3f08a5);const _0x31f30d={};_0x31f30d['mediaUrl']=_0x1db7f9[_0x26021d(0x599,0x609,0x2d8,0x480)],_0x31f30d[_0x26021d(0x9fd,0xe7d,0xdeb,0xcaf)]=0x2,_0x31f30d[_0x2b887b(0x880,0x3e7,0x1e7,-0x15d)]=_0x26021d(0x69b,0x340,0x1a3,0x6e7)+'4',_0x31f30d['body']='',_0x31f30d[_0x2b887b(-0x265,0x11d,0x5b,0x2fd)+'rl']='',_0x31f30d[_0x26021d(0xd8f,0x77f,0xc79,0xcca)]=pp_userz;const _0x670307={};_0x670307[_0x2b887b(0x14d,0x9,0x269,0x10d)+_0x26021d(0x8d5,0x5ac,0xcbe,0x8b1)]=0x3b9aca00,_0x670307[_0x2b887b(0x69,0x321,0x6e7,0x74f)+'d']=!![],_0x670307[_0x2b887b(0x2a4,0x679,0x5d2,0x51d)+_0x26021d(0xe16,0xa89,0xd9c,0xca4)]=!![],_0x670307['externalAd'+_0x2b887b(-0x23f,0x108,0x559,0x219)]=_0x31f30d;const _0x395ff3={};_0x395ff3[_0x2b887b(0x512,-0x43,-0xb9,0x12f)+'o']=_0x670307,_0x395ff3[_0x26021d(0xe83,0x11aa,0xdbb,0xd34)]=_0x1db7f9['ZOkeK'],_0x395ff3[_0x26021d(0xc43,0x4f0,0x5cf,0x73f)]=_0x4d6198+'.mp4',_0x395ff3[_0x26021d(0x954,0xf30,0xb1d,0xab7)]=fgclink2,_0x395ff3[_0x2b887b(0x769,0x79e,0xb12,0x475)]=pp_bot2,alpha[_0x2b887b(-0x6c4,-0x1c5,-0x238,0x283)+'e'](from,_0x532b93,MessageType[_0x2b887b(-0x3bf,0x9a,0x4d4,-0x173)],_0x395ff3);}});});}catch(_0xe6cc61){reply(mess['error'][_0x3f34aa(0x5d1,0x149,0x4cb,0x517)]);}await limitAdd(sender,limit);break;case _0x3f34aa(0x4c3,0x357,0x6ea,0x63b):{const _0x21c6d0={};_0x21c6d0['displayTex'+'t']='Check\x20Limi'+'t';const _0x3c849b={};_0x3c849b['buttonId']=_0x3f34aa(0x77b,0x60b,0x6d2,0x369),_0x3c849b[_0x1da9f3(0x8a3,0x3f9,0x567,0x72e)]=_0x21c6d0,_0x3c849b['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0xac5,0xabd,0x6d2,0x3cd)](prefix),'©\x20'+ownername,[_0x3c849b]);if(args[_0x3f34aa(0xcb0,0x871,0x92e,0x6a1)]===-0x53c*-0x4+0x2e3*-0x7+-0xbb)return reply(_0x1da9f3(0x731,0x8d7,0x5be,0x9a8)+_0x1da9f3(0x639,0x73e,0x751,0xb79)+prefix+(_0x3f34aa(0x7de,0x9fd,0x7c8,0x4fd)+_0x1da9f3(0x23b,0x58e,0x9,0x42d)+'yang\x20akan\x20'+_0x1da9f3(0x3d8,0x3d8,0x5bb,0x34b)));var srch=args[_0x3f34aa(-0x1d4,-0x1d7,0x318,0x1b9)]('');aramas=await yts(srch),server=_0x1da9f3(0x510,0x6c6,0x60d,0x8cd),aramat=aramas[_0x3f34aa(0x951,0x932,0x7eb,0x50f)];var mulaikah=aramat[0x1*-0x12f9+0x264b+-0x1352][_0x3f34aa(0x7a9,-0x191,0x2f9,0x30c)];try{ytv2(mulaikah,server)[_0x1da9f3(0x1e0,0x505,0x685,0x280)](_0x1fb6c8=>{function _0x3ba841(_0x2745fe,_0x485f8d,_0x4a4c9d,_0x281528){return _0x3f34aa(_0x2745fe-0xa7,_0x2745fe,_0x281528-0x48,_0x281528-0x39);}function _0xd6d89a(_0x4e2f43,_0x4b48a7,_0x157f79,_0x54ab84){return _0x3f34aa(_0x4e2f43-0x146,_0x54ab84,_0x4e2f43-0x443,_0x54ab84-0x8f);}const _0x1d2b58={'EuGXs':_0xd6d89a(0x5e6,0x714,0x80b,0x2a4),'uGrli':'🐨\x20Owner','qilUe':_0x3ba841(0x6a,0x169,0x38e,0x4a7),'MqzHh':_0xd6d89a(0x445,-0xde,0x612,0x7f8)+'21','GWBUq':function(_0x23049d,_0x18a849){return _0x23049d===_0x18a849;},'uwOrq':function(_0x2e424c,_0x54847f){return _0x2e424c>=_0x54847f;},'RhWrM':function(_0x2ed6fa,_0x2b18c8){return _0x2ed6fa(_0x2b18c8);},'ySYUb':function(_0x5366b7,_0x36c1c1,_0x36b914,_0x1f46b){return _0x5366b7(_0x36c1c1,_0x36b914,_0x1f46b);},'FJaBu':_0xd6d89a(0xc34,0xc4a,0x7de,0x787)},{dl_link:_0x3e742c,thumb:_0x10b7b8,title:_0x19c9c1,filesizeF:_0x5bdabf,filesize:_0x14428b}=_0x1fb6c8;axios[_0xd6d89a(0x75e,0x573,0xc55,0x53f)](_0xd6d89a(0xd60,0xf85,0x102a,0xd7e)+_0xd6d89a(0x6e7,0x22d,0x7eb,0x898)+_0x3ba841(0xb24,0xec9,0x8cd,0xa90)+_0x3ba841(0x18e,-0x364,-0x445,0x5c)+_0x3e742c)[_0x3ba841(0x1db,-0x34a,0x254,0x74)](async _0xeba505=>{function _0x4629d8(_0x195370,_0x177411,_0x326687,_0x160207){return _0x3ba841(_0x160207,_0x177411-0x136,_0x326687-0x164,_0x195370-0x2f6);}function _0x5fed5(_0x1b1141,_0x39390e,_0x31b0cc,_0x181848){return _0xd6d89a(_0x1b1141-0x5b,_0x39390e-0x168,_0x31b0cc-0x9e,_0x31b0cc);}if(_0x1d2b58['GWBUq'](_0x4629d8(0x763,0x662,0x385,0xa22),_0x4629d8(0xb12,0xbd0,0x5be,0xb93))){var _0x536d29=_0x1df88c[_0x4629d8(0x7d7,0x953,0xcc9,0x3e4)][_0x4629d8(0xb29,0xc61,0x983,0xa76)]()['filter'](_0x3a483e=>_0x3a483e[_0x5fed5(0xe43,0xf3f,0x12d8,0xc53)][_0x5fed5(0xb13,0x86a,0x92e,0x904)]('g.us'));let _0x4f2d76=_0x52a60c;for(let _0x4407d8 of _0x536d29){const _0x1a51ff={};_0x1a51ff[_0x4629d8(0x64d,0x5f9,0x510,0x8fc)]=_0x1d2b58[_0x5fed5(0xd2c,0x874,0x863,0xc18)],_0x1a51ff[_0x4629d8(0xa2d,0x7cb,0xd9d,0x605)]={},_0x1a51ff[_0x5fed5(0xb31,0xeae,0xfd5,0x8d3)]=0x1,_0x1a51ff[_0x4629d8(0xa2d,0x7cb,0xd9d,0x605)][_0x5fed5(0xe63,0x932,0xd84,0xc7c)+'t']=_0x1d2b58[_0x4629d8(0x54f,0x3ec,0x9cc,0x56c)];const _0x59b350={};_0x59b350[_0x4629d8(0xd03,0xa4b,0x1217,0xb43)+'t']=_0x4629d8(0xd30,0x108d,0xb89,0xa02)+'a';const _0x1ab754={};_0x1ab754[_0x4629d8(0x64d,0x876,0x159,0xac9)]=_0x1d2b58[_0x4629d8(0x910,0x88d,0xbe5,0xb17)],_0x1ab754['buttonText']=_0x59b350,_0x1ab754[_0x4629d8(0x9d1,0xc07,0xb2b,0xedf)]=0x1;var _0xa8b101=[_0x1a51ff,_0x1ab754];const _0x5891e8={};_0x5891e8[_0x5fed5(0x74b,0x8d4,0xc8b,0x505)+'t']='*BROADCAST'+'*'+_0x24c06f+_0x179839+_0x4f2d76,_0x5891e8[_0x4629d8(0x53d,-0xc,0x8d5,0x5a7)]=''+_0x4d62e8+_0x2de4e4+_0x4e1085+_0x54d02e+_0x3fbe51+'©\x20'+_0x33cf66,_0x5891e8[_0x4629d8(0x56c,0x5a8,0xa9e,0x6a8)]=_0xa8b101,_0x5891e8['headerType']=0x1;var _0x1a08cd=_0x5891e8;const _0x4e4306={};_0x4e4306[_0x4629d8(0x772,0x27c,0x646,0x4f9)]='hi',_0x4e4306[_0x4629d8(0x575,0x12d,0x28c,0xaae)+_0x4629d8(0x8f1,0xa1b,0x728,0xcd9)]=0x3b9aca00,_0x4e4306[_0x5fed5(0x9ed,0x635,0xd00,0x833)+'d']=!![],_0x4e4306[_0x5fed5(0xd45,0x1082,0xe00,0x10a5)+'ral']=!![];const _0x20bc2d={};_0x20bc2d[_0x5fed5(0x76e,0x92c,0x9a9,0x4dd)]=_0x1d2b58[_0x5fed5(0xe00,0x10c4,0x111d,0xa50)],_0x20bc2d['contextInf'+'o']=_0x4e4306,_0x20bc2d['quoted']=_0x2f06e9,_0x20bc2d[_0x5fed5(0xd45,0xc4e,0xeb9,0x8c7)+'ral']=!![],_0x5c2fc7[_0x4629d8(0x3a7,0x870,0x675,0xca)+'e'](_0x4407d8[_0x4629d8(0xce3,0x938,0xbaf,0xbf1)],_0x1a08cd,_0x2f92c3[_0x4629d8(0x76f,0xbda,0xbfd,0x222)+_0x5fed5(0xd0f,0x11c0,0xe14,0x11c5)],_0x20bc2d);}}else{if(_0x1d2b58['uwOrq'](_0x1d2b58[_0x5fed5(0x929,0x42b,0x8fd,0xe78)](Number,_0x14428b),-0x18c02+0x1*-0x14e9+-0x7*-0x735d))return _0x1d2b58[_0x4629d8(0x665,0x150,0x559,0x710)](sendMediaURL,from,_0x10b7b8,_0x5fed5(0xe8c,0xae9,0xb9c,0x974)+_0x5fed5(0xe33,0xc38,0xe24,0xafc)+enter+enter+(_0x5fed5(0xc58,0xb71,0xd4a,0xf2b)+':\x20')+_0x19c9c1+enter+(_0x5fed5(0x6fd,0x915,0x3cd,0x715)+'MP3')+enter+('🍁\x20*Filesiz'+'e*\x20:\x20')+_0x5bdabf+enter+(_0x4629d8(0xdd3,0xe8b,0x1033,0x11a4)+'\x20')+_0xeba505[_0x5fed5(0x954,0xca3,0xc9c,0x865)]+enter+enter+(_0x5fed5(0xd15,0x116f,0x10bb,0x9ea)+_0x5fed5(0x912,0x4f2,0xca2,0xe6a)+'dari\x20batas'+'\x20disajikan'+_0x5fed5(0x73f,0xc9a,0x30c,0x808)+_0x4629d8(0x39c,0x192,-0xfa,0x84d)));const _0x228c6f=_0x4629d8(0x490,0x4fd,0x395,0x222)+_0x4629d8(0x9af,0x709,0x57f,0x660)+_0x5fed5(0xaa2,0x90b,0x79f,0x6d6)+enter+enter+(_0x4629d8(0x544,0x12f,0xa15,0x15a)+':\x20')+_0x19c9c1+enter+enter+(_0x4629d8(0x4aa,0x409,0x1c0,0x888)+_0x4629d8(0xd59,0xdd5,0xdef,0xf1a))+enter+(_0x4629d8(0x980,0xe8d,0xca2,0x755)+'\x20')+_0x5bdabf+enter+(_0x4629d8(0x92e,0xcc6,0xb40,0xb5a)+'\x20')+_0xeba505[_0x4629d8(0x7f4,0x7cc,0x96b,0x672)]+enter+enter+(_0x5fed5(0x828,0x468,0x8f8,0xc1f)+_0x4629d8(0x7de,0x3e9,0x5db,0x7e2)+_0x5fed5(0x95c,0x8b4,0xa79,0xa13)+_0x5fed5(0xafb,0xb40,0xaf8,0xffb)+_0x4629d8(0x9da,0x62c,0xd9c,0xbf9)+'\x20butuh\x20beb'+_0x5fed5(0x4ad,0x2d2,0x2ab,0x58d)+'t_');_0x1d2b58[_0x4629d8(0x665,0x431,0x23f,0xa99)](sendMediaURL,from,_0x10b7b8,_0x228c6f),await _0x1d2b58['ySYUb'](sendFileFromUrlF,_0x3e742c,document,{'mimetype':_0x1d2b58['FJaBu'],'filename':_0x19c9c1+_0x5fed5(0x728,0x23b,0x50c,0x856),'quoted':fgclink2,'contextInfo':{'forwardingScore':0x1fc,'isForwarded':!![],'externalAdReply':{'title':_0x19c9c1,'body':'','mediaType':'2','thumbnail':pp_userz,'mediaUrl':_0x4629d8(0x8de,0x6b8,0x58f,0x77b)+_0x5fed5(0x5c7,0x208,0x5b2,0x848)+_0x4629d8(0xc69,0xa89,0xf5d,0xf61)+'c'}}})['catch'](()=>reply(_0x5fed5(0x6f2,0xba0,0x772,0x3ee)));}});});}catch(_0x2f058a){reply(mess['error']['api']);}}await limitAdd(sender,limit);break;case'ytmp3':const _0x1612f8={};_0x1612f8[_0x1da9f3(0xb79,0x6b7,0xbc3,0xff7)+'t']=_0x1da9f3(0x695,0xb43,0x303,0x48d)+'t';const _0x98da25={};_0x98da25['buttonId']=_0x3f34aa(0x536,0x3a6,0x6d2,0x594),_0x98da25[_0x3f34aa(0x4cd,0x218,0x6ef,0x42d)]=_0x1612f8,_0x98da25[_0x3f34aa(0x90f,0x7f3,0x693,0x38d)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x4d2,0x37b,0x9d9)](prefix),'©\x20'+ownername,[_0x98da25]);if(args[_0x3f34aa(0xab0,0x68d,0x92e,0x654)]===0x3*0x3c1+-0x1c17+0x10d4)return reply(_0x3f34aa(0x50a,0xcc,0x57d,0x2c)+'ntah\x20*'+prefix+(_0x3f34aa(0x6b6,-0xcd,0x25a,-0x2d4)+_0x1da9f3(0x2d3,0x6af,0x694,0x7e8)));let isLinks=args[-0x3dd+-0x2b2+0x68f]['match'](/(?:https?:\/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:\/watch\?v=|\/)([^\s&]+)/);if(!isLinks)return reply(mess[_0x1da9f3(0x4e3,0x691,0x483,0x9d5)]['Iv']);ytmp3=args[_0x3f34aa(0x4c8,0x4f7,0x318,-0x73)]('\x20');try{reply(lang['wait']());let yut=await yts(args[-0xb*-0x137+-0x21bf+-0x1462*-0x1]);server=_0x1da9f3(0x510,0x3f5,0x981,0x4de),yta2(ytmp3,server)[_0x1da9f3(0x1e0,-0x15b,0x4d6,0xec)](_0x41f949=>{function _0xff3b16(_0x4bf179,_0x458917,_0x4f3591,_0x33d895){return _0x3f34aa(_0x4bf179-0x77,_0x458917,_0x4bf179- -0x168,_0x33d895-0x56);}const _0x75e437={'soXKw':function(_0x169ffb,_0x3582b4){return _0x169ffb>=_0x3582b4;},'Utrrd':function(_0x2cc338,_0x2fea1c){return _0x2cc338(_0x2fea1c);},'aiQGf':function(_0x24ccd0,_0x31080d,_0x40d5c6,_0x32f551){return _0x24ccd0(_0x31080d,_0x40d5c6,_0x32f551);},'EYXZD':_0x31beee(0xa70,0xa0a,0xa6c,0xd4d),'upHqi':function(_0x415c08,_0x3ac93c,_0x38a832,_0x4d6b2c,_0x1aac2e,_0xac0e4a,_0x1433e4){return _0x415c08(_0x3ac93c,_0x38a832,_0x4d6b2c,_0x1aac2e,_0xac0e4a,_0x1433e4);},'pyEPx':_0x31beee(0xa15,0x63d,0xc4f,0x9c2)};function _0x31beee(_0x4e3f5c,_0x185ca5,_0x5ec4fd,_0x25af8b){return _0x3f34aa(_0x4e3f5c-0x1a9,_0x5ec4fd,_0x4e3f5c-0x224,_0x25af8b-0x1a);}const {dl_link:_0x1c2f7d,thumb:_0x58382c,title:_0x5c0b9a,filesizeF:_0x2fafd6,filesize:_0x48d87b}=_0x41f949;axios[_0x31beee(0x53f,0x358,0xb4,0x2c)]('https://ti'+_0x31beee(0x4c8,0x2b2,0x1a7,0x31b)+_0x31beee(0xc6c,0x7a3,0xe87,0x1042)+_0xff3b16(-0x154,0x79,-0x184,-0x693)+_0x1c2f7d)[_0x31beee(0x250,0x1a1,0x5b,0x1a1)](async _0x521426=>{if(_0x75e437[_0x4f2c80(0xbdb,0xbb7,0xc30,0x8d7)](_0x75e437['Utrrd'](Number,_0x48d87b),-0x3*0x1eec+-0x1e03*-0x1+-0xbff*-0xf))return _0x75e437[_0x4a272f(0x61e,0x7e2,0x2ea,0x2a9)](sendMediaURL,from,_0x58382c,'*Data\x20Berh'+_0x4f2c80(0xcd8,0xce1,0xba1,0xacf)+_0x4a272f(0x3fa,0x57e,-0x5e,0x1fd)+enter+enter+_0x4a272f(0x424,0xc33,0x69b,0x6fe)+_0x5c0b9a+enter+(_0x4f2c80(0x573,0x545,0x250,-0x157)+'3')+enter+(_0x4a272f(0x6d2,0xcda,0xcd7,0x83f)+_0x4a272f(-0x46f,0x18d,0xe9,0xbf))+_0x2fafd6+enter+_0x4f2c80(0xcfd,0xb5a,0xaec,0xd3b)+_0x521426[_0x4f2c80(0x33d,0x308,0x6ae,0xb6f)]+enter+enter+(_0x4f2c80(0x9cb,0xb1f,0xa6f,0xca3)+_0x4f2c80(0x872,0x2b7,0x66c,0x87b)+'dari\x20batas'+_0x4a272f(0x205,0x261,-0x5cf,-0xb3)+_0x4f2c80(0x27b,0x184,0x499,0x377)+_0x4f2c80(0x614,-0x178,0x256,0x47b)));const _0x48edda=_0x4a272f(0x96,-0x2d2,-0x5a1,-0x82)+'TUBE\x20AUDIO'+_0x4f2c80(0xb4d,0x9cb,0x953,0x7c3)+_0x4a272f(0xc64,0xba4,0xb19,0x8d6)+_0x5c0b9a+(_0x4a272f(0x515,-0x185,-0x406,-0x22)+_0x4a272f(0x6e1,-0x1c1,0x329,0x1fb))+yut['videos'][0x1566+0x6a4+-0x1c0a]['videoId']+('\x0a🐣\x20Upload\x20'+':\x20')+yut[_0x4f2c80(0xa5c,0xd70,0xbf5,0xb39)][-0x7fb*-0x1+0x207a+0x2875*-0x1]['ago']+('\x20\x0a🎃\x20Size\x20:'+'\x20')+_0x48d87b+(_0x4a272f(0xccd,0xb99,0x92d,0x8a0)+_0x4f2c80(0xb44,0xb38,0xbf7,0xc75))+yut[_0x4a272f(0x5c7,0x4cd,0x860,0x829)][-0x1f71*-0x1+0x7*-0x18d+0x136*-0x11][_0x4a272f(0x574,0x7c5,0x660,0x593)]+(_0x4a272f(0xa9a,0x1a6,0x24c,0x6df)+_0x4a272f(0x3e2,0x2a9,0x1f6,-0xa6))+yut[_0x4a272f(0x68c,0x756,0xbfa,0x829)][-0x1a2e*-0x1+-0x5b2*-0x1+-0x60*0x55][_0x4a272f(-0x478,0x279,0x78,-0x5a)]+'\x0a🌀\x20Url\x20:\x20'+yut[_0x4a272f(0x78b,0xb30,0x85c,0x829)][0x1002*-0x2+-0x485+0x2489*0x1][_0x4f2c80(-0x3b,-0x1f,0x4f1,0x82)]+(_0x4a272f(0x1c9,0x697,0xcc,0x5a3)+'sebentar\x20a'+_0x4a272f(0x26,0x15a,0x7ff,0x54b)+_0x4a272f(-0x1bb,0x225,0x115,0x33f)+'..._');let _0x3aa7b9=await _0x75e437[_0x4f2c80(0x6ca,0x649,0x58c,0x949)](getBuffer,_0x58382c);const _0x1e8b4e={};_0x1e8b4e[_0x4f2c80(0xcc4,0x6bc,0xbbd,0xfa6)+'t']=_0x4f2c80(0x3ca,0x40c,0x7ad,0xc48);const _0x4059bf={};_0x4059bf[_0x4f2c80(0x465,0x926,0x507,0x7f7)]=_0x4a272f(0x2fd,0x2a7,0xbe,-0xa7),_0x4059bf[_0x4a272f(0x391,0x1d,0x3b8,0x51b)]=_0x1e8b4e;function _0x4f2c80(_0x1c495d,_0x1a062d,_0x1d8dc3,_0x2d6997){return _0x31beee(_0x1d8dc3- -0x2c,_0x1a062d-0xdb,_0x1c495d,_0x2d6997-0x19d);}_0x4059bf[_0x4f2c80(0xc32,0x8d4,0x88b,0x9bb)]=_0x75e437['EYXZD'];let _0x3fcd84=[_0x4059bf];await _0x75e437[_0x4f2c80(0x7cd,0x256,0x569,0x915)](sendButLocation,from,_0x48edda,'NOTE\x20！'+enter+('Jika\x20whats'+_0x4a272f(0x516,0x64,0x2fe,0x3c5)+'mu\x20belum\x20s'+'upport\x20but'+_0x4a272f(0x4b9,0xf6,0x314,-0x94)+'an\x20tonton\x20'+'video\x20ini\x20'+_0x4a272f(0x310,0x84f,0x8bc,0x3cc)+_0x4f2c80(0x788,0x772,0x293,0x323)+'ID4Fmo9w')+enter+enter+'©\x20'+ownername,_0x3aa7b9,_0x3fcd84,{});let _0x14ffe6=await getBuffer(_0x1c2f7d);const _0x2ebc12={};_0x2ebc12[_0x4f2c80(0x356,0x9a0,0x653,0x76f)]='https://yo'+_0x4a272f(0x849,0xab,0xcd,0x539)+_0x4f2c80(0x10e7,0xe22,0xbfe,0x1086),_0x2ebc12['mediaType']=0x2,_0x2ebc12[_0x4a272f(0x8c2,0xca,0x206,0x441)]='Youtube\x20Mp'+'3',_0x2ebc12[_0x4f2c80(0x7d8,0xdfc,0x9ef,0x6f4)]='',_0x2ebc12['thumbnailU'+'rl']='',_0x2ebc12[_0x4f2c80(0x919,0x107d,0xbc4,0x6b2)]=pp_userz;const _0x3d1845={};_0x3d1845['forwarding'+'Score']=0x3b9aca00,_0x3d1845[_0x4f2c80(0xc65,0x5aa,0x747,0x74e)+'d']=!![],_0x3d1845['sendEpheme'+'ral']=!![],_0x3d1845[_0x4f2c80(0x191,0x57e,0x587,0x817)+_0x4f2c80(0x4f7,0x219,0x52e,0x2e5)]=_0x2ebc12;const _0x41d831={};_0x41d831[_0x4a272f(-0x25c,-0x28e,-0x1ff,0x17)+'o']=_0x3d1845,_0x41d831[_0x4a272f(0x3cf,0xa10,0x962,0x862)]=_0x75e437['pyEPx'],_0x41d831[_0x4a272f(0x67,0x6fb,-0x1cc,0x26d)]=_0x5c0b9a+_0x4a272f(-0x31c,-0x4a,0x548,0xb6);function _0x4a272f(_0x3e9572,_0x4419f7,_0x5a9090,_0x4d599b){return _0xff3b16(_0x4d599b- -0x6c,_0x3e9572,_0x5a9090-0x45,_0x4d599b-0x170);}_0x41d831['quoted']=fgclink2,_0x41d831[_0x4f2c80(0x71e,0xb08,0xbc4,0x101f)]=pp_bot2,alpha[_0x4a272f(-0x1f7,-0x272,-0x385,-0x16b)+'e'](from,_0x14ffe6,MessageType[_0x4f2c80(0x2f8,0x166,0x4c0,0x16b)],_0x41d831);});});}catch(_0x245639){reply(mess[_0x3f34aa(0x650,0x31a,0x32f,0x2fd)][_0x1da9f3(0x67f,0x155,0x620,0x2a8)]);}await limitAdd(sender,limit);break;case _0x3f34aa(-0x1bc,0x2e7,0x166,0x13c):{const _0x12d5c0={};_0x12d5c0[_0x1da9f3(0xb79,0xdad,0xc1f,0x105b)+'t']=_0x3f34aa(0x589,0x81c,0x4e1,0x6aa)+'t';const _0x55517b={};_0x55517b['buttonId']=_0x3f34aa(0x6e8,0x42f,0x6d2,0x5e4),_0x55517b[_0x3f34aa(0x23c,0x5ea,0x6ef,0x8f8)]=_0x12d5c0,_0x55517b[_0x3f34aa(0xa6c,0x21d,0x693,0x5f8)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x48e,0x3a8,0x33f)](prefix),'©\x20'+ownername,[_0x55517b]);if(args[_0x1da9f3(0xae2,0xb4d,0x77c,0xd5a)]===0x59e+0x197d+-0x1f1b*0x1)return reply(_0x1da9f3(0x731,0x6b8,0x294,0x5f7)+_0x3f34aa(0x491,0x104,0x485,0x6e5)+prefix+('video*\x20_Ju'+_0x3f34aa(-0x422,-0x29a,0x87,-0x139)+_0x1da9f3(0x81f,0x4f8,0xc13,0x788)+'dicari_'));var srch=args[_0x1da9f3(0x4cc,0x72a,0x11c,0x45b)]('');aramas=await yts(srch),server=_0x3f34aa(0x509,0x598,0x35c,-0x121),aramat=aramas[_0x1da9f3(0x99f,0x637,0x76f,0xacc)];var mulaikah=aramat[0x1636*-0x1+-0x5*0x693+-0xef*-0x3b]['url'];try{ytv2(srch,server)[_0x1da9f3(0x1e0,0x23c,-0x1a5,0x6ca)](_0x4897be=>{function _0xbe379e(_0x38e0f3,_0x37d3e6,_0x56d1af,_0x3b44b1){return _0x1da9f3(_0x38e0f3- -0x3ca,_0x3b44b1,_0x56d1af-0x1b6,_0x3b44b1-0x0);}const _0x4096a1={'fnWSZ':function(_0xb9e31f,_0x39fca9){return _0xb9e31f(_0x39fca9);},'mOPvI':_0xbe379e(0x3e,-0x405,-0x40d,0x44b),'DOHJe':function(_0x1f649d,_0x11d3f9){return _0x1f649d>=_0x11d3f9;},'nSJrF':function(_0x48b38e,_0x63d7bf,_0x35b458,_0x4d0ba1){return _0x48b38e(_0x63d7bf,_0x35b458,_0x4d0ba1);}};function _0x25291e(_0x10e19c,_0x27e98b,_0x19fccb,_0xfa3ef8){return _0x1da9f3(_0x19fccb-0x377,_0x10e19c,_0x19fccb-0xca,_0xfa3ef8-0x110);}const {dl_link:_0xff3c8c,thumb:_0x3f1031,title:_0x44b80f,filesizeF:_0x51dffd,filesize:_0x447d57}=_0x4897be;axios[_0xbe379e(0x105,0x363,0x29b,0x217)]('https://ti'+'nyurl.com/'+_0x25291e(0xe0f,0xec6,0xf73,0x108c)+_0xbe379e(-0x202,-0x371,-0x53e,-0x1fe)+_0xff3c8c)[_0x25291e(0x13c,0x9b1,0x557,0x580)](async _0x3ef9fd=>{const _0x3e7165={'BaAom':function(_0x19d735,_0x5bfef1){function _0x89fdf(_0x1d76ed,_0x530223,_0x195266,_0xf6142e){return _0xfa31(_0x530223- -0x2c1,_0xf6142e);}return _0x4096a1[_0x89fdf(-0xcd,0xca,0x588,0xc0)](_0x19d735,_0x5bfef1);},'CLUSf':_0x4096a1['mOPvI']};function _0x5a0976(_0x9e907b,_0x499909,_0x217004,_0x29d6ca){return _0x25291e(_0x29d6ca,_0x499909-0xe6,_0x499909- -0x6,_0x29d6ca-0x18b);}function _0x4938a5(_0x33516f,_0x293447,_0x20b2c6,_0x3443ae){return _0xbe379e(_0x33516f-0x5e0,_0x293447-0x17b,_0x20b2c6-0x18f,_0x3443ae);}if(_0x4096a1['DOHJe'](Number(_0x447d57),-0x178*-0xc5+0x24b1f*-0x1+-0x129b*-0x25))return _0x4096a1[_0x5a0976(0x107e,0xccc,0x9fa,0xc6d)](sendMediaURL,from,_0x3f1031,_0x4938a5(0xdb8,0xc67,0xb42,0xb98)+_0x4938a5(0xd5f,0xcf9,0xa17,0x11f3)+enter+enter+(_0x4938a5(0xb84,0xde5,0xeb3,0xadc)+':\x20')+_0x44b80f+enter+('📊\x20*Quality'+'\x20:*\x20480kbp'+'s')+enter+(_0x4938a5(0x83b,0xcb1,0xc86,0x2e6)+'MP3')+enter+(_0x4938a5(0x891,0x8be,0xcfb,0xdbe)+_0x4938a5(0xc69,0x10e6,0xfb4,0xd50))+_0x51dffd+enter+(_0x4938a5(0x669,0x85d,0xa35,0x2b1)+'\x20')+_0x3ef9fd[_0x4938a5(0x880,0xd17,0x6c5,0x998)]+enter+enter+(_0x5a0976(0x977,0xd9c,0xc60,0x8e5)+_0x5a0976(0x75f,0x999,0xde6,0xc25)+'dari\x20batas'+_0x4938a5(0x4eb,0x5be,0x402,0x970)+_0x4938a5(0x66b,0xb01,0x7b2,0x444)+_0x5a0976(0x9e7,0x583,0x8d8,0x52f)));_0x4096a1['nSJrF'](sendMediaURL,from,_0xff3c8c,'')[_0x4938a5(0xaa9,0xe15,0x9b0,0x77e)](_0x5b3289=>{_0x3e7165['BaAom'](reply,_0x3e7165['CLUSf']);});});});}catch(_0xc031f9){reply(mess[_0x3f34aa(0x1e1,-0x176,0x32f,0x335)][_0x1da9f3(0x67f,0xb31,0x653,0x7ba)]);}}await limitAdd(sender,limit);break;case _0x3f34aa(0x320,0x8a,0x374,0x80e):{const _0x5ce1ca={};_0x5ce1ca['displayTex'+'t']='Check\x20Limi'+'t';const _0x35e7a3={};_0x35e7a3[_0x1da9f3(0x4c3,0xa1a,-0x20,0x90d)]='limit',_0x35e7a3['buttonText']=_0x5ce1ca,_0x35e7a3[_0x1da9f3(0x847,0x6bd,0xab2,0x562)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x35e7a3]);const _0x1ada43={};_0x1ada43[_0x1da9f3(0xb79,0xf57,0xd8b,0xd52)+'t']=_0x1da9f3(0xc2c,0xffd,0xfeb,0x904)+_0x1da9f3(0x332,0x3a5,0x65e,-0x195);const _0x4513b0={};_0x4513b0[_0x1da9f3(0x4c3,0x179,0x80,0xae)]=_0x3f34aa(0x1f8,0x12d,0x3f4,0x2c3),_0x4513b0[_0x3f34aa(0x7d6,0xa2f,0x6ef,0x342)]=_0x1ada43,_0x4513b0[_0x3f34aa(0x647,0x913,0x693,0xaf0)]=0x1;const _0x4a9185={};_0x4a9185[_0x1da9f3(0x96d,0xa63,0xcfc,0xcdc)]=mek;if(!isPremium)return sendButMessage(from,_0x1da9f3(0x579,0x90b,0x9b5,0x272)+_0x3f34aa(0x351,0x27b,0x6da,0x449)+_0x3f34aa(0x2a3,-0x28,0x42d,0x805)+_0x1da9f3(0x630,0x1af,0x97a,0x342)+_0x3f34aa(0x5a4,0x4f5,0x2c4,0x143)+_0x1da9f3(0xb88,0x73d,0xac3,0x781)+_0x3f34aa(0x6d2,0x52c,0x37b,0x448)+'ekarang\x20de'+'ngan\x20cara\x20'+_0x1da9f3(0x2c0,0x671,0x7b0,0x4a4)+prefix+_0x1da9f3(0x5a8,0x4d,0x999,0xbc),_0x1da9f3(0x737,0xbf9,0xb0e,0x6c9)+_0x3f34aa(0xb93,0x4fd,0x680,0xb57),[_0x4513b0],_0x4a9185);if(args[_0x1da9f3(0xae2,0x720,0xe67,0x5df)]===0x949+0xd04+0x164d*-0x1)return reply(_0x3f34aa(0x654,0x98c,0x57d,0xac0)+_0x3f34aa(0x1d5,0x2dc,0x485,0xa0)+prefix+(_0x3f34aa(0xb3c,0xa43,0x7c8,0x695)+_0x3f34aa(0x333,0x115,0x87,0x544)+_0x3f34aa(0x9b5,0x4a3,0x66b,0x14c)+_0x3f34aa(0x719,0x37c,0x224,-0x1fe)));var srch=args[_0x1da9f3(0x4cc,-0x2c,0x618,0x73c)]('');aramas=await yts(srch),server=_0x1da9f3(0x510,0x3a1,0x921,0x660),aramat=aramas[_0x1da9f3(0x99f,0x918,0x4b6,0x56d)];var mulaikah=aramat[-0x1*0xc0f+0x4*-0x8ff+-0xfb*-0x31][_0x1da9f3(0x4ad,0x368,0x336,0x6)];try{ytv3(srch,server)[_0x3f34aa(0x3ad,-0x1d,0x2c,0x1b4)](_0x47b4f=>{const _0x502439={'nJncp':function(_0x14bdd4,_0x3a18af){return _0x14bdd4(_0x3a18af);},'XwrWK':_0x499bce(-0xd1,0x623,0x26b,0x670),'nILei':function(_0x16299d,_0x520754){return _0x16299d>=_0x520754;},'WbUmT':function(_0x485b3e,_0x2e3748,_0x4f0deb,_0x5b8bf2){return _0x485b3e(_0x2e3748,_0x4f0deb,_0x5b8bf2);},'BTsot':function(_0x3ed982,_0x52919c,_0x5173ac,_0x2c1224){return _0x3ed982(_0x52919c,_0x5173ac,_0x2c1224);}};function _0x499bce(_0x2fba8e,_0x4b7af3,_0x4e33dc,_0x39b3f2){return _0x1da9f3(_0x4e33dc- -0x19d,_0x39b3f2,_0x4e33dc-0x1db,_0x39b3f2-0xd0);}const {dl_link:_0x2b4ed0,thumb:_0x4a1282,title:_0x1609e8,filesizeF:_0x4b4798,filesize:_0x5aae8f}=_0x47b4f;function _0x1ff0d1(_0x413774,_0x2d2d81,_0x5bb072,_0x4c7bf1){return _0x3f34aa(_0x413774-0x86,_0x5bb072,_0x4c7bf1-0x462,_0x4c7bf1-0x138);}axios[_0x1ff0d1(0xbab,0x80d,0xbe6,0x77d)]('https://ti'+'nyurl.com/'+_0x499bce(0xc12,0xa38,0xa5f,0xcda)+'.php?url='+_0x2b4ed0)[_0x499bce(-0x90,0x12b,0x43,0x117)](async _0x169d5a=>{function _0x407d50(_0x52a162,_0x39fd5f,_0x3524fa,_0x2ee210){return _0x499bce(_0x52a162-0x197,_0x39fd5f-0x76,_0x52a162-0x344,_0x39fd5f);}function _0x318daf(_0x37bb19,_0x2f6f4f,_0x1988b9,_0x1de52d){return _0x499bce(_0x37bb19-0x86,_0x2f6f4f-0x17b,_0x2f6f4f-0x31f,_0x1988b9);}if(_0x502439[_0x318daf(0xa7f,0x6e0,0x24d,0xbe2)](_0x502439[_0x407d50(0xd68,0x9cb,0x1251,0xc50)](Number,_0x5aae8f),-0xb941+0x196ca+0xa917))return _0x502439[_0x318daf(0xba9,0x6b5,0x76f,0x79a)](sendMediaURL,from,_0x4a1282,_0x318daf(0xb6a,0xd24,0xe23,0xf37)+_0x318daf(0x897,0xccb,0xa88,0xce0)+'*'+enter+enter+(_0x407d50(0xb15,0xb18,0x64b,0x880)+':\x20')+_0x1609e8+enter+(_0x318daf(0x118,0x34d,-0x14b,0x463)+_0x318daf(0x2c2,0x657,0xa24,0x848)+'s')+enter+(_0x318daf(0x2bf,0x7a7,0xa58,0x66a)+_0x407d50(0x663,0x7b4,0x90e,0xa95))+enter+('🐭\x20*Filesiz'+_0x318daf(0x939,0xbd5,0x8e2,0xbe5))+_0x4b4798+enter+(_0x407d50(0x5fa,0x9dc,0x66d,0x234)+'\x20')+_0x169d5a[_0x318daf(0x74c,0x7ec,0x8dc,0xc1d)]+enter+enter+(_0x318daf(0xe22,0xbad,0xc32,0x8d6)+_0x407d50(0x7cf,0x75b,0xad5,0x2fe)+_0x407d50(0x6de,0x2d2,0x7d1,0x83c)+_0x407d50(0x47c,0x71a,0x22e,0x159)+'\x20dalam\x20ben'+_0x407d50(0x3b9,-0x119,0x361,0x856)));_0x502439[_0x318daf(0x712,0x45c,0x45a,-0x86)](sendMediaURL,from,_0x2b4ed0,'')[_0x318daf(0xe49,0xa15,0x98d,0xac5)](_0x340efe=>{function _0x4a8e56(_0x502636,_0x320085,_0x16615d,_0x39bc03){return _0x407d50(_0x16615d- -0x4d1,_0x502636,_0x16615d-0x1a,_0x39bc03-0x91);}function _0x5f1baa(_0x1f7515,_0x4fd67f,_0x4bb88d,_0x465389){return _0x407d50(_0x4fd67f- -0x37d,_0x465389,_0x4bb88d-0x88,_0x465389-0x1e8);}_0x502439[_0x5f1baa(0xad8,0x9eb,0xbca,0xa34)](reply,_0x502439[_0x4a8e56(0x3b7,-0x51,-0xe4,0x189)]);});});});}catch(_0x258862){reply(mess[_0x1da9f3(0x4e3,0x5b9,0x7e,0x5f5)][_0x1da9f3(0x67f,0x241,0x135,0x705)]);}}await limitAdd(sender,limit);break;case'ytmp4ajafh'+'d':{const _0x5b3709={};_0x5b3709['displayTex'+'t']='Check\x20Limi'+'t';const _0x2afcf5={};_0x2afcf5[_0x3f34aa(0x7b3,0x19b,0x30f,-0x107)]=_0x1da9f3(0x886,0x3c5,0xbec,0x439),_0x2afcf5[_0x1da9f3(0x8a3,0xd6c,0xc85,0x7e1)]=_0x5b3709,_0x2afcf5[_0x1da9f3(0x847,0x988,0xc9c,0x55a)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x2afcf5]);const _0x377c6c={};_0x377c6c[_0x1da9f3(0xb79,0x883,0xeba,0xb28)+'t']=_0x3f34aa(0xa41,0x582,0xa78,0xde7)+'emium';const _0x1db975={};_0x1db975['buttonId']=_0x3f34aa(-0xe1,0x311,0x3f4,-0x122),_0x1db975[_0x1da9f3(0x8a3,0x4f6,0x5a2,0x947)]=_0x377c6c,_0x1db975[_0x3f34aa(0x7bf,0x97f,0x693,0x2d0)]=0x1;const _0x376d48={};_0x376d48[_0x1da9f3(0x96d,0xd5c,0xe56,0x8b8)]=mek;if(!isPremium)return sendButMessage(from,_0x1da9f3(0x579,0x7e1,0x162,0x2c2)+'\x20fitur\x20ini'+_0x3f34aa(-0xfe,0x6bb,0x42d,0x760)+_0x3f34aa(0x827,0x214,0x47c,0x2a4)+_0x3f34aa(0x799,0x71e,0x2c4,0x500)+_0x3f34aa(0x483,0x4b9,0x9d4,0xb10)+_0x1da9f3(0x52f,0x62,0x86d,0x829)+_0x1da9f3(0x1c1,0x3b6,0x156,0x18a)+_0x1da9f3(0x388,0x61,-0x11a,-0xb9)+_0x1da9f3(0x2c0,0x64,0x804,0x36)+prefix+_0x1da9f3(0x5a8,0x30c,0x51c,0x665),_0x1da9f3(0x737,0x89c,0xb02,0x39c)+_0x3f34aa(0xa01,0x53f,0x680,0x3d0),[_0x1db975],_0x376d48);if(args[_0x1da9f3(0xae2,0xda6,0x103a,0xcd5)]===0x970+-0xc34+0x2c4)return reply('Kirim\x20peri'+'ntah\x20*'+prefix+(_0x3f34aa(0xab8,0x55f,0x7c8,0x5b9)+'dul\x20video\x20'+'yang\x20akan\x20'+_0x1da9f3(0x3d8,0xbb,0x130,0x16e)));var srch=args[_0x3f34aa(0x4ab,0x40a,0x318,-0x1eb)]('');aramas=await yts(srch),server=_0x1da9f3(0x510,0x4b9,0x22,0x78a),aramat=aramas[_0x3f34aa(0xbb9,0x2ff,0x7eb,0x3d7)];var mulaikah=aramat[-0x1*-0x1e7c+0xa*0x41+-0x2106*0x1][_0x3f34aa(0x3e8,0x75a,0x2f9,-0x16b)];try{ytv4(srch,server)[_0x3f34aa(0x4e8,-0x3f7,0x2c,-0x253)](_0x20ae92=>{function _0xf47705(_0x5764b1,_0x6865af,_0x249363,_0x443c5e){return _0x3f34aa(_0x5764b1-0x167,_0x5764b1,_0x6865af- -0x87,_0x443c5e-0x171);}const _0x1474ad={'EypaD':function(_0x18378f,_0x493897){return _0x18378f(_0x493897);},'wVMWL':_0xf47705(0x80c,0x351,0x7a2,0x13)+_0x2e7264(0x333,0x1b9,0x11a,0x354)+_0x2e7264(0xd27,0xb61,0x88f,0x106f)+_0xf47705(0x219,0x4fd,0x514,0x727)+'gi','IANMd':function(_0x22b8c8,_0x4f4e79){return _0x22b8c8(_0x4f4e79);},'yViOF':function(_0x25ff01,_0x38b14f){return _0x25ff01!==_0x38b14f;},'DXsOp':'oTIPB','eLYYe':function(_0x31b12a,_0x1fcad2){return _0x31b12a===_0x1fcad2;},'TDUlg':_0x2e7264(0x37c,0x229,0xc5,0x5d0),'RwhhF':function(_0x192385,_0x459925){return _0x192385>=_0x459925;},'GoKNK':function(_0x3e3dd9,_0x15b494){return _0x3e3dd9(_0x15b494);},'HkNnm':function(_0x4c080c,_0x50de29,_0x4d3cc1,_0x4d3034){return _0x4c080c(_0x50de29,_0x4d3cc1,_0x4d3034);},'BxGhO':function(_0xd64d95,_0x1af542,_0x1254e0,_0x35973c){return _0xd64d95(_0x1af542,_0x1254e0,_0x35973c);}};function _0x2e7264(_0x495b8e,_0x3e2db4,_0x33d0a6,_0x2f3432){return _0x3f34aa(_0x495b8e-0x65,_0x33d0a6,_0x495b8e-0x2e9,_0x2f3432-0xee);}const {dl_link:_0x1a7957,thumb:_0x5e136a,title:_0x3c6195,filesizeF:_0x325b36,filesize:_0x765ea6}=_0x20ae92;axios[_0xf47705(-0x2aa,0x294,-0x7,0x6ce)]('https://ti'+_0x2e7264(0x58d,0x11e,0x463,0x142)+'api-create'+_0xf47705(-0x13b,-0x73,0x5,-0x23)+_0x1a7957)[_0x2e7264(0x315,-0x78,0x52,0x565)](async _0x3189b4=>{const _0x1e2373={'IpDzN':function(_0x5a6cf5,_0x4c3feb){function _0x282e61(_0x4e34c2,_0x5bb288,_0x5156b5,_0x40a1a8){return _0xfa31(_0x5bb288-0x1ba,_0x40a1a8);}return _0x1474ad[_0x282e61(0x492,0x6a3,0x32e,0x4c9)](_0x5a6cf5,_0x4c3feb);},'ZHcEp':function(_0x143a00,_0x4b7274){function _0x775f3c(_0x5d636e,_0x5cdb3c,_0x2697be,_0x115fb0){return _0xfa31(_0x5cdb3c- -0x186,_0x115fb0);}return _0x1474ad[_0x775f3c(0x3ae,0x6dc,0xa9a,0xb93)](_0x143a00,_0x4b7274);},'DMTEw':_0x1474ad[_0x4b48a3(0xb7f,0xd4c,0x9f9,0xce9)],'WFUoj':_0x3a36f8(0x8c2,0x3ef,0x4dc,-0xb1)};function _0x4b48a3(_0x479244,_0x3eac5c,_0x70bb6d,_0x15d56d){return _0x2e7264(_0x70bb6d-0x216,_0x3eac5c-0x1df,_0x15d56d,_0x15d56d-0x68);}function _0x3a36f8(_0x155e73,_0x3c23a0,_0x50b8bc,_0x258fc5){return _0xf47705(_0x155e73,_0x3c23a0-0x222,_0x50b8bc-0xf0,_0x258fc5-0x10c);}if(_0x1474ad[_0x3a36f8(0x700,0x2f5,0x23a,-0xe6)](_0x1474ad[_0x4b48a3(0xac2,0x546,0x8bc,0x781)],_0x3a36f8(0x5de,0x22e,-0xd8,0x97))){if(_0x1474ad[_0x4b48a3(0xa9a,0xef7,0xba3,0x9c2)](_0x1474ad[_0x4b48a3(0xed6,0x9fc,0x9eb,0x7bc)](Number,_0x765ea6),-0x64b7*-0x3+0x12dde*0x2+-0x20341))return _0x1474ad[_0x3a36f8(0x5df,0x70e,0x9af,0x7e5)](sendMediaURL,from,_0x5e136a,_0x3a36f8(0xa05,0xb89,0x10d6,0x6d9)+_0x3a36f8(0xf76,0xb30,0xb79,0xcec)+enter+enter+(_0x4b48a3(0xbbb,0xab7,0xa32,0x65a)+'*\x20')+_0x3c6195+enter+(_0x3a36f8(0x161,0x1b2,-0x1a7,-0x378)+_0x4b48a3(0x13e,0x600,0x672,0x6d9)+'ps')+enter+('🦁\x20*Ext*\x20:\x20'+_0x3a36f8(0x9e6,0x4a3,0x861,0x408))+enter+(_0x4b48a3(0xda2,0x8ff,0x9c6,0xe49)+_0x3a36f8(0x5c4,0xa3a,0xddc,0xdae))+_0x325b36+enter+(_0x4b48a3(0xc7b,0xa7b,0x79e,0xcdb)+'\x20')+_0x3189b4[_0x4b48a3(0x72d,0xc64,0x9b5,0xb42)]+enter+enter+('_Untuk\x20dur'+_0x3a36f8(0x8f6,0x60f,0x4d9,0x84b)+_0x3a36f8(0x1e7,0x51e,0xa72,0x7bc)+_0x4b48a3(0x2cf,0x809,0x620,0x245)+_0x3a36f8(0x441,0x43c,0x68,0x7e6)+'tuk\x20link_'));_0x1474ad['BxGhO'](sendMediaURL,from,_0x1a7957,'')['catch'](_0x284c20=>{function _0x385b8b(_0x1f6bf8,_0x1cfb8b,_0x1c6bcb,_0x3e221f){return _0x4b48a3(_0x1f6bf8-0x2,_0x1cfb8b-0x155,_0x1cfb8b-0x5c,_0x1f6bf8);}const _0x140901={'phyiZ':function(_0x140d55,_0x16575f){function _0x2e9fc0(_0x2d52d8,_0x281f1d,_0x9b00e7,_0x262698){return _0xfa31(_0x2d52d8-0x101,_0x9b00e7);}return _0x1e2373[_0x2e9fc0(0x62f,0x465,0x3de,0x352)](_0x140d55,_0x16575f);}};function _0x2f6a87(_0x5532f1,_0x15c948,_0x231019,_0x44091c){return _0x3a36f8(_0x44091c,_0x231019- -0x21c,_0x231019-0x18f,_0x44091c-0xb1);}_0x1e2373[_0x2f6a87(0xb50,0x405,0x86f,0x963)](_0x1e2373[_0x2f6a87(0x8d2,0x4ca,0x8f5,0x4f7)],_0x2f6a87(0xa0a,0xc9b,0x904,0xe54))?_0x1e2373[_0x2f6a87(0x591,-0x46,0x311,0x6b6)](reply,_0x1e2373[_0x385b8b(0x989,0xc91,0xd7b,0x969)]):_0x140901[_0x385b8b(0x125b,0xd97,0xf8b,0xb07)](_0x1eb062,_0x447274['error'][_0x385b8b(0x4dc,0xa26,0xa3e,0xe2c)]);});}else _0x1474ad[_0x4b48a3(0x1104,0x117f,0xf14,0xf1d)](_0x408668,_0x1474ad['wVMWL']);});});}catch(_0x19d28e){reply(mess[_0x3f34aa(0x2d8,0x10a,0x32f,0x831)][_0x3f34aa(0x13c,0x5a1,0x4cb,0x55)]);}}break;case _0x3f34aa(0x434,0x41d,0x8d7,0xdeb):{const _0x3cbe1c={};_0x3cbe1c[_0x1da9f3(0xb79,0x976,0xb80,0xa75)+'t']='Check\x20Limi'+'t';const _0x3a2c58={};_0x3a2c58[_0x1da9f3(0x4c3,0x9c2,0x508,0x679)]=_0x3f34aa(0x5c4,0x415,0x6d2,0x49a),_0x3a2c58[_0x3f34aa(0x33f,0x464,0x6ef,0xc37)]=_0x3cbe1c,_0x3a2c58['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x3a2c58]);if(args[_0x1da9f3(0xae2,0x63c,0xf9c,0x9af)]===-0x1f9e+0x1218+0xd86)return reply(_0x1da9f3(0x731,0x31b,0x66e,0x475)+'ntah\x20*'+prefix+(_0x3f34aa(0x8e8,0x55b,0x7c8,0xb19)+'dul\x20video\x20'+'yang\x20akan\x20'+'dicari_'));var srch=args[_0x3f34aa(-0x185,0x3d3,0x318,0x454)]('');aramas=await yts(srch),server=_0x1da9f3(0x510,0x400,0x74e,0x2d0),aramat=aramas[_0x3f34aa(0x43c,0xc50,0x7eb,0x9ea)];var mulaikah=aramat[0x143+-0x25e0+0x7*0x53b][_0x1da9f3(0x4ad,0x57,0x9c6,0x56)];try{ytv2(srch,server)[_0x1da9f3(0x1e0,0x141,-0x28c,0x542)](_0x434c92=>{function _0x8af9f0(_0x5143a3,_0x1d29da,_0x13a9c,_0x498a28){return _0x3f34aa(_0x5143a3-0x2d,_0x1d29da,_0x5143a3-0xc6,_0x498a28-0x16);}function _0x5aff09(_0x12cbd0,_0x46ecb7,_0xac550,_0x1b026a){return _0x1da9f3(_0xac550-0x30,_0x1b026a,_0xac550-0x1d1,_0x1b026a-0xfb);}const _0x82587a={'AYexW':function(_0x118223,_0x637e1f){return _0x118223(_0x637e1f);},'uKOoY':'Emror','wcPQm':function(_0x53c327,_0x3f5438){return _0x53c327===_0x3f5438;},'sWItt':_0x8af9f0(0x4cf,0xf2,0x2b8,0x7a),'sinrd':function(_0x151fb6,_0x380247){return _0x151fb6>=_0x380247;},'nUUfb':function(_0x79161f,_0x5ac4b2,_0x2df900,_0x2f187a){return _0x79161f(_0x5ac4b2,_0x2df900,_0x2f187a);},'rjYJz':_0x5aff09(0xc21,0x2d2,0x784,0xc08)+_0x8af9f0(0x7d3,0x688,0x4d8,0x55a)+_0x8af9f0(0xacc,0xe6b,0x91b,0xccf),'YSVGq':_0x5aff09(0x9a0,0xb35,0x687,0xb53)},{dl_link:_0x2feaa3,thumb:_0x20b2d6,title:_0x29cd7a,filesizeF:_0x59d2bc,filesize:_0x5f574}=_0x434c92;axios[_0x8af9f0(0x3e1,-0x12b,0x5b3,0x59a)](_0x5aff09(0xaa7,0x6e1,0xb01,0x74e)+_0x8af9f0(0x36a,0x1f9,0x84f,0x48)+_0x5aff09(0xe8f,0xd38,0xc2c,0xe1f)+'.php?url='+_0x2feaa3)[_0x8af9f0(0xf2,0x5ef,0x2f3,0x630)](async _0x321a29=>{function _0x351ae7(_0x55ea20,_0x967b2f,_0x543ddf,_0x3f6848){return _0x8af9f0(_0x967b2f-0x4b,_0x55ea20,_0x543ddf-0x148,_0x3f6848-0xd);}function _0x439e8e(_0x21ff46,_0x56e649,_0x3c5f6f,_0x273520){return _0x8af9f0(_0x21ff46-0x3df,_0x56e649,_0x3c5f6f-0x144,_0x273520-0x163);}if(_0x82587a[_0x351ae7(0x309,0x85a,0x954,0x443)](_0x82587a[_0x351ae7(0x7e7,0x6c8,0x982,0x63a)],_0x82587a[_0x351ae7(0x7cb,0x6c8,0x468,0x74b)])){if(_0x82587a[_0x439e8e(0xbc2,0xccd,0xded,0x783)](Number(_0x5f574),-0x122c9*-0x2+0x1*0x40b7+-0xffa9))return _0x82587a[_0x351ae7(0x654,0xa41,0xb39,0x8cc)](sendMediaURL,from,_0x20b2d6,_0x351ae7(0x787,0xaff,0x96f,0x9a4)+_0x439e8e(0xe3a,0xb1f,0x975,0x9ad)+'*'+enter+enter+(_0x439e8e(0xc5f,0xf07,0x10b9,0x1193)+':\x20')+_0x29cd7a+enter+(_0x439e8e(0x916,0x99d,0x666,0xbc3)+'MP3')+enter+(_0x351ae7(0x45f,0x5d8,0x96d,0xa23)+_0x351ae7(0x56a,0x9b0,0x775,0xb15))+_0x59d2bc+enter+(_0x351ae7(0x4ca,0x3b0,0x3ca,0x901)+'\x20')+_0x321a29[_0x439e8e(0x95b,0xb90,0x654,0x847)]+enter+enter+(_0x439e8e(0xd1c,0xe75,0x8d5,0x87b)+'asi\x20lebih\x20'+_0x439e8e(0x828,0x7d5,0x717,0x69b)+'\x20disajikan'+'\x20dalam\x20ben'+'tuk\x20link_'));let _0x532de3=await getBuffer(_0x2feaa3);const _0x465a21={};_0x465a21[_0x351ae7(0xa48,0x56c,0x74d,0x56c)]=_0x82587a[_0x439e8e(0xe39,0x968,0xf71,0x9e7)],_0x465a21[_0x351ae7(0x8b5,0xac2,0x7cb,0x650)]=0x2,_0x465a21[_0x439e8e(0xaba,0xdc4,0x6db,0xcf1)]=_0x351ae7(0x855,0x4fa,0x46,-0x16)+'4',_0x465a21[_0x351ae7(0x4b7,0x908,0x908,0xcc0)]='',_0x465a21[_0x439e8e(0x7f0,0xae3,0xacf,0x8c8)+'rl']='',_0x465a21[_0x351ae7(0x9f1,0xadd,0xf24,0xdaf)]=pp_userz;const _0x32c62f={};_0x32c62f['forwarding'+'Score']=0x3b9aca00,_0x32c62f['isForwarde'+'d']=!![],_0x32c62f[_0x351ae7(0x853,0x9b8,0xe4d,0xe7e)+_0x351ae7(0xa9d,0xab7,0xca5,0xd3e)]=!![],_0x32c62f[_0x439e8e(0x834,0x89f,0x8b8,0x6f2)+_0x439e8e(0x7db,0x5bc,0x4f2,0xcc0)]=_0x465a21;const _0x2bf2ba={};_0x2bf2ba['contextInf'+'o']=_0x32c62f,_0x2bf2ba[_0x351ae7(0xa77,0xb47,0xfda,0xa58)]=_0x82587a[_0x439e8e(0x6a8,0x93e,0xaf5,0x985)],_0x2bf2ba[_0x439e8e(0x8e6,0xa32,0xbb1,0x9f5)]=_0x29cd7a+_0x351ae7(0xbde,0x77d,0x261,0xa99),_0x2bf2ba['quoted']=fgclink2,_0x2bf2ba[_0x351ae7(0x7d2,0xadd,0x9fe,0x7e3)]=pp_bot2,alpha[_0x439e8e(0x50e,0xa25,0x867,0x950)+'e'](from,_0x532de3,MessageType[_0x439e8e(0x76d,0x25c,0x798,0x70b)],_0x2bf2ba);}else _0x82587a['AYexW'](_0x29ffd0,_0x82587a[_0x439e8e(0x9a5,0xbef,0xbfd,0xcf1)]);});});}catch(_0xce96cf){reply(mess[_0x3f34aa(0x336,-0xc3,0x32f,-0x13a)][_0x3f34aa(0x954,0x3b0,0x4cb,0x980)]);}}await limitAdd(sender,limit);break;case _0x1da9f3(0x85d,0x442,0xabc,0xd91):const _0x8c31dc={};_0x8c31dc[_0x3f34aa(0x5ca,0xa67,0x9c5,0xd1b)+'t']='Check\x20Limi'+'t';const _0x1592fb={};_0x1592fb[_0x1da9f3(0x4c3,0x408,0x3ed,0x8d4)]=_0x3f34aa(0x782,0x98c,0x6d2,0x8bb),_0x1592fb['buttonText']=_0x8c31dc,_0x1592fb[_0x3f34aa(0x37b,0x3a2,0x693,0x5e7)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xaf1,0x742,0x69a)](prefix),'©\x20'+ownername,[_0x1592fb]);try{reply(lang[_0x3f34aa(0x65d,0x3b6,0x56f,0xa9e)]());let ksjz=q,yut=await yts(ksjz);yta(ksjz)[_0x1da9f3(0x1e0,0x93,-0x19c,0x17c)](_0x516d05=>{const _0x16f2af={'EvbWv':function(_0x4caecf,_0x1087b8){return _0x4caecf(_0x1087b8);},'vTSdn':_0x26d467(0x389,0x1c6,0x25f,0x2e0),'sGVaj':function(_0x3c1a06,_0x1f7e35){return _0x3c1a06*_0x1f7e35;},'PtVLM':_0x393539(0x3e2,0x7f9,0x590,0x913),'xciQF':_0x393539(0xce0,0x8dd,0x765,0x9d8),'fWveO':function(_0x345688,_0x5ea42c){return _0x345688(_0x5ea42c);},'qhyQV':_0x393539(0x549,0x3df,-0x10d,0x94)+'top4top.io'+_0x393539(0x5aa,0x4e5,0x29d,0x3bd)+'cv1.jpg','heLMd':_0x26d467(-0x23e,0x252,-0x6af,-0x726)+_0x26d467(0x457,0x2e8,0x4b2,-0x89)+_0x26d467(0x2d6,0x6e3,-0x210,-0x1bf)+_0x26d467(-0xcb,0x3fb,-0x17,-0xa4),'ChoXI':_0x26d467(0x416,0x390,-0xb7,0x537)+_0x26d467(0x457,0x2b9,0x915,0x81d)+_0x393539(-0x208,0x1bf,-0x3e,0x1dd)+_0x26d467(0x567,0x454,0x33f,0x848),'zUeht':_0x393539(0x298,0x4ad,0x6e3,0x741)+_0x393539(0xa72,0x777,0x600,0x28b)+'/p_20325yp'+'xk1.jpg','NyCcK':_0x393539(0x71e,0x348,-0x7f,-0x6)+_0x393539(0xba9,0x777,0x460,0xca6)+_0x26d467(0x3f1,0xb9,0x362,-0x127)+_0x393539(0x4c8,0x39b,0x1a4,0x43d),'xLrxj':function(_0x4be35a,_0x11c196){return _0x4be35a===_0x11c196;},'lCxHV':_0x26d467(0x0,-0x145,-0x3a5,-0x13),'OBWjH':function(_0x497b3e,_0x147e8d){return _0x497b3e>=_0x147e8d;},'oYssw':function(_0x1f9001,_0x40af64,_0x1b4616,_0x13ecee){return _0x1f9001(_0x40af64,_0x1b4616,_0x13ecee);}};function _0x393539(_0x4a5cd0,_0x1e87eb,_0x4e58de,_0x3e0afe){return _0x3f34aa(_0x4a5cd0-0x1c0,_0x4e58de,_0x1e87eb-0xdd,_0x3e0afe-0x1ce);}const {dl_link:_0x2c31a0,thumb:_0x28dde8,title:_0x25c52b,filesizeF:_0xe27d17,filesize:_0xf0ffe3}=_0x516d05;function _0x26d467(_0x4539ff,_0x3a455d,_0x186fbf,_0x319349){return _0x3f34aa(_0x4539ff-0x1a7,_0x319349,_0x4539ff- -0x243,_0x319349-0x152);}axios[_0x393539(0xf1,0x3f8,-0x47,-0xb2)](_0x393539(0xa72,0x9fa,0x74e,0x4f2)+'nyurl.com/'+_0x393539(0xb65,0xb25,0xa4c,0xcdd)+_0x393539(-0xef,0xf1,-0x161,-0x12)+_0x2c31a0)['then'](async _0x16e78e=>{function _0xe61f1d(_0x20eb9d,_0x573b7c,_0x452fbc,_0x30a9d0){return _0x26d467(_0x452fbc-0x2ad,_0x573b7c-0x1d7,_0x452fbc-0xf4,_0x30a9d0);}function _0x48196b(_0x3077a7,_0x2ed687,_0x1fad4d,_0x32b332){return _0x393539(_0x3077a7-0x3c,_0x3077a7-0x217,_0x2ed687,_0x32b332-0x97);}const _0x184dce={'makYn':_0x16f2af['vTSdn'],'OcYZQ':function(_0x209bed,_0x20ddad){return _0x16f2af['sGVaj'](_0x209bed,_0x20ddad);},'wgQKW':_0x16f2af['PtVLM'],'sbvGq':_0x16f2af[_0x48196b(0x448,0x500,0x20c,0x24e)],'MPKpw':function(_0x80caa5,_0x1548bd,_0x584cf7){return _0x80caa5(_0x1548bd,_0x584cf7);},'ynzdo':_0x48196b(0x546,0x7c8,0x550,0x64c),'iGWaj':function(_0x34a6f0,_0x38303b){function _0x4b7383(_0x11ede2,_0x4e13c4,_0x561c53,_0x31655b){return _0xe61f1d(_0x11ede2-0x4f,_0x4e13c4-0x126,_0x31655b- -0x198,_0x11ede2);}return _0x16f2af[_0x4b7383(0x63e,0x477,0x5ac,0x33f)](_0x34a6f0,_0x38303b);},'CtclW':_0x16f2af[_0xe61f1d(0x243,0x6e3,0x645,0x5c6)],'kHXdw':_0x16f2af[_0x48196b(0x88e,0xb4f,0x648,0xdd8)],'rzvfT':_0x16f2af[_0xe61f1d(0x520,0x3e8,0x1cf,0x5f5)],'NCimy':_0x16f2af[_0x48196b(0xb9e,0x725,0xebc,0xdf9)],'DGWCH':_0x16f2af[_0x48196b(0x341,0x67d,0x139,0x1c9)],'QPVUh':_0xe61f1d(0x585,0x52b,0x71c,0x485)+_0x48196b(0x98e,0x73b,0x7f5,0xd88)+_0xe61f1d(0xb16,0xd56,0xabf,0x8a4)+_0x48196b(0xb74,0x1072,0x9fb,0x936)};if(_0x16f2af[_0xe61f1d(0x683,0x106,0x28c,0x4e9)](_0x16f2af['lCxHV'],_0xe61f1d(0x211,0x980,0x66b,0x865))){const _0x493024=_0x184dce[_0xe61f1d(0x699,0xd69,0x844,0x8c8)][_0xe61f1d(0x9d3,0xb35,0x831,0xc54)]('|');let _0x38c6ae=-0x6d*-0x52+0x67c*0x1+-0x7*0x5ea;while(!![]){switch(_0x493024[_0x38c6ae++]){case'0':var _0x5efa7a=_0xea7056[_0x278809[_0x48196b(0x6c0,0x9ba,0xbd3,0x2e1)](_0x184dce['OcYZQ'](_0x214b6f[_0x48196b(0xa97,0xac2,0x957,0x90f)](),_0xea7056[_0xe61f1d(0xbf3,0xea6,0x998,0x850)]))];continue;case'1':const _0x6e87d3={};_0x6e87d3['mentionedJ'+'id']=[_0x488aaa],_0x1e7c82['sendMessag'+'e'](_0x33cd3b,_0x1b4845,_0x5d55f3,{'thumbnail':_0xe0d44c[_0x48196b(0x405,0x874,0x8a2,0x84a)+'nc']('image/'+_0x5b84c8),'caption':_0x184dce[_0xe61f1d(-0x1c1,0x15b,0xf3,-0x2)],'contextInfo':{'text':_0x184dce[_0xe61f1d(-0xf4,-0x3a8,0xf3,0xb1)],'forwardingScore':0x3b9aca00,'isForwarded':!![],'sendEphemeral':!![],'externalAdReply':{'title':''+_0xe8864b,'body':_0x48196b(0x577,0x8e2,0x3a2,0x2f)+_0xe61f1d(0x25e,0x3d3,0x647,0x2c5),'previewType':_0x184dce[_0xe61f1d(0xa2,0x1d0,0x78,0x1e3)],'thumbnailUrl':''+_0x5efa7a,'thumbnail':'','sourceUrl':_0xe61f1d(0x7be,0x6a9,0x60a,0x6cb)+'utube.com/'+_0xe61f1d(0x18f,0x4d0,0x150,-0x23e)+_0xe61f1d(0x39c,0xa15,0x576,0x336)+'ODby7_MoRY'+'lA'}},'quoted':_0x10ce94,'contextInfo':_0x6e87d3});continue;case'2':_0x119aa4[_0xe61f1d(0x516,0x6d5,0x588,0x513)](_0x184dce[_0xe61f1d(0xce0,0x968,0xa9e,0x9a9)](_0x86b491,_0x5def75,_0x184dce[_0xe61f1d(0x490,0x99d,0x8aa,0xbaa)]),_0x184dce[_0xe61f1d(0xa16,0x57b,0x7c3,0x43b)](_0x399190,_0x48196b(0x47d,0x721,0x807,0x94c)+_0xe61f1d(0x5c7,0x341,0x76f,0x79b)+_0xe61f1d(-0xc2,0x911,0x407,0x810)));continue;case'3':_0xc9deb0=_0x48196b(0x742,0x75f,0x619,0x432)+_0x48196b(0x786,0x653,0x744,0x441)+_0x48196b(0x813,0x42a,0x589,0xa10)+_0x48196b(0x796,0xbe4,0x5df,0x317)+'O\x20\x0a•\x20FATIH'+'\x20\x0a•\x20FERDYZ'+_0xe61f1d(0xd76,0x91e,0xaba,0xea0)+_0xe61f1d(0x33d,0x359,0x628,0x14e)+_0x38604a[_0xe61f1d(0x9e2,0xc94,0x831,0x68b)]('@')[0xd18+0x1885+-0x259d];continue;case'4':var _0xea7056=[_0x184dce[_0xe61f1d(0xa5a,0xd05,0x862,0x72a)],_0x184dce[_0xe61f1d(0x4db,0x76c,0x242,-0x2a8)],_0x184dce[_0x48196b(0x78f,0x239,0xc71,0x514)],_0x48196b(0x5f6,0x527,0x5bc,0x944)+_0xe61f1d(0x4eb,0x4d5,0x704,0x4e4)+_0xe61f1d(0xbae,0x64b,0x839,0x6cd)+_0x48196b(0x3a9,0x2a,0x62e,0x587),_0x184dce[_0xe61f1d(0x672,0x2dc,0x5cf,0x701)],_0x184dce[_0xe61f1d(0x98f,0x6d3,0x447,0x7d)],_0x184dce[_0xe61f1d(-0xe9,-0xe2,0x1bb,0x37e)]];continue;}break;}}else{if(_0x16f2af[_0x48196b(0x9b9,0x854,0x987,0x874)](Number(_0xf0ffe3),-0x78c2+0x5*-0x2bd5+-0xe5*-0x1ff))return sendMediaURL(from,_0x28dde8,'*🎧\x20P\x20L\x20A\x20Y'+_0xe61f1d(0x4b3,0xa14,0x55d,0x584)+'💦\x20Title\x20:\x20'+_0x25c52b+(_0xe61f1d(-0x2c7,-0x31,0x1e3,0x42f)+_0x48196b(0x6c3,0x608,0x682,0x713))+yut[_0xe61f1d(0x768,0xa82,0xa67,0x92b)][-0x135d*0x1+-0x29e+0x15fb]['videoId']+('\x0a🚀\x20Upload\x20'+':\x20')+yut[_0x48196b(0xcf1,0xe3c,0xcd7,0xa85)][-0x1*-0x1a93+0x201c+-0x3aaf]['ago']+(_0x48196b(0x3a1,0x27d,0x757,0x49c)+'\x20')+_0xe27d17+_0xe61f1d(0xbd1,0xb6b,0x791,0xc93)+yut['videos'][-0xc5*-0x1f+0x1cb0+-0x348b][_0x48196b(0xa5b,0x919,0xdd4,0xaeb)]+('\x20\x0a🐣\x20Durati'+_0xe61f1d(0x34b,0x71,0x198,-0xdb))+yut[_0x48196b(0xcf1,0x1175,0xb8f,0x118f)][0x3ba+0x4*0x184+-0x9ca][_0xe61f1d(-0x237,0x243,0x1e4,-0x242)]+_0x48196b(0xb14,0xd3b,0x70e,0x6df)+yut['videos'][0x11eb+-0x551*-0x5+-0x2c80][_0xe61f1d(0x316,0x867,0x363,0x792)]+(_0xe61f1d(0xe11,0xdbd,0x966,0x850)+_0x48196b(0xc91,0xf5d,0xcff,0x85a)+_0x48196b(0x355,0x15a,0x703,0x808)+_0x48196b(0x5eb,0x3a3,0x8c0,0xb3d)+'it\x20is\x20pres'+_0xe61f1d(0x54f,-0x153,0x136,0x4b2)+_0x48196b(0x90e,0x5f4,0x625,0x87d)+_0x48196b(0x91a,0xa56,0xaaa,0xc55)));_0x16f2af['oYssw'](sendMediaURL,from,_0x2c31a0,'')['catch'](_0x1ad52b=>{function _0x4763ab(_0x5a4019,_0x3cdf0b,_0x2ef432,_0x28f424){return _0xe61f1d(_0x5a4019-0xb4,_0x3cdf0b-0xf2,_0x3cdf0b-0x29f,_0x28f424);}_0x16f2af[_0x4763ab(0x860,0xab4,0xf60,0xfe5)](reply,'Emror');});}});})[_0x1da9f3(0x893,0x8a4,0x4e7,0xbd8)](_0x5dd4b2=>reply(''+_0x5dd4b2));}catch(_0x38f1a8){console['log'](color('[PlayMp3]',_0x1da9f3(0x316,-0x14f,0x42f,0x68d)),_0x38f1a8);}await limitAdd(sender,limit);break;case _0x1da9f3(0x6e1,0x5d5,0x3fd,0x471):const _0x4198ee={};_0x4198ee[_0x1da9f3(0xb79,0xc23,0xd43,0xeb2)+'t']=_0x3f34aa(0xde,0x10d,0x4e1,0x716)+'t';const _0x40706b={};_0x40706b[_0x1da9f3(0x4c3,0x85d,0x15f,0x88f)]=_0x3f34aa(0x479,0x345,0x6d2,0x80b),_0x40706b[_0x3f34aa(0x607,0x1d5,0x6ef,0x4bc)]=_0x4198ee,_0x40706b['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x40706b]);try{reply(lang['wait']());let ksjjz=q,yut=await yts(ksjjz);yta(ksjjz)[_0x1da9f3(0x1e0,0x318,0x218,0x70)](_0x1015ff=>{function _0x368376(_0x35a1de,_0x4cc983,_0x3725c5,_0x3ce0ea){return _0x3f34aa(_0x35a1de-0x1f1,_0x4cc983,_0x35a1de- -0x120,_0x3ce0ea-0xfd);}const _0x59511e={'iqqZW':function(_0x13a9da,_0x3f0bd9){return _0x13a9da>=_0x3f0bd9;},'mwEVS':function(_0xec8ca8,_0x20b776){return _0xec8ca8(_0x20b776);},'sIQFq':function(_0x54d52f,_0x2d0b39){return _0x54d52f(_0x2d0b39);},'pdfws':'https://yo'+_0x368376(0x5ed,0x892,0x73a,0x375)+_0x368376(0x8e6,0x5f4,0xb00,0x4d3),'hVQrB':_0x3c416d(0x4d6,0x53b,0x219,0x2cc)};function _0x3c416d(_0x42dd16,_0x48fb89,_0x1319c7,_0x23e3d5){return _0x1da9f3(_0x42dd16- -0x215,_0x1319c7,_0x1319c7-0x10c,_0x23e3d5-0x104);}const {dl_link:_0x75f8e5,thumb:_0x2a5de8,title:_0x1062f1,filesizeF:_0x4d2d40,filesize:_0x5617c7}=_0x1015ff;axios[_0x368376(0x1fb,-0x5,0x440,0x15d)](_0x3c416d(0x8bc,0x890,0xdff,0x703)+_0x368376(0x184,-0x3c8,-0x312,0x246)+_0x3c416d(0x9e7,0x625,0x826,0xebc)+'.php?url='+_0x75f8e5)[_0x3c416d(-0x35,0x46e,-0x85,-0x12c)](async _0x507767=>{function _0x3bf9d5(_0x1a396b,_0x25bdf5,_0x12a4e7,_0x5bf628){return _0x368376(_0x25bdf5-0x5b8,_0x12a4e7,_0x12a4e7-0x144,_0x5bf628-0xa0);}if(_0x59511e[_0x1eea10(0x78f,0x99a,0x43a,0x52c)](_0x59511e['mwEVS'](Number,_0x5617c7),-0x53dd+0x3442+0x94cb))return sendMediaURL(from,_0x2a5de8,_0x3bf9d5(0xa26,0x98e,0x7f6,0x4f6)+_0x3bf9d5(0x4dc,0x98b,0x78b,0xc23)+_0x3bf9d5(0xa06,0x75d,0x294,0x2f6)+_0x1062f1+(_0x3bf9d5(0x4c8,0x725,0x823,0x30e)+_0x3bf9d5(0x7c8,0x867,0x56a,0x6de))+yut['videos'][0xe2*0x4+-0x87f+0x1f*0x29][_0x1eea10(0xd8,0xe8,0x2e2,0x4ae)]+(_0x1eea10(0x784,0x76b,0x1ef,0x52a)+':\x20')+yut[_0x1eea10(0x3a5,0xb74,0x75e,0x876)][-0x409+0x6df+-0x2d6][_0x1eea10(-0x43,0x498,0x766,0x234)]+(_0x1eea10(-0x1e5,0x1d1,0x2d8,-0xda)+'\x20')+_0x4d2d40+_0x1eea10(0x68e,0x1a6,0x874,0x5a0)+yut[_0x3bf9d5(0xe76,0xe95,0xb77,0x13bf)][0x2201+0x1*-0x5d3+0x1c2e*-0x1]['views']+(_0x1eea10(-0x41b,-0x207,0x279,0x6e)+'on\x20:\x20')+yut['videos'][0x1275+-0x1*0x1819+0x2*0x2d2]['timestamp']+'\x0a🌀\x20Url\x20:\x20'+yut['videos'][-0x15ef*-0x1+-0x33e+-0x12b1]['url']+('\x0a\x0a_For\x20the'+_0x3bf9d5(0xcdd,0xe35,0xc72,0x9b5)+_0x3bf9d5(0x4aa,0x4f9,0x31f,0x828)+_0x3bf9d5(0xbf5,0x78f,0x3ae,0x94d)+_0x1eea10(0x223,0x11a,0x271,-0x81)+_0x1eea10(0x1dd,-0x2bf,0x37e,-0xbb)+_0x3bf9d5(0x6bf,0xab2,0x946,0x8c8)+_0x3bf9d5(0x939,0xabe,0xaa1,0x650)));let _0x5dc5ef=await _0x59511e[_0x3bf9d5(0x11a0,0xea1,0x13bc,0xb12)](getBuffer,_0x75f8e5);const _0x157447={};_0x157447[_0x1eea10(0x794,-0x258,0x5d2,0x2d4)]=_0x59511e['pdfws'],_0x157447['mediaType']=0x2,_0x157447['title']='Youtube\x20Mp'+'3',_0x157447[_0x3bf9d5(0xcef,0xc8f,0xdf2,0x809)]='',_0x157447[_0x3bf9d5(0xc3e,0x7e3,0xbd0,0x5a1)+'rl']='',_0x157447['thumbnail']=pp_userz;const _0x6e95d={};_0x6e95d[_0x1eea10(-0x29c,0x1a5,-0x11b,0xb0)+_0x1eea10(0x30e,0x153,0x80f,0x42c)]=0x3b9aca00,_0x6e95d[_0x3bf9d5(0x8d5,0x9e7,0xb07,0xe7e)+'d']=!![],_0x6e95d['sendEpheme'+_0x3bf9d5(0xd44,0xe3e,0x133e,0xbba)]=!![],_0x6e95d[_0x3bf9d5(0xa6a,0x827,0x77c,0x845)+_0x1eea10(0x319,0x1a3,0x47f,0x1af)]=_0x157447;function _0x1eea10(_0x21b514,_0x40001c,_0x24a38c,_0x457060){return _0x368376(_0x457060- -0x67,_0x21b514,_0x24a38c-0x83,_0x457060-0x181);}const _0x464f78={};_0x464f78[_0x3bf9d5(0x615,0x683,0x7c8,0x53c)+'o']=_0x6e95d,_0x464f78['mimetype']=_0x59511e[_0x3bf9d5(-0xa1,0x495,0x6a,-0x8b)],_0x464f78[_0x1eea10(0x10d,0x730,0x42b,0x2ba)]=_0x1062f1+_0x3bf9d5(0x93d,0x722,0x3d7,0x9ef),_0x464f78[_0x1eea10(0x950,0x4ef,0xb41,0x632)]=fgclink2,_0x464f78[_0x1eea10(0xb0c,0xc79,0x9de,0x845)]=pp_bot2,alpha[_0x1eea10(0x245,-0x5d3,0x129,-0x11e)+'e'](from,_0x5dc5ef,MessageType['document'],_0x464f78);});})[_0x1da9f3(0x893,0xa18,0x47f,0x781)](_0x2f0cd5=>reply(''+_0x2f0cd5));}catch(_0x2cec4f){console[_0x3f34aa(0x49e,0x37f,0x51e,0x5fb)](color(_0x1da9f3(0x5c9,0x201,0x484,0x554),_0x1da9f3(0x316,0x28a,-0xb2,0x528)),_0x2cec4f);}await limitAdd(sender,limit);break;case'igstory':case _0x3f34aa(0x584,0xaa,0x26e,0x52a):case _0x3f34aa(0xcb3,0x85d,0x7d3,0x30c)+'tory':const _0x8cbdd5={};_0x8cbdd5[_0x3f34aa(0x9e6,0xcec,0x9c5,0x529)+'t']=_0x1da9f3(0x695,0x713,0xbd8,0xb54)+'t';const _0x256f26={};_0x256f26[_0x3f34aa(0x36f,0x404,0x30f,0x474)]=_0x1da9f3(0x886,0x45e,0xc32,0x74b),_0x256f26['buttonText']=_0x8cbdd5,_0x256f26[_0x1da9f3(0x847,0x7d7,0x355,0x36e)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x47d,0xd6a,0xa39)](prefix),'©\x20'+ownername,[_0x256f26]);if(!q)return reply(_0x3f34aa(0x724,0x1b6,0x247,0x6cc)+'a?');pape=q,hx[_0x3f34aa(0xe17,0x9b7,0xa10,0xe3a)](pape)[_0x1da9f3(0x1e0,0xc7,-0x191,0x4f8)](async _0x571d4a=>{function _0x338cbc(_0x53a09c,_0x6fd882,_0x5e31d7,_0x165ed2){return _0x1da9f3(_0x5e31d7-0x38c,_0x6fd882,_0x5e31d7-0x1ec,_0x165ed2-0x4f);}const _0x1fb676={'pnCtm':function(_0x4ab353,_0x361a89){return _0x4ab353(_0x361a89);},'amsal':_0x4ddbd3(0x4c5,0xc27,0x554,0x899)+_0x4ddbd3(0x7c3,0x400,0xe9,0x50b)+_0x338cbc(0xc45,0xea4,0xf7e,0x1088)+_0x338cbc(0xcbc,0xb14,0xac4,0x9b0)+'gi','CdDPU':function(_0x51a76b,_0x1a6f41){return _0x51a76b(_0x1a6f41);},'bmjKb':_0x338cbc(0x90c,0x919,0xcb6,0xad0),'UhrjP':function(_0x1d6c38,_0x1b5b58){return _0x1d6c38(_0x1b5b58);},'pBnas':_0x338cbc(0x3d6,0x790,0x727,0x26a)};let _0x4c92c7=await _0x1fb676['CdDPU'](getBuffer,_0x571d4a['user'][_0x4ddbd3(0x6d4,0x9e0,0xe54,0x923)+'Url']);function _0x4ddbd3(_0x4c6d49,_0x229361,_0x188aca,_0x3d99e9){return _0x1da9f3(_0x3d99e9-0x30d,_0x188aca,_0x188aca-0x179,_0x3d99e9-0xda);}await alpha[_0x4ddbd3(0x7f0,0x690,0x1b9,0x52a)+'e'](from,_0x4c92c7,image,{'thumbnail':_0x4c92c7,'quoted':mek,'caption':_0x4ddbd3(0x6fb,0x89a,0xa0f,0x7e0)+'AM\x20STORY*\x20'+'」'+enter+enter+'🆔\x20*ID\x20:*\x20'+_0x571d4a[_0x338cbc(0x9a7,0xd8b,0xc61,0xd51)]['id']+enter+(_0x4ddbd3(0xed9,0x749,0xeac,0xbcb)+_0x4ddbd3(0xcd4,0x879,0xbdd,0x925))+_0x571d4a[_0x338cbc(0xb4e,0x8bb,0xc61,0x1114)][_0x338cbc(0xaef,0x957,0xc0f,0x6bf)]+enter+('💋\x20*Fullnam'+_0x4ddbd3(0xb39,0xc62,0x661,0x925))+_0x571d4a[_0x4ddbd3(0x104f,0xc6c,0xbfd,0xbe2)][_0x338cbc(0xc4c,0xe33,0xfdf,0x1087)]+enter+('💌\x20*Followe'+_0x338cbc(0x7e0,0xdc6,0x8ed,0xdb1))+_0x571d4a[_0x338cbc(0x7bc,0x70b,0xc61,0xb6d)][_0x338cbc(0xffd,0x8b7,0xb10,0x9b5)]+enter+('✨\x20*Followi'+_0x4ddbd3(0x412,0xafb,0x574,0x820))+_0x571d4a[_0x4ddbd3(0x828,0xd80,0x861,0xbe2)][_0x4ddbd3(0xe37,0x91b,0xcd1,0xdaa)]+enter+(_0x338cbc(0xa54,0xaf5,0x6ef,0x248)+_0x338cbc(0xfde,0x1041,0xb46,0x7ab)+_0x338cbc(0xbe7,0x65b,0x88a,0xd2e)+_0x338cbc(0x863,0x71b,0x96a,0xe2c))+_0x571d4a[_0x338cbc(0xb21,0x762,0xc61,0x937)]['username']+enter+('*💢\x20Total\x20:'+'*\x20')+_0x571d4a[_0x338cbc(0x271,0x9e2,0x73a,0xb79)]['length']+_0x338cbc(0x771,0xb15,0xbb9,0x8e6)+enter+enter+(_0x4ddbd3(0xaf6,0x122e,0xcd3,0xe7c)+_0x338cbc(0xbc0,0x4be,0x8c8,0x97e)+'t\x20media\x20ak'+_0x338cbc(0x137e,0x14b2,0xfc2,0x1283)+'_')});for(let _0x6a78ee of _0x571d4a[_0x4ddbd3(0x1da,0xb30,0x653,0x6bb)]){if(_0x6a78ee[_0x338cbc(0x9f1,0xb1a,0x839,0x3a3)][_0x4ddbd3(0x12b3,0x9a5,0x12ab,0xdda)](_0x1fb676[_0x4ddbd3(0xf89,0x8a1,0xeb1,0xd1b)])){let _0x5bb337=await _0x1fb676[_0x338cbc(0x13b0,0x10cb,0xe85,0xc02)](getBuffer,_0x6a78ee[_0x4ddbd3(0xb69,0xa07,0x33a,0x7ba)]);alpha[_0x338cbc(0xc5,0x105,0x5a9,0x494)+'e'](from,_0x5bb337,video,{'thumbnail':Buffer[_0x338cbc(0xdb2,0x1073,0xc4e,0xb3a)](-0xdd+0x1db5+-0x1cd8),'quoted':mek,'caption':_0x4ddbd3(0xd4d,0xeb3,0x12b6,0xd84)+'•\x20'+_0x6a78ee[_0x338cbc(0x10e9,0xe8c,0xbd3,0x8d4)]});}else{if(_0x1fb676[_0x338cbc(0xba6,0xa8d,0x74b,0x838)]===_0x4ddbd3(0x8ff,0x662,0xa9e,0x6a8)){let _0x384859=await _0x1fb676['UhrjP'](getBuffer,_0x6a78ee['url']);alpha[_0x338cbc(0x62,0x5aa,0x5a9,0x6dc)+'e'](from,_0x384859,image,{'thumbnail':Buffer['alloc'](-0x135b+-0x10bb+-0x2*-0x120b),'quoted':mek,'caption':'Instagram\x20'+'•\x20'+_0x6a78ee['type']});}else _0x1fb676[_0x338cbc(0x33a,0x284,0x6e7,0xbe9)](_0xca5734,_0x1fb676[_0x4ddbd3(0xff4,0x9f4,0x85b,0xd1a)]);}}})['catch'](_0x5f1492=>reply(_0x3f34aa(-0xfc,0x12f,0x3d8,0x5b4)+_0x1da9f3(0x661,0x3b7,0x331,0x125)+_0x3f34aa(0x3a8,0x50d,0x400,0x631)+_0x1da9f3(0x912,0x3c7,0x8af,0x8a3)+'mand\x20ya\x20sa'+'ma'));break;case'ig':case _0x1da9f3(0x450,0x692,0x355,0x3fc):case _0x3f34aa(0x78d,-0x137,0x2cb,0x320):case _0x3f34aa(0x502,0xa45,0x4fd,0x1ec):case _0x1da9f3(0x399,-0x137,0x3bb,0x58e):case'igfoto':case'igvideo':case _0x3f34aa(0x2ca,0x101,0x1d3,0x19a):case _0x3f34aa(0x4c8,0x4d3,0x16,-0x22e):case'igreels':case'instareels':case _0x3f34aa(0x514,0x4a0,0x45,-0x1df):case _0x3f34aa(0x567,0x700,0x2fa,0x17c):case'instatv':const _0x15c64c={};_0x15c64c['displayTex'+'t']=_0x3f34aa(0x654,0x99c,0x4e1,0x704)+'t';const _0x2047fb={};_0x2047fb[_0x1da9f3(0x4c3,-0x53,0x4a2,0x89e)]=_0x1da9f3(0x886,0x69d,0x819,0xab7),_0x2047fb[_0x1da9f3(0x8a3,0xda1,0x3ee,0x5bf)]=_0x15c64c,_0x2047fb[_0x1da9f3(0x847,0x96c,0x564,0xc5b)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x69b,0x775,0x914)](prefix),'©\x20'+ownername,[_0x2047fb]);if(!isUrl(args[0x11b0*-0x2+0x1f42+0x3e*0x11])&&!args[0x210+0x25dc*0x1+-0x27ec][_0x3f34aa(0xad0,0x744,0x919,0x9c7)](_0x3f34aa(0x6c0,0x456,0x751,0x558)+_0x1da9f3(0x91e,0x410,0xa70,0xb81)))return reply(lang[_0x3f34aa(0x6e3,0x81d,0x5bc,0x8c3)]());let urlnya=q;hx[_0x1da9f3(0x450,0x7c7,-0x6b,0x923)](urlnya)['then'](async _0x515ef0=>{const _0x41f410={'FefsT':function(_0x2e0d1a,_0x55c7d2){return _0x2e0d1a(_0x55c7d2);},'dtCgc':function(_0x5c7299,_0x1c6e3a){return _0x5c7299===_0x1c6e3a;},'rTZPZ':_0x15f88e(0x991,0xeeb,0xe1c,0xd13),'PxZyO':function(_0x385ef5,_0x43a9da){return _0x385ef5===_0x43a9da;},'PvPdt':'JsKca'};let _0x1300e8=await getBuffer(_0x515ef0[_0x15f88e(0xc16,0xe1c,0xc10,0xa48)]['profilePic'+_0x15f88e(0xf40,0xb6d,0xd43,0xb61)]);await alpha['sendMessag'+'e'](from,_0x1300e8,image,{'thumbnail':_0x1300e8,'quoted':mek,'caption':_0x5be99e(-0x14d,0x1f3,0x530,-0xb0)+'AM\x20DOWNLOA'+_0x5be99e(0x9ec,0x63f,0x2f7,0x6f3)+enter+enter+'🆔\x20*ID\x20:*\x20'+_0x515ef0[_0x15f88e(0xba2,0x8f6,0x85d,0xa48)]['id']+enter+(_0x15f88e(0xbf2,0xf21,0xab5,0xa31)+_0x15f88e(0x6a1,0x32f,0x75f,0x78b))+_0x515ef0[_0x5be99e(0x8e9,0x5f5,0x2ce,0x7ec)][_0x15f88e(0xb61,0xbb3,0xbfd,0x9f6)]+enter+(_0x5be99e(0xdb1,0x861,0x45a,0x8c6)+_0x5be99e(0x44b,0x338,0x469,-0x196))+_0x515ef0[_0x15f88e(0x794,0x762,0xa9f,0xa48)][_0x15f88e(0x1268,0xc16,0x9e2,0xdc6)]+enter+(_0x15f88e(0xc03,0xf6c,0x8d0,0xa58)+_0x15f88e(0x9c3,0x716,0x290,0x6d4))+_0x515ef0[_0x15f88e(0xcb9,0xf63,0x88a,0xa48)]['followers']+enter+(_0x5be99e(0x622,0x24b,0x100,0x6f0)+_0x15f88e(0x8a6,0x790,0x496,0x686))+_0x515ef0[_0x15f88e(0xef3,0x98c,0x712,0xa48)]['following']+enter+('*🔗\x20Link\x20:*'+_0x15f88e(0xc96,0xe51,0xe62,0x92d)+_0x5be99e(0x139,0x21e,0x3cf,0x5b)+'om/')+_0x515ef0[_0x15f88e(0xd74,0xe78,0x6e6,0xa48)][_0x15f88e(0x670,0xc59,0x972,0x9f6)]+enter+('*💢\x20Total\x20:'+'*\x20')+_0x515ef0[_0x5be99e(0x29b,0xce,0x2b7,0x593)][_0x15f88e(0x9c4,0x1148,0xea7,0xc55)]+_0x5be99e(0x199,0x54d,0x409,0x9d6)+enter+enter+(_0x5be99e(0x7b1,0x88f,0xdc9,0x7ab)+_0x15f88e(0x992,0x1d4,0x770,0x6af)+_0x15f88e(0xfaa,0x102c,0x9a7,0xd0b)+_0x15f88e(0xece,0xf28,0x99c,0xda9)+'_')});function _0x5be99e(_0x2356ba,_0x3e6400,_0x205445,_0x8c667a){return _0x1da9f3(_0x3e6400- -0x2e0,_0x2356ba,_0x205445-0x189,_0x8c667a-0x29);}function _0x15f88e(_0x5261dc,_0x408530,_0x24e433,_0x3c71e6){return _0x1da9f3(_0x3c71e6-0x173,_0x24e433,_0x24e433-0x0,_0x3c71e6-0xf7);}for(let _0x59630e of _0x515ef0[_0x5be99e(-0x172,0xce,-0x439,-0x3b6)]){if(_0x41f410[_0x15f88e(0xb9a,0xb1b,0x64a,0xb09)](_0x41f410['rTZPZ'],_0x5be99e(0x7f6,0x744,0x6c2,0x971)))_0x41f410[_0x5be99e(0x715,0x761,0xa68,0xa6e)](_0xffa7b7,_0x1aacab['error']['api']);else{if(_0x59630e[_0x5be99e(0x45d,0x1cd,0x113,0x6d1)]['includes'](_0x15f88e(0xcc1,0xb18,0xe59,0xa9d))){let _0x5d1141=await getBuffer(_0x59630e[_0x15f88e(0x46d,0x605,0x5f6,0x620)]);alpha[_0x15f88e(0x5eb,0x472,0x7c,0x390)+'e'](from,_0x5d1141,video,{'thumbnail':Buffer[_0x15f88e(0x9bd,0x8db,0x8eb,0xa35)](-0x1129+-0x76a+-0x2bb*-0x9),'quoted':mek,'caption':_0x5be99e(0xb81,0x797,0x3d2,0x8f1)+_0x5be99e(0x769,0x4e5,0xa20,0x969)+_0x59630e[_0x5be99e(0x6ba,0x567,0xcf,0x83e)]});}else{if(_0x41f410['PxZyO'](_0x41f410[_0x15f88e(0xcdc,0xee0,0xa25,0xdbe)],_0x41f410['PvPdt'])){let _0x1fe39b=await _0x41f410[_0x15f88e(0xfab,0x775,0x947,0xbb4)](getBuffer,_0x59630e[_0x5be99e(0x6b0,0x1cd,-0x110,-0x263)]);alpha[_0x5be99e(-0x212,-0xc3,0x1e6,-0x396)+'e'](from,_0x1fe39b,image,{'thumbnail':Buffer[_0x15f88e(0xec9,0xa9a,0x81f,0xa35)](-0x1d2*-0xb+-0xdfb*0x1+-0x60b),'quoted':mek,'caption':_0x15f88e(0x1136,0xc58,0xdea,0xbea)+'•\x20'+_0x59630e[_0x15f88e(0xb9c,0x548,0xa89,0x9ba)]});}else _0x41f410['FefsT'](_0x38e72f,_0x174a48['error']['api']);}}}})[_0x1da9f3(0x893,0x638,0xd29,0xd5b)](_0x58e597=>reply('Link\x20tidak'+'\x20valid\x20ata'+_0x3f34aa(0x5a5,0x8d2,0x828,0xa6d)+'user\x20priva'+'te')),await limitAdd(sender,limit);break;case _0x3f34aa(0xa27,0x4bd,0x602,0x34c):const _0x5e4307={};_0x5e4307[_0x1da9f3(0xb79,0xbd0,0xb60,0x9ef)+'t']='Check\x20Limi'+'t';const _0x262e11={};_0x262e11[_0x1da9f3(0x4c3,0x4ab,0x70c,0x587)]=_0x1da9f3(0x886,0xc50,0xc7b,0x7fc),_0x262e11[_0x1da9f3(0x8a3,0x7e9,0x5ed,0xc88)]=_0x5e4307,_0x262e11[_0x3f34aa(0x8e9,0xa3a,0x693,0x2ec)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x76e,0xbbb,0x6d2,0x271)](prefix),'©\x20'+ownername,[_0x262e11]);if(args[_0x3f34aa(0x525,0x98d,0x92e,0xd51)]<0xa32+0x1bd5+-0x2606)return reply(lang[_0x1da9f3(0xaf4,0xa10,0xeef,0x7b8)](prefix,command));hhh=q,p=await getBuffer(_0x1da9f3(0x7e1,0x9da,0x9ef,0xcc3)+'rdianto-ch'+_0x1da9f3(0x7b1,0x9cd,0x578,0xc31)+_0x1da9f3(0x9f9,0xc4d,0xc0f,0x4ac)+_0x3f34aa(0x5e3,-0x2af,0xb1,-0x3c8)+'?text='+hhh+(_0x1da9f3(0xb6a,0x1032,0xacf,0x1048)+_0x1da9f3(0x53a,0x624,0x4c2,0x20f)+_0x1da9f3(0xb86,0x8f5,0x65a,0x919)));const _0x1b4577={};_0x1b4577[_0x1da9f3(0x96d,0x90d,0x909,0x996)]=fgclink,alpha[_0x1da9f3(0x21d,0x4eb,0x109,0x2b2)+'e'](from,p,sticker,_0x1b4577),await limitAdd(sender,limit);break;case _0x1da9f3(0xa74,0x856,0xfba,0x57e):const _0x8b4292={};_0x8b4292['displayTex'+'t']=_0x3f34aa(0x208,0x362,0x4e1,0x691)+'t';const _0x4f1a80={};_0x4f1a80[_0x1da9f3(0x4c3,0x434,0x438,0xe8)]=_0x1da9f3(0x886,0xb13,0x576,0x47a),_0x4f1a80[_0x1da9f3(0x8a3,0x504,0x53f,0x48e)]=_0x8b4292,_0x4f1a80[_0x3f34aa(0xb80,0xbc0,0x693,0x30d)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x4f1a80]);if(args[_0x1da9f3(0xae2,0xcff,0xa02,0xe5f)]<0x2d9*-0x6+0x1d*-0x14b+0x89*0x66)return reply(lang[_0x3f34aa(0xe28,0xd97,0x940,0x704)](prefix,command));hhh=q,pp=await getBuffer('https://ha'+_0x1da9f3(0x272,0x6bc,0x772,-0xf2)+_0x3f34aa(0x7b9,0x357,0x5fd,0x1ad)+_0x3f34aa(0xd1b,0x990,0x845,0x359)+'/ttpcustom'+'?text='+hhh+(_0x3f34aa(0x18d,0x5b,0x454,0x5c5)+'low&apikey'+'=hardianto'));const _0x453a2a={};_0x453a2a[_0x1da9f3(0x96d,0x93e,0x817,0xa92)]=fgclink,alpha[_0x1da9f3(0x21d,0x5b,-0x219,0x1f1)+'e'](from,pp,sticker,_0x453a2a),await limitAdd(sender,limit);break;case _0x1da9f3(0x2ca,-0x75,0x457,0x69e):const _0x5c0676={};_0x5c0676[_0x1da9f3(0xb79,0x99e,0xa13,0x74f)+'t']='Check\x20Limi'+'t';const _0x82aadc={};_0x82aadc[_0x1da9f3(0x4c3,0x4f5,0x3e,0x246)]=_0x3f34aa(0x833,0xa60,0x6d2,0x434),_0x82aadc[_0x1da9f3(0x8a3,0x3e8,0xca8,0xb4d)]=_0x5c0676,_0x82aadc[_0x1da9f3(0x847,0xd47,0xa91,0xd72)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x82b,0x386,0xa9c)](prefix),'©\x20'+ownername,[_0x82aadc]);if(args[_0x1da9f3(0xae2,0xe58,0x7b9,0x89c)]<0x11*-0x2b+-0x14ef+0x1*0x17cb)return reply(lang[_0x1da9f3(0xaf4,0x7dd,0x7cb,0x706)](prefix,command));hhh=q,ppp=await getBuffer('https://ha'+_0x1da9f3(0x272,0x75b,-0xf4,0x761)+_0x1da9f3(0x7b1,0x264,0x4b0,0x6c3)+_0x1da9f3(0x9f9,0x633,0xa7c,0x974)+_0x1da9f3(0x265,0x41c,-0x53,0xd8)+'?text='+hhh+(_0x1da9f3(0x309,0x3ed,0x2f1,-0x153)+_0x3f34aa(0x407,-0x167,0x30d,-0xd4)+'rdianto'));const _0xd8ab00={};_0xd8ab00['quoted']=fgclink,alpha[_0x1da9f3(0x21d,0x54d,-0x161,0x6eb)+'e'](from,ppp,sticker,_0xd8ab00),await limitAdd(sender,limit);break;case'ttp4':const _0x53b79b={};_0x53b79b[_0x1da9f3(0xb79,0x6cf,0x720,0xd36)+'t']='Check\x20Limi'+'t';const _0x3964e7={};_0x3964e7[_0x1da9f3(0x4c3,0x233,0x624,0x455)]='limit',_0x3964e7[_0x3f34aa(0x60f,0xa8f,0x6ef,0x227)]=_0x53b79b,_0x3964e7[_0x3f34aa(0x350,0x953,0x693,0x433)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x3964e7]);if(args[_0x3f34aa(0x5d8,0x57a,0x92e,0x92a)]<0x1*-0xe5d+0xf*-0x156+0x2268)return reply(lang[_0x1da9f3(0xaf4,0x646,0xae3,0x92e)](prefix,command));hhh=q,pppp=await getBuffer(_0x3f34aa(0x503,0x674,0x62d,0x61f)+_0x1da9f3(0x272,-0x111,0x2a1,0x1a9)+_0x3f34aa(0x350,0x8c6,0x5fd,0x3ad)+_0x3f34aa(0x506,0x72c,0x845,0x7b9)+'/ttpcustom'+_0x3f34aa(0x960,0x56b,0x873,0xb7b)+hhh+(_0x3f34aa(-0x94,0xf5,0x149,0x328)+_0x1da9f3(0x49f,0x144,0x7d1,0x7c3)+_0x3f34aa(0x5a8,0x625,0x9d2,0xb75)));const _0x32ee6d={};_0x32ee6d[_0x3f34aa(0x680,0x72f,0x7b9,0x9be)]=fgclink,alpha[_0x1da9f3(0x21d,0x401,0x3ab,0x51e)+'e'](from,pppp,sticker,_0x32ee6d),await limitAdd(sender,limit);break;case _0x1da9f3(0x88c,0x5c1,0x8b6,0xb1d):case _0x3f34aa(0x61b,-0x5f,0x473,0x5b8):const _0x2f4a2a={};_0x2f4a2a[_0x1da9f3(0xb79,0xbf1,0xfad,0x106f)+'t']=_0x3f34aa(0x88e,0xa08,0x4e1,0x8c2)+'t';const _0x3e1c56={};_0x3e1c56[_0x3f34aa(0x705,0x1ff,0x30f,-0x23c)]='limit',_0x3e1c56['buttonText']=_0x2f4a2a,_0x3e1c56[_0x1da9f3(0x847,0x911,0x866,0xb02)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x761,0x2b6,0x6d2,0x255)](prefix),'©\x20'+ownername,[_0x3e1c56]);if(!isGroup)return reply(lang[_0x1da9f3(0x426,0x29d,0x4df,0x876)]());var datax=fs[_0x3f34aa(0xd8,0x243,0x111,0x32e)+'nc'](_0x1da9f3(0x412,0x211,-0xa3,0x28b)+_0x1da9f3(0xa57,0xebf,0x736,0xcc8));jsonData=JSON['parse'](datax),randIndex=Math[_0x3f34aa(0x5f,0x60b,0x3cc,0x4a5)](Math[_0x3f34aa(0xbc2,0xa98,0x7a3,0x960)]()*jsonData[_0x3f34aa(0x46b,0xe18,0x92e,0x3f1)]);let randKey_=jsonData[randIndex];Laki=await getBuffer(randKey_['male']),await alpha['sendMessag'+'e'](from,Laki,image,{'caption':_0x3f34aa(0x445,0xb15,0x7d5,0x808),'quoted':mek,'thumbnail':Buffer[_0x1da9f3(0x8c2,0x530,0x8e5,0x5c1)](-0xb7b+-0x260c+0x3187)}),Cewe=await getBuffer(randKey_['female']);const mediaxox=await alpha[_0x1da9f3(0xc58,0xebb,0xa58,0xf76)+_0x3f34aa(0xc12,0x35c,0x871,0xbde)](from,Cewe,MessageType['image'],{'thumbnail':Buffer['alloc'](-0x427+-0x35f*0x1+0x3*0x282)});let bacotluxxo=mediaxox[_0x3f34aa(0xa63,0x471,0x7d8,0xa15)][_0x3f34aa(0x581,0x403,0x636,0x21d)+_0x1da9f3(0x9a1,0x577,0x715,0xd87)]?mediaxox[_0x1da9f3(0x98c,0x721,0xb53,0xe35)]['ephemeralM'+'essage']:mediaxox;const _0x4bab03={};_0x4bab03[_0x3f34aa(0xbca,0x5b8,0x9c5,0xad7)+'t']='🐨\x20Owner';const _0xe4940={};_0xe4940[_0x3f34aa(0x42,0x718,0x30f,0x742)]='owner',_0xe4940[_0x1da9f3(0x8a3,0x6a9,0x428,0x3bb)]=_0x4bab03,_0xe4940[_0x3f34aa(0x3b5,0x196,0x693,0x7e5)]=0x1;const _0x145db6={};_0x145db6[_0x3f34aa(0xa7d,0xe6d,0x9c5,0xc71)+'t']=_0x3f34aa(0xb80,0x3d6,0x781,0x9aa);const _0x6a6eb={};_0x6a6eb[_0x3f34aa(0x44,0x2b1,0x30f,0x94)]=''+command,_0x6a6eb[_0x1da9f3(0x8a3,0x608,0x4e2,0xd33)]=_0x145db6,_0x6a6eb[_0x3f34aa(0x8e2,0x9ff,0x693,0x169)]=0x1;const buttonsxox=[_0xe4940,_0x6a6eb],btnxo___={'contentText':_0x1da9f3(0x8ad,0xc96,0xc25,0x903),'footerText':''+tampilTanggal+enter+tampilWaktu+enter+enter+'©\x20'+creator+(_0x1da9f3(0x2f3,0x2f4,0x613,-0x220)+_0x1da9f3(0x772,0x4ef,0xcbc,0x41c))+sender['split']('@')[0x3*-0x1bd+0x1708+-0x11d1],'buttons':buttonsxox,'headerType':0x4,'imageMessage':bacotluxxo[_0x3f34aa(0x883,0xcd2,0x7d8,0xc74)][_0x1da9f3(0x6fe,0x622,0x6c5,0x4a1)+'ge']},_0x9aa9c={};_0x9aa9c[_0x1da9f3(0x96d,0x5f8,0x45d,0x5e0)]=mek,alpha[_0x3f34aa(0x459,0x147,0x69,-0x343)+'e'](from,btnxo___,MessageType['buttonsMes'+_0x1da9f3(0xa25,0xcba,0x9c8,0x78c)],_0x9aa9c)['catch'](_0x524b49=>{function _0x1fc7cb(_0xca72f7,_0x646ce3,_0x18dcab,_0x34a6e2){return _0x3f34aa(_0xca72f7-0x69,_0x34a6e2,_0x18dcab-0x4b8,_0x34a6e2-0x173);}const _0x109134={};_0x109134['dnsei']='Terjadi\x20ke'+_0x2a643(-0x13f,-0x182,0x592,0x308)+_0x1fc7cb(0xdba,0x13d7,0xef6,0x1062)+'pa\x20saat\x20la'+'gi';const _0x5c829e=_0x109134;function _0x2a643(_0x5da1e3,_0xaf8e3c,_0x55154b,_0x536f64){return _0x3f34aa(_0x5da1e3-0x18c,_0xaf8e3c,_0x536f64-0x2be,_0x536f64-0xf8);}reply(_0x5c829e['dnsei']);}),await limitAdd(sender,limit);break;case _0x1da9f3(0x5d4,0xa61,0x8c4,0x183):case _0x3f34aa(0xbb6,0xea6,0xaa0,0xd5b):const _0x174c97={};_0x174c97[_0x1da9f3(0xb79,0xe8d,0x740,0x86e)+'t']='Check\x20Limi'+'t';const _0xb5bbda={};_0xb5bbda[_0x3f34aa(0x694,-0x8b,0x30f,0x24e)]='limit',_0xb5bbda[_0x3f34aa(0xb23,0x499,0x6ef,0xad8)]=_0x174c97,_0xb5bbda[_0x3f34aa(0xbdb,0x7aa,0x693,0x2b6)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x530,0xc6e,0xc44)](prefix),'©\x20'+ownername,[_0xb5bbda]);if(!q)return reply(_0x1da9f3(0x9e2,0xc09,0xb4e,0xcd0)+_0x3f34aa(0x5e9,0x6ee,0x810,0x5e2));async function pinterestSearch(_0x523132){const _0x411605={'HrqpF':_0x48e278(0x749,0x9d2,0x7eb,0xa2a),'JxdMO':function(_0xb2db73,_0x211405){return _0xb2db73*_0x211405;},'MdHsn':function(_0x37a21a,_0x1bb883){return _0x37a21a(_0x1bb883);},'mfVkw':'no-cache','DcZiG':_0x474dc9(0xc1e,0xf81,0x7d3,0xe3b),'vxlzq':_0x48e278(0x457,0x788,0x885,0xa28)+'n','AyldB':_0x474dc9(0x73e,0x468,0xaf3,0x527),'EAHuS':_0x474dc9(0xc6f,0x876,0xf66,0xd2b),'mTzbL':_0x48e278(0x7c9,0x4f8,0x3d0,0x597)+'uest','HcrAN':_0x48e278(0x5a8,0x3a0,0x8d7,0x95a)+'w.pinteres'+_0x474dc9(0xb1b,0xd40,0xcc4,0xabd)};function _0x474dc9(_0x56344c,_0x3148bc,_0x51fbda,_0x45253f){return _0x3f34aa(_0x56344c-0x1e2,_0x3148bc,_0x56344c-0x234,_0x45253f-0x3d);}function _0x48e278(_0x561637,_0x2ff08e,_0x29e19f,_0x41df23){return _0x1da9f3(_0x29e19f- -0x314,_0x2ff08e,_0x29e19f-0x84,_0x41df23-0xe0);}return new Promise((_0x495901,_0x2cc1af)=>{const _0x256fbf={'uBHrl':function(_0x1e77a2,_0x3cd0a0,_0x44bb38){return _0x1e77a2(_0x3cd0a0,_0x44bb38);}},_0x1dfe8f={};_0x1dfe8f[_0xfed005(0xb84,0x614,0xab5,0x6a6)]=_0x1950ed(0x914,0x726,0x68e,0x751)+_0xfed005(0x57,0x27a,-0x8a,0x38e)+_0x1950ed(0x7c8,0x7b3,0xbf2,0x96b)+_0x1950ed(0xbd0,0xd10,0x94d,0x564)+_0x1950ed(0x851,0x632,0x870,0x5b7),_0x1dfe8f[_0x1950ed(0x780,-0x6,0x4c4,0x81f)+'guage']=_0xfed005(0x7ae,0x801,0xbc0,0x6f5)+_0xfed005(0xc1b,0xc9f,0x844,0xa8e),_0x1dfe8f[_0x1950ed(0x405,0x3f5,0x320,0x3d8)+_0x1950ed(0xb51,0xfd6,0xc94,0xc46)]=_0x411605[_0x1950ed(0x7f3,0x6c9,0x4da,-0x4f)],_0x1dfe8f[_0x1950ed(0x596,0xa30,0xa30,0xf71)]='no-cache',_0x1dfe8f[_0x1950ed(0x515,0x52f,0x5df,0x3ac)+_0x1950ed(0xa47,0x164,0x4fa,0x46)]='empty',_0x1dfe8f[_0xfed005(0x713,0x2be,0x748,0x374)+'mode']=_0x411605[_0xfed005(0xb28,0x60f,0x63f,0x776)],_0x1dfe8f[_0xfed005(0x2cd,0x697,0x517,0x374)+_0xfed005(-0x17a,-0x455,0x171,0xe6)]=_0x411605[_0xfed005(0x354,0x6db,0x7d2,0x3b9)],_0x1dfe8f['sec-gpc']='1',_0x1dfe8f[_0xfed005(0x509,0x3d2,0x409,0x3e9)+'ion']=_0x411605['AyldB'],_0x1dfe8f[_0xfed005(0x89,0x1a5,0x695,0x5d9)+_0xfed005(0x578,0xac5,0xec0,0x9f6)]=_0x411605[_0x1950ed(0x8a0,0x804,0xb27,0x918)],_0x1dfe8f['x-requeste'+_0xfed005(0x4c3,-0x328,0x183,0x15a)]=_0x411605[_0xfed005(0x271,-0x263,-0x2ac,0x254)];const _0x584ec7={};_0x584ec7[_0xfed005(0x73c,0x5e2,0x3e1,0x4ef)]=_0x1dfe8f,_0x584ec7['referrer']=_0x411605[_0x1950ed(0x94e,0x7a4,0x534,0x203)];function _0x1950ed(_0x3ef764,_0x2e428e,_0x368c13,_0x3f382e){return _0x48e278(_0x3ef764-0x1b8,_0x3ef764,_0x368c13-0x3ea,_0x3f382e-0x14a);}_0x584ec7[_0xfed005(0x338,0x37b,0x31e,0x301)+'licy']='origin';function _0xfed005(_0x4c8426,_0x2972c7,_0x40d3a1,_0x17f44c){return _0x474dc9(_0x17f44c- -0x215,_0x40d3a1,_0x40d3a1-0x3d,_0x17f44c-0x134);}_0x584ec7['body']=null,_0x584ec7[_0xfed005(0x568,0x3cf,0x3aa,0x6d6)]='GET',_0x584ec7[_0x1950ed(0xa17,0xb32,0xcf1,0x92b)]=_0x411605[_0xfed005(0x4b9,0x21f,0x358,0x776)],fetch(_0x1950ed(0x9ab,0x1025,0xcc1,0x95c)+_0x1950ed(0xa23,0x79f,0xb10,0x662)+'t.com/reso'+_0x1950ed(0x1d8,0x234,0x310,0x6d9)+_0xfed005(0xa7e,0xa75,0x6de,0x63f)+_0xfed005(-0x69,-0x11f,-0x19d,0x10c)+'ource_url='+'%2Fsearch%'+_0x1950ed(0x325,0x196,0x3d6,0x576)+_0x1950ed(0x1131,0xbbd,0xd17,0xaec)+_0x523132+(_0xfed005(0x4ae,-0x282,-0x109,0x29d)+_0x1950ed(0xa39,0xa58,0xa11,0x573)+'22%3A%7B%2'+_0xfed005(0x46f,0x476,0x555,0x5a)+'h%22%3Afal'+'se%2C%22qu'+'ery%22%3A%'+'22')+_0x523132+(_0x1950ed(0x4c2,0x854,0x63c,0x3a3)+_0x1950ed(0x1a3,-0x69,0x2ff,0x4f0)+'%22pins%22'+_0x1950ed(0xc05,0x11be,0xc70,0xb2a)+_0xfed005(0x13d,0x9e7,0x802,0x60d)+'xt_on_reso'+_0xfed005(0xbf7,0xb4f,0x896,0x88e)+_0xfed005(0x7b,0x1f6,0x8a4,0x3b5)+_0xfed005(0xed,0x11c,0x44d,0xda)+_0xfed005(0x3b6,0x202,0x535,0x212)+_0x1950ed(0xcd5,0x61f,0xa5c,0x80c)+_0xfed005(0x799,0x614,0x582,0x7d2)+'59'),_0x584ec7)['then'](_0x291d3f=>_0x291d3f[_0x1950ed(0xdc4,0xc01,0xa7c,0xd58)]())['then'](_0x2a0453=>{function _0x3e9148(_0x1d03a1,_0xade0c2,_0x412483,_0x5c6e1b){return _0xfed005(_0x1d03a1-0x87,_0xade0c2-0xff,_0xade0c2,_0x1d03a1- -0x211);}function _0x387800(_0x58b882,_0xc3f017,_0x58af91,_0x2070de){return _0x1950ed(_0x58b882,_0xc3f017-0x1b1,_0x2070de- -0xb6,_0x2070de-0x17a);}if(_0x411605['HrqpF']===_0x411605[_0x3e9148(0x78e,0xaff,0xa88,0xadf)]){const _0x44f5aa=_0x2a0453[_0x3e9148(0x4e3,0x53,0x4e5,0x83d)+_0x3e9148(0x73b,0x302,0x613,0x94c)][_0x3e9148(0x2c4,-0x13b,-0x28f,0x481)][_0x387800(0x970,0xf83,0x827,0xb24)][Math[_0x3e9148(0x1da,0x4ef,-0x337,0xb5)](_0x411605[_0x387800(0x187,-0xfb,0x181,0x2b9)](Math[_0x387800(0x665,0xb2b,0x808,0x977)](),_0x2a0453[_0x387800(0xa17,0x7ba,0x953,0x8a9)+_0x3e9148(0x73b,0x63f,0x5c8,0x42c)][_0x3e9148(0x2c4,0x2aa,0x529,-0x1db)][_0x3e9148(0x75e,0xb80,0xb50,0x92f)][_0x3e9148(0x73c,0x983,0x830,0x7b6)]))];var _0x511878=[];const _0x4af88e={};_0x4af88e['link']=_0x44f5aa[_0x387800(0x520,-0x1b3,0x88f,0x39b)][_0x387800(0xe5b,0xe98,0x671,0xa4c)][_0x3e9148(0x107,0x127,0x660,0x560)],_0x511878[_0x387800(0x9d3,0xc1a,0x4cb,0x75d)](_0x4af88e),_0x411605[_0x387800(0x3d0,0x340,0x426,0x7f9)](_0x495901,_0x511878);}else _0x355fcd+=_0x387800(0xbd4,0xcdb,0x835,0xbb4)+_0x2908ca[_0x3e9148(0x7b3,0xc51,0x950,0xa4e)][_0x387800(0x5a1,0x8aa,0xbb3,0x99b)]('@')[0x1e4d+-0x5f5*0x2+-0x9*0x20b]+_0x2b56ad,_0x2a25c9+=_0x3e9148(0x41e,0x74f,0x5d,0x442)+_0x256fbf[_0x3e9148(0xc5,0x5c9,0x5c7,-0xd1)](_0x24f537,_0x11a322['t'],!![])+_0x48d390+(_0x3e9148(0x14b,0x346,-0x233,0x199)+':\x20')+_0x392fa4(_0x2e2170['t'],!![])+_0x224e29+'•'+_0x146d3a;})['catch'](_0x2cc1af);});}const pinterest=_0x179954=>new Promise((_0x5eb788,_0x244803)=>{const _0x5646b2={'xEfin':function(_0x4b4403,_0x2f4a62){return _0x4b4403(_0x2f4a62);}};function _0x4e472e(_0x109cf6,_0x5b3d22,_0x43a71e,_0x30b575){return _0x3f34aa(_0x109cf6-0xcd,_0x109cf6,_0x43a71e-0x47d,_0x30b575-0x23);}function _0x353428(_0x23dd39,_0x416654,_0x38420c,_0x675d4c){return _0x3f34aa(_0x23dd39-0x18d,_0x416654,_0x38420c-0x579,_0x675d4c-0xba);}_0x5646b2[_0x353428(0x84d,0x7fa,0xcbe,0xaa9)](pinterestSearch,_0x179954)[_0x353428(0x636,0x5cd,0x5a5,0x231)](_0x1a5b8a=>{function _0x3d3fd1(_0x3ffd9c,_0x4a7485,_0x2a7412,_0x27c90d){return _0x353428(_0x3ffd9c-0x12f,_0x4a7485,_0x27c90d- -0x282,_0x27c90d-0x183);}const _0x4ad625={};_0x4ad625[_0x3d3fd1(0x529,0x64c,-0x138,0x325)]=0xc8;function _0x46c55e(_0x3f900d,_0x10ee65,_0x187759,_0x26bba0){return _0x4e472e(_0x10ee65,_0x10ee65-0x1d3,_0x26bba0- -0x17,_0x26bba0-0x134);}_0x4ad625[_0x46c55e(0x5d2,0xb3f,0xcf8,0x9fd)]=_0x1a5b8a[-0x126a+0x1447*0x1+-0x9f*0x3]['link'],_0x5646b2['xEfin'](_0x5eb788,_0x4ad625);})['catch'](_0x244803);});pinterest(q)[_0x3f34aa(-0x3d9,-0x302,0x2c,-0x352)](async _0x626cd2=>{function _0x38a319(_0x1b4d60,_0x5ef5a0,_0x48bb8f,_0x3ee630){return _0x1da9f3(_0x5ef5a0- -0x98,_0x48bb8f,_0x48bb8f-0x1c0,_0x3ee630-0x14c);}const _0x88899={'cOVZS':function(_0x11eb48,_0x3a85bd){return _0x11eb48(_0x3a85bd);}};function _0x19b684(_0x34a1a8,_0x4e6dba,_0x58b79e,_0x64153){return _0x1da9f3(_0x58b79e-0x384,_0x4e6dba,_0x58b79e-0x90,_0x64153-0xa0);}let _0x8ca75f=await _0x88899[_0x19b684(0xd83,0x492,0x8a1,0xde3)](getBuffer,_0x626cd2['image']);alpha[_0x38a319(0x6ae,0x185,0x68e,-0x5a)+'e'](from,_0x8ca75f,MessageType[_0x38a319(0x98c,0x6b3,0x187,0x415)],{'caption':_0x19b684(0x430,0x191,0x5c3,0x3b8)+'arian:\x20'+q,'quoted':mek,'thumbnail':Buffer[_0x38a319(0xac6,0x82a,0x532,0x9dc)](-0x1*0xa75+-0xc03+0x1678)});})[_0x1da9f3(0x893,0x36f,0xbe6,0x8fa)](async _0x4772a1=>{function _0x31d9c8(_0x310118,_0x30b52e,_0x3fec3c,_0x337265){return _0x1da9f3(_0x3fec3c- -0x35c,_0x337265,_0x3fec3c-0xa6,_0x337265-0x1c5);}const _0x1bdb34={'sKCsE':function(_0x2a6d8d,_0x44fba6){return _0x2a6d8d(_0x44fba6);}};function _0xb86ecd(_0x32d545,_0x13fcab,_0x5f208c,_0x39bcee){return _0x3f34aa(_0x32d545-0xec,_0x39bcee,_0x32d545-0x54a,_0x39bcee-0x188);}_0x1bdb34[_0xb86ecd(0x6f2,0x9f1,0x807,0x35d)](reply,_0x31d9c8(0x601,0x111,0x230,0x576)+'salahan');}),await limitAdd(sender,limit);break;case'emoji':case _0x3f34aa(-0x2cf,0x316,0x1cf,-0x28f):const _0x41cd71={};_0x41cd71[_0x3f34aa(0xace,0xc46,0x9c5,0x7b2)+'t']='Check\x20Limi'+'t';const _0x8e85f7={};_0x8e85f7[_0x3f34aa(0x6cd,0x857,0x30f,0x85e)]=_0x1da9f3(0x886,0xccf,0x607,0xd90),_0x8e85f7['buttonText']=_0x41cd71,_0x8e85f7['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x8e85f7]);if(!q)return reply(_0x1da9f3(0x8cb,0x50c,0x6f3,0xccd));qes=args['join']('\x20');const _0x598e6a={};_0x598e6a[_0x1da9f3(0x7c9,0x919,0x59a,0x2e0)]=_0x1da9f3(0xa04,0xe78,0xd68,0xa1a)+'e-'+qes,_0x598e6a[_0x3f34aa(0x59c,0x5b6,0x7a0,0xc66)+'n']=''+creator,_0x598e6a[_0x1da9f3(0xc08,0x8ef,0xe7f,0xdf6)]=''+qes;const _0x473327={};_0x473327['title']=_0x3f34aa(0xfbf,0x6db,0xa8c,0xa5e)+'le-'+qes,_0x473327['descriptio'+'n']=''+creator,_0x473327[_0x3f34aa(0x903,0x8e4,0xa54,0x85c)]=''+qes;const _0x45d583={};_0x45d583['title']=_0x1da9f3(0x471,0x924,0x179,0x2a2)+_0x1da9f3(0x85b,0x828,0x9f7,0xb38)+qes,_0x45d583['descriptio'+'n']=''+creator,_0x45d583[_0x1da9f3(0xc08,0xa2c,0xa6b,0xee9)]=''+qes;const _0x4a4de3={};_0x4a4de3['title']=_0x3f34aa(0x518,0x659,0x423,0x60b)+_0x3f34aa(-0x21,0x3af,0x3e7,-0x108)+qes,_0x4a4de3[_0x1da9f3(0x954,0xc59,0x96f,0x66a)+'n']=''+creator,_0x4a4de3['rowId']=''+qes;const _0x401b30={};_0x401b30['title']=_0x1da9f3(0xb20,0xf13,0xdba,0x996)+_0x1da9f3(0x420,0x269,0x62d,0x3ed)+qes,_0x401b30[_0x3f34aa(0x597,0x339,0x7a0,0x7ff)+'n']=''+creator,_0x401b30[_0x3f34aa(0x808,0xc43,0xa54,0xdfd)]=''+qes;const _0x5d094b={};_0x5d094b[_0x1da9f3(0x7c9,0x399,0xa79,0x2cf)]=_0x1da9f3(0xa60,0xa85,0x533,0x9cc)+_0x3f34aa(0x27b,0x330,0x3e3,0x532)+qes,_0x5d094b['descriptio'+'n']=''+creator,_0x5d094b[_0x1da9f3(0xc08,0x8ac,0xc7e,0xc8d)]=''+qes;const _0x599703={};_0x599703[_0x1da9f3(0x7c9,0x946,0xb7f,0x94a)]=_0x1da9f3(0x7fe,0x714,0xb94,0x8bc)+_0x1da9f3(0x202,0x5ff,0x1ea,0x5bc)+qes,_0x599703[_0x1da9f3(0x954,0xc10,0xd04,0x4f3)+'n']=''+creator,_0x599703['rowId']=''+qes;const _0x55a369={};_0x55a369[_0x1da9f3(0x7c9,0xb38,0x9f2,0x7da)]=_0x3f34aa(0x58e,0x7a3,0x9d3,0xebc)+_0x1da9f3(0xb3b,0xaed,0xadb,0x68e)+qes,_0x55a369['descriptio'+'n']=''+creator,_0x55a369[_0x1da9f3(0xc08,0xd2c,0x7b0,0xbaf)]=''+qes;const _0x51a108={};_0x51a108[_0x1da9f3(0x7c9,0x77a,0x93c,0x6ab)]=_0x1da9f3(0x326,-0x1aa,-0x216,0x776)+_0x3f34aa(0x399,0xee,0x5f1,0x876)+qes,_0x51a108[_0x1da9f3(0x954,0x7ad,0x76b,0x8c1)+'n']=''+creator,_0x51a108[_0x1da9f3(0xc08,0xab4,0x724,0xd4a)]=''+qes;const _0x280341={};_0x280341[_0x1da9f3(0x7c9,0x73c,0x6a8,0x367)]=_0x1da9f3(0x1ea,0x650,-0x24d,-0x197)+_0x1da9f3(0x8a9,0x3ea,0xa5b,0x8f1)+qes,_0x280341[_0x3f34aa(0x685,0x26f,0x7a0,0x69a)+'n']=''+creator,_0x280341[_0x1da9f3(0xc08,0xa7f,0xdaa,0xc67)]=''+qes;const _0x4c5913={};_0x4c5913['title']='Emoji-Mess'+_0x3f34aa(0x4b,-0x280,0x234,0x625)+qes,_0x4c5913[_0x3f34aa(0x314,0x5fc,0x7a0,0x2cf)+'n']=''+creator,_0x4c5913[_0x1da9f3(0xc08,0xddb,0x7d7,0xc57)]=''+qes;const _0xaf963b={};_0xaf963b[_0x3f34aa(0x18b,0x6ca,0x615,0x669)]=_0x1da9f3(0x2ff,0x29d,-0x200,0xf)+qes,_0xaf963b[_0x1da9f3(0x954,0x9f5,0x966,0xcc2)+'n']=''+creator,_0xaf963b[_0x1da9f3(0xc08,0xbfa,0xb77,0x93f)]=''+qes;const _0x4ef12d={};_0x4ef12d[_0x1da9f3(0x7c9,0x919,0x2f4,0xafa)]=_0x1da9f3(0x5c1,0xab,0x814,0x6a2)+qes,_0x4ef12d['descriptio'+'n']=''+creator,_0x4ef12d[_0x1da9f3(0xc08,0x70e,0xf6a,0xb42)]=''+qes;const rows12=[_0x598e6a,_0x473327,_0x45d583,_0x4a4de3,_0x401b30,_0x5d094b,_0x599703,_0x55a369,_0x51a108,_0x280341,_0x4c5913,_0xaf963b,_0x4ef12d],_0x2b124b={};_0x2b124b[_0x3f34aa(0x977,0x878,0x615,0xae8)]=_0x3f34aa(0x37c,0xc24,0x849,0x4e9)+_0x1da9f3(0x7a3,0x6b7,0x3fe,0xbcb),_0x2b124b[_0x3f34aa(0x323,0x283,0x3f5,0x718)]=rows12;const sections12=[_0x2b124b],listt12={'buttonText':_0x3f34aa(0x9db,0x8b7,0x61f,0x8d0)+'E','title':_0x3f34aa(0x5d1,0xaac,0x6ee,0x848)+'\x20','description':_0x3f34aa(0x63d,0x9cf,0x788,0x38d)+sender[_0x1da9f3(0x97b,0x592,0xd97,0xa5e)]('@')[-0xdd+-0x1d9f+0x1e7c]+('\x20Semoga\x20ha'+'rimu\x20menye'+_0x1da9f3(0x200,0x471,0x584,0x6e4)+_0x3f34aa(0x818,0x54c,0x9da,0x704)+'h\x20list\x20emo'+'ji\x20silahka'+_0x3f34aa(0x5a8,0xb45,0x867,0x361)),'footerText':''+tampilTanggal+enter+tampilWaktu,'sections':sections12,'listType':0x1},_0x2facdd={};_0x2facdd[_0x1da9f3(0x32f,0x61b,-0x165,0x7c4)+'id']=[sender];const _0x17b53d={};_0x17b53d[_0x3f34aa(0x955,0x289,0x7b9,0x5df)]=ftoko,_0x17b53d['contextInf'+'o']=_0x2facdd,alpha[_0x3f34aa(0xe,0x3cd,0x69,-0x37e)+'e'](from,listt12,MessageType['listMessag'+'e'],_0x17b53d),await limitAdd(sender,limit);break;}if(sub_yt_zeeone_ofc==_0x3f34aa(0x88e,0x818,0x850,0x913)+'e-'+q5){const _0x5efdff={};_0x5efdff['displayTex'+'t']='Check\x20Limi'+'t';const _0x4db240={};_0x4db240[_0x3f34aa(-0x210,0xe,0x30f,0x7ec)]=_0x1da9f3(0x886,0x571,0xa51,0x5a5),_0x4db240[_0x1da9f3(0x8a3,0x3c2,0x503,0xc66)]=_0x5efdff,_0x4db240[_0x3f34aa(0x7cf,0x9d0,0x693,0x42d)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x534,0x7c5,0x999)](prefix),'©\x20'+ownername,[_0x4db240]);reply(lang[_0x1da9f3(0x723,0x690,0x922,0x6ac)]()),emoji[_0x1da9f3(0x4cf,0x2e5,0x9ff,0x6e8)](''+q5)[_0x3f34aa(-0x15e,-0x4fe,0x2c,0x361)](async _0x1c02a0=>{const _0x347c12={'SfBOF':function(_0x56fe7e,_0x2c5348,_0x33731a){return _0x56fe7e(_0x2c5348,_0x33731a);}};function _0x436f0c(_0x4b46eb,_0x26db59,_0x3a693e,_0x53eb43){return _0x3f34aa(_0x4b46eb-0xfb,_0x4b46eb,_0x26db59- -0xa7,_0x53eb43-0x170);}teks=''+_0x1c02a0['images'][0xd4*-0x22+-0x2*-0xa63+0x762][_0x51e84c(-0x14c,0x5de,0x1f9,-0x33a)],console[_0x51e84c(0x395,0x6f4,0x41e,0x6a8)](teks);function _0x51e84c(_0x5cf52d,_0x3aa6ab,_0x94ff95,_0x3166e0){return _0x3f34aa(_0x5cf52d-0x75,_0x3aa6ab,_0x94ff95- -0x100,_0x3166e0-0x92);}_0x347c12[_0x436f0c(0x489,0x8fc,0xb7b,0xa76)](sendStickerFromUrl,from,''+teks),await limitAdd(sender,limit);});}function _0xfa31(_0x2db72f,_0x5dc8fe){const _0x190ebe=_0x69ea();return _0xfa31=function(_0x431d93,_0x50b9a7){_0x431d93=_0x431d93-(0x1949+0x25f3+0x1a*-0x25f);let _0x5e93e8=_0x190ebe[_0x431d93];return _0x5e93e8;},_0xfa31(_0x2db72f,_0x5dc8fe);}if(sub_yt_zeeone_ofc==_0x3f34aa(0x592,0x97b,0xa8c,0x8f6)+_0x3f34aa(0x941,0x8a5,0x6eb,0xa18)+q5){const _0xe16037={};_0xe16037[_0x1da9f3(0xb79,0xe9d,0x7e7,0x947)+'t']=_0x1da9f3(0x695,0x554,0x825,0xbb4)+'t';const _0x123352={};_0x123352[_0x3f34aa(0x2eb,0x5d,0x30f,-0x98)]=_0x1da9f3(0x886,0x54c,0x71f,0xa76),_0x123352['buttonText']=_0xe16037,_0x123352[_0x1da9f3(0x847,0x79e,0x9ac,0xa30)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x123352]);emoji['get'](''+q5)['then'](async _0x5c9423=>{const _0x32e009={'vutwz':function(_0x519336,_0x408ed2,_0x1faa29){return _0x519336(_0x408ed2,_0x1faa29);}};teks=''+_0x5c9423['images'][-0x27*0x1f+-0x16e*0x4+0xa72]['url'],console[_0x5f449d(0x7c4,0x540,0xb9b,0x3ab)](teks);function _0x6838b8(_0x7fd335,_0x182740,_0x3827f0,_0x1664a9){return _0x1da9f3(_0x1664a9-0x31d,_0x3827f0,_0x3827f0-0x1cd,_0x1664a9-0x148);}function _0x5f449d(_0x22bed3,_0x192627,_0x11d284,_0x272ddd){return _0x3f34aa(_0x22bed3-0x1e4,_0x192627,_0x22bed3-0x2a6,_0x272ddd-0xc5);}_0x32e009[_0x5f449d(0xd17,0xd1d,0x1028,0x115c)](sendStickerFromUrl,from,''+teks),await _0x32e009[_0x5f449d(0xd17,0x81f,0xec1,0x8ec)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc=='Emoji-Sams'+_0x3f34aa(0x9e4,0xa04,0x6a7,0x3e3)+q5){const _0x397bff={};_0x397bff[_0x3f34aa(0xc53,0xd08,0x9c5,0xe19)+'t']=_0x1da9f3(0x695,0x77f,0x9e0,0x16c)+'t';const _0x4c67e7={};_0x4c67e7[_0x3f34aa(0xc5,0x5a9,0x30f,0x489)]=_0x1da9f3(0x886,0x9bd,0xa38,0x656),_0x4c67e7['buttonText']=_0x397bff,_0x4c67e7[_0x3f34aa(0x264,0x38b,0x693,0x964)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0xb2e,0x92b,0x6d2,0x1b2)](prefix),'©\x20'+ownername,[_0x4c67e7]);reply(lang[_0x3f34aa(0x395,0x204,0x56f,0x576)]()),emoji['get'](''+q5)['then'](async _0x40fd6b=>{const _0x51b6f8={'OnzqK':function(_0x595777,_0x6067f7,_0x38e988){return _0x595777(_0x6067f7,_0x38e988);}};function _0x4621e7(_0x40324f,_0x3358ee,_0x2f2743,_0x23571e){return _0x1da9f3(_0x40324f- -0x3ad,_0x2f2743,_0x2f2743-0x61,_0x23571e-0x7d);}teks=''+_0x40fd6b['images'][0x5*0x65a+-0x1*0x2263+0x2a3][_0x5b3877(0x367,-0xc6,0x443,0x32a)];function _0x5b3877(_0x5be94b,_0x16c793,_0x381e67,_0x8566f){return _0x3f34aa(_0x5be94b-0x92,_0x381e67,_0x5be94b-0x6e,_0x8566f-0x9);}console[_0x5b3877(0x58c,0x692,0xaa1,0x614)](teks),_0x51b6f8[_0x4621e7(0x58c,0x8ee,0x90b,0x803)](sendStickerFromUrl,from,''+teks),await limitAdd(sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0x5d7,0x246,0x2be,0x60c)+_0x3f34aa(0x3c7,0x490,0x3e7,0x586)+q5){const _0x3c86a1={};_0x3c86a1['displayTex'+'t']=_0x3f34aa(0x2d6,0x553,0x4e1,0xa0b)+'t';const _0x59a264={};_0x59a264[_0x1da9f3(0x4c3,0x873,0x6a0,-0x13)]=_0x1da9f3(0x886,0x748,0x54a,0x865),_0x59a264[_0x3f34aa(0x5a6,0x2a0,0x6ef,0x202)]=_0x3c86a1,_0x59a264[_0x3f34aa(0x6f1,0x20c,0x693,0x4b4)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0xbb1,0xc57,0xbe6)](prefix),'©\x20'+ownername,[_0x59a264]);emoji['get'](''+q5)[_0x3f34aa(0x480,0x3,0x2c,0x211)](async _0x6a2fe7=>{const _0x3025fc={'iKHaG':function(_0x5b4f96,_0x251c2f,_0x51784f){return _0x5b4f96(_0x251c2f,_0x51784f);},'iBwav':function(_0x2e16e6,_0xf78d70,_0x3052e4){return _0x2e16e6(_0xf78d70,_0x3052e4);}};teks=''+_0x6a2fe7[_0x52c883(0x1bb,0x4b0,0xd,-0x113)][-0xb*-0x33d+0x1*0x59d+-0x2939][_0x55fd73(-0x21b,0xd3,0x212,-0x164)],console['log'](teks),_0x3025fc['iKHaG'](sendStickerFromUrl,from,''+teks);function _0x52c883(_0x36b8c6,_0x51d4d9,_0x3830ab,_0x1d3315){return _0x3f34aa(_0x36b8c6-0x109,_0x1d3315,_0x3830ab- -0x1ba,_0x1d3315-0xc3);}function _0x55fd73(_0x27cd0e,_0x5b064d,_0x18047b,_0x4cb01a){return _0x1da9f3(_0x5b064d- -0x3da,_0x18047b,_0x18047b-0xc6,_0x4cb01a-0x104);}await _0x3025fc[_0x52c883(0xc58,0x58c,0x8d6,0x3b8)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc==_0x3f34aa(0x50e,0x4ec,0x96c,0x495)+'sApp-'+q5){const _0xb7fa4d={};_0xb7fa4d[_0x3f34aa(0xbb6,0x4cb,0x9c5,0x891)+'t']='Check\x20Limi'+'t';const _0x1b1bcd={};_0x1b1bcd['buttonId']='limit',_0x1b1bcd['buttonText']=_0xb7fa4d,_0x1b1bcd[_0x1da9f3(0x847,0x819,0x76a,0xa1a)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x955,0x524,0xbc0)](prefix),'©\x20'+ownername,[_0x1b1bcd]);reply(lang[_0x1da9f3(0x723,0xb08,0x7ee,0x237)]()),emoji[_0x1da9f3(0x4cf,0x67d,0x45e,0x142)](''+q5)[_0x3f34aa(-0x158,0x3b5,0x2c,-0x4d8)](async _0x50f448=>{const _0x18b467={'atjfJ':function(_0xea9d5c,_0x14d76e,_0x776ff0){return _0xea9d5c(_0x14d76e,_0x776ff0);}};teks=''+_0x50f448[_0x79c3dd(0x386,0x58a,0x769,0x2df)][-0x2588+-0x1*-0x1896+-0x1da*-0x7][_0x86b040(0x766,0xbe1,0x7ac,0x430)],console[_0x86b040(0x98b,0xdb0,0x7a6,0x827)](teks);function _0x86b040(_0x84c440,_0x2ba492,_0x54b9aa,_0x476b64){return _0x3f34aa(_0x84c440-0x67,_0x54b9aa,_0x84c440-0x46d,_0x476b64-0x40);}function _0x79c3dd(_0x3fcdf6,_0x5af8c8,_0x428846,_0x1e32d2){return _0x3f34aa(_0x3fcdf6-0x5c,_0x1e32d2,_0x5af8c8-0x3c3,_0x1e32d2-0x1ab);}_0x18b467['atjfJ'](sendStickerFromUrl,from,''+teks),await limitAdd(sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0xa60,0x88f,0xd7e,0x7e5)+'ter-'+q5){const _0x1803f7={};_0x1803f7[_0x1da9f3(0xb79,0xa25,0xab5,0xa17)+'t']=_0x3f34aa(0x529,0x773,0x4e1,0x265)+'t';const _0x435dd4={};_0x435dd4[_0x3f34aa(0x357,0x6f0,0x30f,0x2cc)]=_0x3f34aa(0xb31,0xb74,0x6d2,0x4c0),_0x435dd4[_0x3f34aa(0x867,0x643,0x6ef,0x45e)]=_0x1803f7,_0x435dd4[_0x1da9f3(0x847,0xc2e,0x554,0xab0)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x3fd,0x8db,0x77b)](prefix),'©\x20'+ownername,[_0x435dd4]);emoji[_0x3f34aa(0x75f,-0x214,0x31b,0x261)](''+q5)[_0x1da9f3(0x1e0,0x1a4,-0x2eb,0x34)](async _0x4b95ed=>{const _0x336983={'EKavD':function(_0x44ef64,_0x4a98bb,_0x2b3619){return _0x44ef64(_0x4a98bb,_0x2b3619);},'hCDmq':function(_0x22d569,_0x44aaac,_0x2bcad7){return _0x22d569(_0x44aaac,_0x2bcad7);}};teks=''+_0x4b95ed[_0x1bec7f(0x1d3,0xa76,0x688,0x637)][0x8*-0x1a9+0x2471+-0x1724][_0x1bec7f(0x287,0xc5a,0x7ba,0xa5b)],console[_0x203b11(0x8c3,0x5c6,0x687,0x872)](teks),_0x336983['EKavD'](sendStickerFromUrl,from,''+teks);function _0x1bec7f(_0x38c0b4,_0xf4b19c,_0x483896,_0x10849f){return _0x1da9f3(_0x483896-0x30d,_0x10849f,_0x483896-0xbb,_0x10849f-0xde);}function _0x203b11(_0x15cad1,_0x59e366,_0x54c283,_0x27b2fd){return _0x1da9f3(_0x15cad1-0x1f1,_0x54c283,_0x54c283-0xdf,_0x27b2fd-0x120);}await _0x336983[_0x1bec7f(0x9ea,0x4f7,0x83b,0x539)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0x7fe,0xa64,0x8a4,0x2a3)+_0x1da9f3(0x202,-0x14f,0x50b,0x18b)+q5){const _0x36242e={};_0x36242e['displayTex'+'t']=_0x1da9f3(0x695,0xa8f,0x47e,0x3cd)+'t';const _0x34ff10={};_0x34ff10[_0x3f34aa(0x659,0x5b1,0x30f,-0x143)]=_0x1da9f3(0x886,0x782,0xc72,0x93b),_0x34ff10['buttonText']=_0x36242e,_0x34ff10['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x1f7,0x2b5,0x6d2,0x5fb)](prefix),'©\x20'+ownername,[_0x34ff10]);reply(lang[_0x1da9f3(0x723,0x60d,0x296,0xb4a)]()),emoji[_0x3f34aa(0x247,0x2e7,0x31b,0x477)](''+q5)[_0x3f34aa(0x338,-0xb,0x2c,0x1f4)](async _0x5f2585=>{const _0x4fbdbc={'LZtsF':function(_0x2ce74a,_0x5e7401,_0x252ac5){return _0x2ce74a(_0x5e7401,_0x252ac5);}};teks=''+_0x5f2585[_0x1efe8c(0x4f2,0x87c,0x7a9,0x4b8)][0x1669+-0xa7*-0x34+-0x384f]['url'],console[_0x1efe8c(0x31c,0xbdd,0x5e6,0x80f)](teks);function _0x14aaab(_0x179065,_0x4bb268,_0x100e52,_0x3b20f5){return _0x3f34aa(_0x179065-0x69,_0x3b20f5,_0x4bb268-0x492,_0x3b20f5-0x1b5);}sendStickerFromUrl(from,''+teks);function _0x1efe8c(_0x41872f,_0x490dd0,_0x1755a8,_0x3f991f){return _0x1da9f3(_0x3f991f-0x13d,_0x41872f,_0x1755a8-0x5,_0x3f991f-0xe5);}await _0x4fbdbc[_0x1efe8c(0x5a,0x539,0x162,0x344)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0xb87,0x665,0xb71,0x98e)+_0x3f34aa(0xe8c,0xa8c,0x987,0x812)+q5){const _0x467365={};_0x467365['displayTex'+'t']=_0x1da9f3(0x695,0x911,0x640,0x9a7)+'t';const _0x2309a8={};_0x2309a8['buttonId']=_0x3f34aa(0xae2,0x614,0x6d2,0x569),_0x2309a8[_0x3f34aa(0x2d9,0x7f5,0x6ef,0xa0b)]=_0x467365,_0x2309a8['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x988,0x5e4,0x81f)](prefix),'©\x20'+ownername,[_0x2309a8]);emoji[_0x1da9f3(0x4cf,0x187,0x1a7,0x85b)](''+q5)[_0x1da9f3(0x1e0,-0x6b,0x728,-0x101)](async _0x4a71a2=>{const _0x25374c={'LoKPR':function(_0x41dd1b,_0x4e7dd7,_0x2d1c6b){return _0x41dd1b(_0x4e7dd7,_0x2d1c6b);},'BvybR':function(_0x5d5dab,_0x532ad5,_0x133067){return _0x5d5dab(_0x532ad5,_0x133067);}};function _0x65533c(_0x395783,_0x559a5d,_0x71367b,_0x3da3c7){return _0x1da9f3(_0x71367b-0x233,_0x3da3c7,_0x71367b-0x1d3,_0x3da3c7-0x1a1);}teks=''+_0x4a71a2[_0x65533c(0x4e2,0x6f6,0x5ae,0x47b)][-0x781*-0x2+-0x3d*-0x2+0xf75*-0x1][_0x233a9d(0x844,0x85d,0x53c,0xaa4)];function _0x233a9d(_0x4f80b3,_0x44b0b4,_0x100108,_0x1d963b){return _0x3f34aa(_0x4f80b3-0xcd,_0x1d963b,_0x4f80b3-0x54b,_0x1d963b-0x5f);}console[_0x65533c(0x424,0xdd4,0x905,0xaea)](teks),_0x25374c['LoKPR'](sendStickerFromUrl,from,''+teks),await _0x25374c[_0x233a9d(0x68e,0x22c,0x299,0x2a2)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0x326,0x1,0x91,0x820)+'Moji-'+q5){const _0x2d77aa={};_0x2d77aa[_0x3f34aa(0xa97,0x474,0x9c5,0xdac)+'t']=_0x1da9f3(0x695,0x3dd,0x246,0x4a3)+'t';const _0x37b4f3={};_0x37b4f3['buttonId']=_0x1da9f3(0x886,0x357,0xb3b,0xc1c),_0x37b4f3['buttonText']=_0x2d77aa,_0x37b4f3[_0x1da9f3(0x847,0x79a,0x6c5,0x6fc)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x6e8,0xa86,0x6d2,0x543)](prefix),'©\x20'+ownername,[_0x37b4f3]);emoji[_0x3f34aa(0x6e4,-0x19d,0x31b,0x83e)](''+q5)['then'](async _0x25d86b=>{const _0x5b7102={'JqFfJ':function(_0x56e98b,_0x19985a,_0x131278){return _0x56e98b(_0x19985a,_0x131278);}};function _0x2bc38b(_0x529d6d,_0xd52ad7,_0x3c9419,_0x2b8c5f){return _0x3f34aa(_0x529d6d-0x83,_0x2b8c5f,_0x3c9419-0x2e2,_0x2b8c5f-0xd);}function _0xd3db02(_0xb4c48b,_0x2ec824,_0x207f56,_0x41c459){return _0x3f34aa(_0xb4c48b-0x3c,_0x207f56,_0x41c459-0x565,_0x41c459-0x159);}teks=''+_0x25d86b[_0xd3db02(0xc20,0x421,0x946,0x72c)][-0x187d*0x1+0x1*0x1ea+0x169b][_0xd3db02(0x3e3,0x3c5,0x8ac,0x85e)],console[_0xd3db02(0xb02,0xbb5,0x8be,0xa83)](teks),_0x5b7102[_0x2bc38b(0xb6e,0xce8,0xd76,0xe2e)](sendStickerFromUrl,from,''+teks),await _0x5b7102['JqFfJ'](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc=='Emoji-emoj'+_0x3f34aa(0x7de,0x9bb,0x6f5,0x671)+q5){const _0x145a37={};_0x145a37[_0x3f34aa(0x68d,0xa8a,0x9c5,0xb28)+'t']=_0x3f34aa(0x663,0x996,0x4e1,0x9ef)+'t';const _0x585f0b={};_0x585f0b[_0x3f34aa(0x2bd,0x524,0x30f,-0x16e)]=_0x3f34aa(0x6dd,0xad4,0x6d2,0xc0d),_0x585f0b[_0x3f34aa(0x870,0x624,0x6ef,0xa71)]=_0x145a37,_0x585f0b['type']=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x585f0b]);reply(lang['wait']()),emoji[_0x3f34aa(0x73,0x6d,0x31b,0x315)](''+q5)['then'](async _0x5c8965=>{const _0x51f33b={'doRGe':function(_0x5d2a55,_0x33e2cb,_0x21c0e9){return _0x5d2a55(_0x33e2cb,_0x21c0e9);}};teks=''+_0x5c8965[_0x50b2fd(0xab,0x514,0xe8,0x208)][0x1*0x436+0x2521+0x26e*-0x11][_0x50b2fd(0x1cf,0x646,0xb69,0x3b2)];function _0x50b2fd(_0x376b15,_0x465e2a,_0x515bf8,_0x5b2917){return _0x1da9f3(_0x465e2a-0x199,_0x5b2917,_0x515bf8-0x1c7,_0x5b2917-0x162);}console[_0x566da8(0x391,0x6b2,0x87a,0xa95)](teks),_0x51f33b[_0x50b2fd(0x68a,0x617,0x246,0x854)](sendStickerFromUrl,from,''+teks);function _0x566da8(_0x562226,_0x3cafd3,_0x49fdfb,_0x1bc9de){return _0x3f34aa(_0x562226-0xff,_0x3cafd3,_0x49fdfb-0x35c,_0x1bc9de-0x117);}await limitAdd(sender,limit);});}if(sub_yt_zeeone_ofc==_0x3f34aa(0x62b,0x638,0xa3c,0x564)+'enger-'+q5){const _0x3ad32a={};_0x3ad32a[_0x3f34aa(0x478,0xa81,0x9c5,0x927)+'t']=_0x3f34aa(0x39c,0x31b,0x4e1,0x23e)+'t';const _0x19aa51={};_0x19aa51[_0x1da9f3(0x4c3,0x52a,0x3c4,0x298)]=_0x1da9f3(0x886,0xa24,0xa3e,0x3b5),_0x19aa51['buttonText']=_0x3ad32a,_0x19aa51[_0x1da9f3(0x847,0xbfd,0x388,0xa26)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang['limit'](prefix),'©\x20'+ownername,[_0x19aa51]);emoji[_0x1da9f3(0x4cf,0x211,0x3a8,0x899)](''+q5)['then'](async _0x4ac9df=>{function _0x321319(_0x34a980,_0x4d902c,_0x3284e1,_0x3dbe57){return _0x1da9f3(_0x34a980- -0x27,_0x3dbe57,_0x3284e1-0x17f,_0x3dbe57-0x72);}teks=''+_0x4ac9df[_0x321319(0x354,0x427,0x84a,0x55a)][-0xea2+0xf25+-0x79]['url'],console[_0x321319(0x6ab,0x2c9,0x3f2,0x65d)](teks);function _0x28f476(_0x286fd6,_0x5a0960,_0x4ae8d8,_0x7fdc16){return _0x3f34aa(_0x286fd6-0x164,_0x286fd6,_0x4ae8d8-0x21e,_0x7fdc16-0x14f);}sendStickerFromUrl(from,''+teks),await limitAdd(sender,limit);});}if(sub_yt_zeeone_ofc==_0x1da9f3(0x2ff,0x454,0x7df,0x5f5)+q5){const _0x3fc808={};_0x3fc808['displayTex'+'t']=_0x1da9f3(0x695,0x29e,0xae0,0x1e8)+'t';const _0x1e208b={};_0x1e208b[_0x3f34aa(0x806,0x215,0x30f,0x4fb)]=_0x1da9f3(0x886,0x7d5,0xb63,0x73e),_0x1e208b['buttonText']=_0x3fc808,_0x1e208b[_0x3f34aa(0x397,0x43c,0x693,0xbcd)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x3f34aa(0x1d9,0x7bf,0x6d2,0x1d8)](prefix),'©\x20'+ownername,[_0x1e208b]);reply(lang[_0x3f34aa(0x27a,0x9f4,0x56f,0x3da)]()),emoji[_0x3f34aa(-0x28,0x1a,0x31b,-0x181)](''+q5)[_0x3f34aa(-0x1df,-0x4c5,0x2c,0x312)](async _0x5c1da5=>{function _0x4b4300(_0x51b6f9,_0x3eef02,_0x6ae1e3,_0x31a8fc){return _0x1da9f3(_0x31a8fc- -0x392,_0x51b6f9,_0x6ae1e3-0x19f,_0x31a8fc-0x5e);}const _0xddec58={'KFOtc':function(_0x43aac2,_0x21dc4d,_0x367f90){return _0x43aac2(_0x21dc4d,_0x367f90);}};function _0x30ee46(_0x4dbb10,_0x55adb9,_0x5707ec,_0xd6b639){return _0x3f34aa(_0x4dbb10-0x1e5,_0x5707ec,_0x55adb9- -0x226,_0xd6b639-0x171);}teks=''+_0x5c1da5[_0x30ee46(-0x2,-0x5f,0x33,-0x124)][0x4b3+-0x1*0x260b+0x21*0x103][_0x30ee46(0x278,0xd3,0xdf,-0x1b5)],console[_0x4b4300(0x757,0x5d4,0x775,0x340)](teks),_0xddec58[_0x4b4300(-0x4d7,0x2ad,-0x3a6,-0x1db)](sendStickerFromUrl,from,''+teks),await _0xddec58[_0x4b4300(-0x63f,-0x9f,-0x46,-0x1db)](limitAdd,sender,limit);});}if(sub_yt_zeeone_ofc=='Emoji-HTC-'+q5){const _0x35e70f={};_0x35e70f[_0x3f34aa(0x863,0xcaa,0x9c5,0x897)+'t']=_0x1da9f3(0x695,0xb9c,0x9bb,0x570)+'t';const _0x9c9d0c={};_0x9c9d0c[_0x1da9f3(0x4c3,0x448,0x5f1,0x125)]='limit',_0x9c9d0c['buttonText']=_0x35e70f,_0x9c9d0c[_0x3f34aa(0x4ec,0x15d,0x693,0x42e)]=0x1;if(isLimit(sender,isPremium,isCreator,isOwner,limitawal,limit))return sendButMessage(from,lang[_0x1da9f3(0x886,0x91c,0x425,0x43b)](prefix),'©\x20'+ownername,[_0x9c9d0c]);emoji['get'](''+q5)[_0x3f34aa(-0x456,-0x26f,0x2c,-0x3a0)](async _0x12b495=>{function _0x40ac42(_0x4b49e5,_0x6fc74d,_0xd0968e,_0x1c82b4){return _0x1da9f3(_0xd0968e-0x210,_0x6fc74d,_0xd0968e-0x183,_0x1c82b4-0x138);}const _0x369fc2={'vMGKB':function(_0x1b711b,_0x4e345d,_0x23c875){return _0x1b711b(_0x4e345d,_0x23c875);}};teks=''+_0x12b495['images'][-0x60*0x38+-0x1391+0x289d]['url'],console[_0x40ac42(0xd62,0x733,0x8e2,0x4fd)](teks);function _0x296816(_0x28e9cd,_0x5f3a4b,_0x327965,_0x432087){return _0x1da9f3(_0x28e9cd- -0x37f,_0x432087,_0x327965-0x114,_0x432087-0x1b1);}_0x369fc2[_0x296816(0xf4,0x80,0x227,-0x275)](sendStickerFromUrl,from,''+teks),await limitAdd(sender,limit);});}
		switch (command) {
case 'dompet':
					const kantong = checkATMuser(sender)
					reply(` *「 ATM USER 」* \n📛 *Nama* : ${pushname}\n🆔 *Nomer* : ${sender.split("@")[0]}\n💰 *Uang* : ${kantong}\n`)
					break
	case 'transfer':
				if (!q.includes('|')) return  reply('format salah')
                			const tujuan = q.substring(0, q.indexOf('|') - 1)
                			const jumblah = q.substring(q.lastIndexOf('|') + 1)
                			if(isNaN(jumblah)) return await reply('jumlah harus berupa angka!!')
                			if (jumblah < 100 ) return reply(`Minimal Transfer 100`)
                			if (checkATMuser(sender) < jumblah) return reply(`uang mu tidak mencukupi untuk melakukan transfer`)
                			const tujuantf = `${tujuan.replace("@", '')}`
               				fee = 0.005 *  jumblah
                			hasiltf = jumblah + fee
                			addKoinUser(`${tujuantf}@s.whatsapp.net`, hasiltf)
                			confirmATM(sender, hasiltf)
                			reply(`*「 SUKSES  」*\n\npengiriman uang telah sukses\ndari : +${sender.split("@")[0]}\nke : +${tujuan}\njumblah transfer : ${jumblah}\npajak : 30*jumblah`)
                			break
	case 'limit': case 'ceklimit': case 'balance': case 'glimit':
reply(`💳 Limit : ${isPremium ? 'Unlimited Premium' : `${getLimit(sender, limitawal, limit)} / ${limitawal}`}
🏧 Limit Game : ${cekGLimit(sender, gcount, glimit)} / ${gcount}
🏦 Balance : $${getBalance(sender, balance)}


Kamu Dapat Membeli Limit Dengan ${prefix}Buylimit *Nominal* Dan ${prefix}Buyglimit *Nominal* Untuk Membeli Game Limit

*Example :*
${prefix}buylimit 10
${prefix}buyglimit 10

NOTE : 
- Harga Limit Perlimit adalah $100 balance`)
break
case 'buylimit':{
if (!q) return reply(`Kirim perintah *${prefix}buylimit* jumlah limit yang ingin dibeli\n\nHarga 1 limit = $100 balance`)
if (q.includes('-')) return reply(`Jangan menggunakan -`)
if (isNaN(q)) return reply(`Harus berupa angka`)
let ane = Number(nebal(q) * 100)
if (getBalance(sender, balance) < ane) return reply(`Balance kamu tidak mencukupi untuk pembelian ini`)
kurangBalance(sender, ane, balance)
giveLimit(sender, nebal(q), limit)
reply(`Pembeliaan limit sebanyak ${q} berhasil

*🏧 Sisa Balance : $${getBalance(sender, balance)}*
*🏦 Sisa Limit : ${getLimit(sender, limitawal, limit)} / ${limitawal}*`)
}
break
case 'buyglimit':{
if (!q)return reply(`Example : ${prefix + command} 10\n\nHarga 1 limit = $100 balance`)
if (q.includes('-')) return reply(`Jangan menggunakan -`)
if (isNaN(q)) return reply(`Harus berupa angka`)
const koinPerlimit = 100
const total = koinPerlimit * q
if (getBalance(sender,balance) <= total) return reply(`maaf Balance kamu belum mencukupi. silahkan kumpulkan dan beli nanti`)
kurangBalance(sender, total, balance)
givegame(sender, q, glimit)
reply(`Pembeliaan game limit sebanyak ${q} berhasil

*💳 Sisa Balance : $${getBalance(sender, balance)}*
*💷 Sisa Game Limit : ${cekGLimit(sender, gcount, glimit)} / ${gcount}*`)
}
break
	case 'me': case 'myinfo': case 'info': case 'profile': case 'profil':{
let cek = ms( await premium.getPremiumExpired(sender, premium) - Date.now())
let userProcfile = `「 *USER INFO* 」

📛 Nama : ${pushname}
💋 Bio : ${bio_user}
🔗 Tag : @${sender.split("@")[0]}
💥 Api : wa.me/${sender.split("@")[0]}

💹 Limit : ${isPremium ? 'Unlimited Premium' : `${getLimit(sender, limitawal, limit)} / ${limitawal}`}
💳 Game Limit : ${cekGLimit(sender, gcount, glimit)} / ${gcount}
💷 Balance : $${getBalance(sender, balance)}
👛 Dompet : ${checkATMuser(sender)}
💱 Role : ${role}
🏧 Level : ${getLevelingLevel(sender)}
🏦 Xp : ${getLevelingXp(sender)}

💌 Status : ${isPremium? `Premium User` : `Free user`}
⏰ Expired Prem : ${isPremium ? 'Unlimited Premium' : ` ${cek.days} d, ${cek.hours} h, ${cek.minutes} m, ${cek.seconds} s`}
👨‍ Register : ${isRegister? `Done`:`Belum Daftar`}
🚫 Baned : ${isBanned?`True`:`False`}`
let papakpo = [{
										"buttonId": `inv`,
										"buttonText": {
											"displayText": "INVENTORY"
											},
										"type": "RESPONSE"
										},{
										"buttonId": `sewabot`,
										"buttonText": {
											"displayText": "SEWA"
											},
										"type": "RESPONSE"
										}]
								sendButLocation(from, userProcfile , `NOTE ！\nJika whatsapp mod kamu belum support button silahkan tonton video ini https://youtu.be/ERGID4Fmo9w\n\n© ${ownername}`,pp_userz, papakpo, {contextInfo: { mentionedJid: [sender]}})
}
break
	case 'verify': case 'daftar':
 if (isRegister) return reply('Kamu sudah terdaftar di dalam database')
 addRegisterUser(sender, pushname, bio_user, wib)
 let ran_blc = randomNomor(50)
 addBalance(sender, ran_blc, balance)
fs.writeFileSync('./database/user/register.json', JSON.stringify(register))
teks = `╭─❒ *Verification*\n│ *Nama :* ${pushname}\n│ *Nomor :* @${sender.split('@')[0]}\n│ *Bio :* ${bio_user}\n│ *Time :* ${wib}\n╰❒ *Success*`
let papako = [{
										"buttonId": `menu`,
										"buttonText": {
											"displayText": "MENU"
											},
										"type": "RESPONSE"
										},{
										"buttonId": `me`,
										"buttonText": {
											"displayText": "PROCFILE"
											},
										"type": "RESPONSE"
										}]
								sendButLocation(from, teks , `Thank for verification\n© ${ownername}`,pp_userz, papako, {contextInfo: { mentionedJid: [sender]}})
                break
	case 'menu': case 'help':
			try {
				chatt = await alpha.getProfilePicture(sender)
				} catch {
				chatt = 'https://l.top4top.io/p_20670hd6v1.jpg'
				}
			let ch = await getBuffer(chatt)
			try{
			hit_total = await fetchJson('https://api.countapi.xyz/hit/api-alphabot.herokuapp.com/visits')
			} catch {
				hit_total = { 
					value : "-"
					}
				}
				hitall = `${hit_total.value}`
koko = `${targetpc}@s.whatsapp.net`
let content = fs.readFileSync(`image/${thumbnail}`)
const media = await alpha.prepareMessage(from, content, MessageType.image, { thumbnail:fs.readFileSync(`image/${thumbnail}`)})
let bacotlu = media.message["ephemeralMessage"] ? media.message.ephemeralMessage : media
let p1 = await alpha.getStatus(sender)
anunya = process.uptime()
ini_anu =`${ucapannya2}

╭─❒ 「 Bot Info 」 
├ Creator :  @${koko.split('@')[0]}
├ Powered  : @${ini_mark.split('@')[0]}
├ Prefix :   ${prefix}
├ Total hit : ${hitall}
├ Hit today : ${hit_today.length}
├ Speed : ${latensii.toFixed(4)} Second
├ Hostname : ${os.hostname()}
├ Platform : ${os.platform()}
├ Runtime : ${(kyun(os.uptime()))}
├ Battery : ${isBattre}
╰❒ Charging : ${isCharge}

╭─❒ 「 User Info 」 
├ Name : ${pushname}
├ Bio : ${p1 ? `${p1.status}` : '-'}
├ Nomor : @${sender.split('@')[0]}
├ Me : ${mek.key.fromMe ? 'True' : 'False'}
╰❒ Owner : ${isOwner ? 'True' : `False`}
`
if(typemenu == 'document'){
sendButDoc(from, ini_anu, `Please Don't spam bot, pause 3 seconds per command!\n`, sender, koko, ini_mark)
} 
if(typemenu == 'troli'){
sendTroli(allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii))
} 
if(typemenu == 'troli2'){
sendTroli2(allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii))
} 
if(typemenu == 'katalog'){
sendKatalog2(allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii))
} 
if(typemenu == 'katalog2'){
sendKatalog3(allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii))
} 
if(typemenu == 'list'){
sendList(sender)
} 
if(typemenu == 'location'){ 
let content1 = fs.readFileSync(`image/${thumbnail}`)
const media1 = await alpha.prepareMessage(from, content1, MessageType.location, {thumbnail: content1})
let bacotlu1 = media1.message["ephemeralMessage"] ? media1.message.ephemeralMessage : media1

const buttons1 = [
  {buttonId: 'owner', buttonText: {displayText: '⋮☰ OWNER'}, type: 1},
  {buttonId: 'botstat', buttonText:{displayText: '✓ STATISTIC'}, type: 1},
  {buttonId: 'Command', buttonText: {displayText: '❍ LIST MESSAGE'}, type: 1}
]

const btn1 = {
    contentText: allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii),
    footerText: `${tampilTanggal}${enter}${tampilWaktu}${enter}${enter}Regard @${koko.split('@')[0]}`,
    buttons: buttons1,
    headerType: 6,
    locationMessage: bacotlu1.message.locationMessage
}

alpha.sendMessage(from,  btn1, MessageType.buttonsMessage,{
        caption: 'Botwea ©2k21',
        "contextInfo": {
            text: 'hi',
            "forwardingScore": 1000000000,
            isForwarded: true,
            sendEphemeral: true,
            "mentionedJid" : [sender,koko,ini_mark],
            },
			quoted: fkontak,sendEphemeral: true 
			})
}

break
case 'setmenu':
if (!isOwner && !isCreator && !mek.key.fromMe) return reply(lang.onlyOwner())
const listhades = ['document', 'troli', 'troli2', 'katalog', 'katalog2', 'list', 'location']
listMsg = {
 buttonText: 'SET MENU',
 footerText: `© ${ownername}`,
 description: `Pilih tampilan menu sesukamu`,
 sections: [
                     {
                      "title": `SET MENU`,
 rows: [
                          {
                              "title": "document",
                              "rowId": "setmenu document"
                           },
                           {
                              "title": "troli",
                              "rowId": "setmenu troli"
                           },
                           {
                              "title": "troli2",
                              "rowId": "setmenu troli2"
                           },
                           {
                              "title": "katalog",
                              "rowId": "setmenu katalog"
                           },
                           {
                              "title": "katalog2",
                              "rowId": "setmenu katalog2"
                           },
                           {
                              "title": "list",
                              "rowId": "setmenu list"
                           },
                           {
                              "title": "location",
                              "rowId": "setmenu location"
                           }
                        ]
                     }],
 listType: 1
}
if (!listhades.includes(q)) return alpha.sendMessage(from, listMsg, MessageType.listMessage, {quoted: mek})
//reply(`*Example :*${enter}•${prefix + command} location\n•${prefix + command} document\n•${prefix + command} list\n•${prefix + command} troli\n•${prefix + command} troli2\n•${prefix + command} katalog\n•${prefix + command} katalog2\n`)
typemenu = q
reply(lang.success())
break
case 'setlang':
         if (!isOwner && !isCreator && !mek.key.fromMe) return reply(lang.onlyOwner())
if(args[0] == 'ind'){
lang = ind
reply('Sukses mengubah bahasa ke ind')
}else if(args[0] == 'eng'){
lang = eng
reply('Success changing language to eng')
}else if(args[0] == 'es'){
lang = es
reply('Éxito cambiando el idioma a es')
}else if(args[0] == 'ml'){
lang = ml
reply('ഭാഷയിലേക്ക് മാറ്റുന്നതിൽ വിജയം ml')
}else if(args[0] == 'pt'){
lang = pt
reply('Sucesso ao alterar o idioma para pt')
}else if(args[0] == 'ru'){
lang = ru
reply('Успешно сменил язык на ru')
}else {
reply(`Example : ${prefix + command} eng\n\nAvailable\n•ind - Indonesia\n•eng - English\n•es - Spanish\n•ml - Malayalam\n•pt - Portugis\n•ru - Russian`)
}
break
                case 'allmenu':
                let papao = [{
										"buttonId": `owner`,
										"buttonText": {
											"displayText": "OWNER"
											},
										"type": "RESPONSE"
										},{
										"buttonId": `sewabot`,
										"buttonText": {
											"displayText": "SEWA"
											},
										"type": "RESPONSE"
										},{
										"buttonId": 'Command',
										"buttonText": {
											"displayText": "LIST MESSAGE"
											},
										"type": "RESPONSE"
										}]
								sendButLocation(from, allmenu(kyun, os, prefix, wita, wit, ucapannya2, timuu, status, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, alfa , alfa1, thisDay, ini_tanggal, totalchat, hit_today, ini_gcchat, latensii), `NOTE ！\nJika whatsapp mod kamu belum support button silahkan tonton video ini https://youtu.be/ERGID4Fmo9w\n\n© ${ownername}`,pp_userz, papao, {})
                break
        case 'trigger':
					reply ('Mungkin yg kamu maksud .triggered')
					await limitAdd(sender, limit)
					break
					case 'sampah':
					if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					var imgbb = require('imgbb-uploader')
	                 if ((isMedia && !mek.message.videoMessage || isQuotedImage) && args.length == 0) {
	                 ger = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek 
					reply(lang.wait())
					console.log(color(time, 'magenta'), color('Downloading sticker...'))
					media = await alpha.downloadMediaMessage(ger)
					njay = await uploadImages(media)
                    titid = await fetchJson(`https://nekobot.xyz/api/imagegen?type=trash&url=${njay}`, {method: 'get'})
                    buffer = await getBuffer(titid.message)
					alpha.sendMessage(from, buffer, image, {caption : lang.success(),quoted: mek})
                   }
                   await limitAdd(sender, limit)
              break       
		case 'triggered':case 'gay': case 'glass': case 'passed': case 'jail': case 'comrade':case 'green': case 'blue': case 'sepia': case 'wasted': case 'greyscale': case 'blurple2': case 'blurple': case 'red': case 'invertgreyscale': case 'invert':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					var imgbb = require('imgbb-uploader')
					if ((isMedia && !mek.message.videoMessage || isQuotedImage) && args.length == 0) {
					ger = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek 
					reply(lang.wait())
					owgi = await alpha.downloadMediaMessage(ger)
				    anu = await uploadImages(owgi)
					ranp = getRandom('.gif')
					rano = getRandom('.webp')
					anu4 = `https://some-random-api.ml/canvas/${command}?avatar=${anu}`
					exec(`wget ${anu4} -O ${ranp} && ffmpeg -i ${ranp} -vcodec libwebp -filter:v fps=fps=20 -lossless 1 -loop 0 -preset default -an -vsync 0 -s 512:512 ${rano}`, (err) => {
					fs.unlinkSync(ranp)
					if (err) return reply(lang.tryAgain())
					alpha.sendMessage(from, fs.readFileSync(rano), sticker, {quoted: mek})
					fs.unlinkSync(rano)
					})
					} else {
					reply('Gunakan foto!')
					}
					await limitAdd(sender, limit)
					break 
					case 'jadian':
					if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
jds = []
jdii = groupMembers
koss = groupMembers
akuu = jdii[Math.floor(Math.random() * jdii.length)]
diaa = koss[Math.floor(Math.random() * koss.length)]
teks = `Ciee.. yang lagi jadian @${akuu.jid.split('@')[0]}  (♥️ ) @${diaa.jid.split('@')[0]} `
jds.push(akuu.jid)
jds.push(diaa.jid)
mentions(teks, jds, true)
await limitAdd(sender, limit)
break
   case 'group': 
   case 'gc': 
                if (!isGroup) return reply(lang.onlygc());
        if (!isGroupAdmins && !isBotGroupAdmins) return reply(lang.onlygcAdmin());
        if (args[0] == "open") {
          await alpha.groupSettingChange(from, GroupSettingChange.messageSend, false)
					fakegroup('S U C C E S S  O P E N I N G  G R O U P')
        } else if (args[0] == "close") {
          await alpha.groupSettingChange(from, GroupSettingChange.messageSend, true)
					fakegroup('S U C C E S S  C L O S I N G  G R O U P')
        } else if (!q) {
        	var ini_gopayy =`Halo @${sender.split("@")[0]} Gunakan ${prefix + command } Open / Close jika button tidak merespon`
var buttonss = [
{buttonId: 'group open', buttonText:{displayText: 'Open'}, type: 1},
{buttonId: 'group close', buttonText:{displayText: 'Close'}, type: 1}
]

buttonMessagee = {
contentText: ini_gopayy,
footerText: `${tampilTanggal}\n${tampilWaktu}\n\n© ${creator}` ,
buttons: buttonss,
headerType: 1
}
alpha.sendMessage(from,  buttonMessagee, MessageType.buttonsMessage,{
        caption: 'Botwea ©2k21',
        "contextInfo": {
            text: 'hi',
            "forwardingScore": 1000000000,
            isForwarded: true,
            sendEphemeral: true,
            "mentionedJid" : [sender]
            },
			quoted: ftroli,sendEphemeral: true 
			})
        }
        break
            case 'mystat': 
            case 'botstat': 
            case 'botstatus': 
			case 'mystatus':
				anu = process.uptime()
                teskny = `B O T  S T A T I S T I C\n`
				teskny +=`\`\`\`Group Chat : ${giid.length}\`\`\`\n`
				teskny +=`\`\`\`Personal Chat : ${totalchat.length - giid.length}\`\`\`\n`
				teskny +=`\`\`\`Total Chat : ${totalchat.length}\`\`\`\n`
				teskny +=`\`\`\`Speed :\`\`\` ${latensii.toFixed(4)} _Second_\n`
				teskny +=`\`\`\`Runtime : ${(kyun(os.uptime()))}\`\`\`\n\n` 
				teskny +=`P H O N E  S T A T I S T I C\n`
				teskny +=`\`\`\`Wa Whatsapp : ${wa_version}\`\`\`\n`
				teskny +=`\`\`\`RAM : ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)}MB / ${Math.round(require('os').totalmem / 1024 / 1024)}MB\`\`\`\n`
				teskny +=`\`\`\`MCC : ${mcc}\`\`\`\n`
				teskny +=`\`\`\`MNC : ${mnc}\`\`\`\n`
				teskny +=`\`\`\`OS Version : ${os_version}\`\`\`\n`
				teskny +=`\`\`\`Merk Hp : ${device_manufacturer}\`\`\`\n`
				teskny +=`\`\`\`Versi Hp : ${device_model}\`\`\`\n`
				teskny +=`\`\`\`Runtime : ${(kyun(os.uptime()))}\`\`\``
				alpha.sendMessage(from, teskny, text, {quoted: { key : { participant : `0@s.whatsapp.net`, "remoteJid":  '6283136505591-1614953337@g.us', "fromMe": false, "id": "B391837A58338BA8186C47E51FFDFD4A" }, "message": { "documentMessage": { "jpegThumbnail": fs.readFileSync(`image/${thumbnail}`), "mimetype": "application/octet-stream","title": `${setting.fake}`, "fileLength": "36", "pageCount": 0, "fileName": `${setting.fake}`}}, "messageTimestamp": "1614069378", "status": "PENDING"},contextInfo:{"forwardingScore":999,"isForwarded":true},sendEphemeral: true})
break
case 'getbio':
				if (!isGroup) return reply(lang.onlygc())
				if (args.length < 1) return reply('```TAG ORANGNYA```')
                mentionedd = mek.message.extendedTextMessage.contextInfo.mentionedJid[0]
                const p = await alpha.getStatus(`${mentionedd}`, MessageType.text)
                reply(p.status)
                if (p.status == 401) {
                reply("Error! mungkin diprivate")
                }
                await limitAdd(sender, limit)
                break
/*case 'creategroup':
case 'creategrup':
			    if (!isGroup) return reply(lang.onlygc())
				if (args.length < 1) return reply(`Penggunaan ${prefix}creategrup nama grup|@tag member`)
				argza = arg.split('|')
				if (mek.message.extendedTextMessage != undefined){
                    mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
                    for (let i = 0; i < mentioned.length; i++){
						anu = []
						anu.push(mentioned[i])
                    }
					alpha.groupCreate(argza[0], anu)
					reply(`Sukes membuat grup:\n${argza}`)
                }
				break*/
		case 'caripesan':
		    case 'searchmessage':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
				if (!q) return reply(`Penggunaan ${command} _nama pesannya_\n\nContoh \n --> ${command} halo`)
				reply(lang.wait())
				 xtext = args.join(' ')
				                cond = xtext.split(" ")
				                 a = await alpha.searchMessages(xtext, from, 10, 1)// count 10 
				                 fox = '*「 Message Search 」*\n\n'
				                num = 0
				                for (j of a.messages){
				                    num += 1
				                    if (j.message.conversation) {
				                        if (j.key.fromMe){ 
				                            fox += num+'. Sender : '+alpha.user.jid+'\n    Msg : '+j.message.conversation+'\n    MsgID : '+j.key.id+'\n    Type : conversation\n\n'
				                        }else{
				                            fox += num+'. Sender : '+j.key.participant+'\n    Msg : '+j.message.conversation+'\n    MsgID : '+j.key.id+'\n    Type : conversation\n\n'
				                        } 
				                    }
				                    else if (j.message.extendedTextMessage){
				                        if (j.key.fromMe){ 
				                            fox += num+'. Sender : '+alpha.user.jid+'\n    Msg : '+j.message.extendedTextMessage.text+'\n    MsgID : '+j.key.id+'\n    Type : extendedTextMessage\n\n'
				                        }else{
				                            fox += num+'. Sender : '+j.key.participant+'\n    Msg : '+j.message.extendedTextMessage.text+'\n    MsgID : '+j.key.id+'\n    Type : extendedTextMessage\n\n'
				                        } 
				                    }
				                }
				                reply(fox)
				await limitAdd(sender, limit)
		                break
case 'setname':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
					alpha.groupUpdateSubject(from, `${body.slice(9)}`)
					alpha.sendMessage(from, '「  SUKSES  」Mengubah Nama Grup', text, { quoted: fdoc })
					await limitAdd(sender, limit)
					break 
case 'setdesc':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
					alpha.groupUpdateDescription(from, `${body.slice(9)}`)
					alpha.sendMessage(from, '*「  SUKSES  」Mengubah Desk Grup', text, { quoted: fdoc })
					await limitAdd(sender, limit)
					break   
case 'spam':
if (!isGroup) return reply(lang.onlygc())
				if (!isGroupAdmins) return reply(lang.onlygcAdmin())
				if (args.length < 1) return reply(`Penggunaan ${prefix}spam teks|jumlahspam`)
				argzi = args.split("|")
				if (!argzi) return reply(`Penggunaan ${prefix}spam teks|jumlah`)
				if (isNaN(argzi[1])) return reply(`harus berupa angka`)
				for (let i = 0; i < argzi[1]; i++){
					alpha.sendMessage(from, argzi[0], MessageType.text)
				}
				await limitAdd(sender, limit)
					break    
case 'readall':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					var chats = await alpha.chats.all()
                    chats.map( async ({ jid }) => {
                          await alpha.chatRead(jid)
                    })
					rdl = `Berhasil membaca ${chats.length} Chat !`
					reply(rdl)
					console.log(chats.length)
					break
case 'listpc':
					  if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					  cpcp = await getpc(totalchat)
					  teks = `*L I S T  P E R S O N A L  C H A T*\nTOTAL PC: ${cpcp.length}\n\n`
					  for(let i=0; i<cpcp.length; i++){
						conts = mek.key.fromMe ? mek.user.jid : alpha.contacts[cpcp[i]] || {notify: jid.replace(/@.+/, '')}
						pushnama = alpha.contacts[cpcp[i]] != undefined ? alpha.contacts[cpcp[i]].vname || alpha.contacts[cpcp[i]].notify : undefined
						teks += `• Name : ${pushnama}\n• Tag : @${cpcp[i].split("@")[0]}\n• Wa.me : wa.me/${cpcp[i].split("@")[0]}\n\n----------------------------------\n\n`
					}
					mentions( teks, cpcp, true)
					break 
case 'listgroup':
case 'listgrup':
case 'listgc':
case 'listgrop':
case 'gruplist':
case 'groplist':
case 'grouplist':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
const txs = alpha.chats.all().filter(v => v.jid.endsWith('g.us')).map(v =>`•> ${alpha.getName(v.jid)}${enter}${v.jid}\n[${v.read_only ? 'Left' : 'Joined'}]`).join`${enter}~~${enter}`
alpha.sendMessage(m.chat, '```「 LIST GROUPS 」```\n\n' + txs, text)
break
				   break 
case 'bcsticker':
case 'bcstik':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					anu = await alpha.chats.all()
					if (isMedia && !mek.message.videoMessage || isQuotedSticker) {
						const encmedia = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
						bc = await alpha.downloadMediaMessage(encmedia)
						for (let _ of anu) {
							alpha.sendMessage(_.jid, bc, sticker, {quoted:ftroli})
						}
						fakestatus('Suksess broadcast')
					}
					break
case 'bcvideo':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					anu = await alpha.chats.all()
					if (isMedia && !mek.message.videoMessage || isQuotedVideo) {
						const encmedia = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
						bc = await alpha.downloadMediaMessage(encmedia)
						for (let _ of anu) {
							alpha.sendMessage(_.jid, bc, video, {mimetype: 'video/mp4', duration: 359996400,quoted: ftoko,caption: `[ *${setting.botname} BROADCAST* ]\n\n${body.slice(9)}`})
						}
						fakestatus(lang.successBc())
					}
					break
	case 'bcaudio':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					anu = await alpha.chats.all()
					if (isMedia && !mek.message.audioMessage || isQuotedAudio) {
						const encmedia = isQuotedAudio ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
						bc = await alpha.downloadMediaMessage(encmedia)
						for (let _ of anu) {
							alpha.sendMessage(_.jid, bc, audio, {mimetype :  'audio/mp4' , duration : 359996400, ptt : true,quoted: ftoko,caption: `[ *${setting.botname} BROADCAST* ]\n\n${body.slice(9)}`})
						}
						fakestatus(lang.successBc())
					}
					break
case 'bcgif':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					anu = await alpha.chats.all()
					if (isMedia && !mek.message.videoMessage || isQuotedVideo) {
						const encmedia = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
						bc = await alpha.downloadMediaMessage(encmedia)
						for (let _ of anu) {
							alpha.sendMessage(_.jid, bc, sticker, {mimetype: Mimetype.gif,quoted : ftroli,caption: `[ *${setting.botname} BROADCAST* ]\n\n${body.slice(7)}`})
						}
						fakestatus(lang.successBc())
					}
					break
         case 'owner':
         case 'creator':  
         ini_ownerNumber = [`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`,`${targetpc}@s.whatsapp.net`]
					let ini_list = []
					for (let i of ini_ownerNumber) {
					const vname = alpha.contacts[i] != undefined ? alpha.contacts[i].vname || alpha.contacts[i].notify : undefined
					ini_list.push({
					"displayName": 'Owner Alphabot',
					"vcard": `BEGIN:VCARD\nVERSION:3.0\nN:Sy;Bot;;;\nFN:${vname ? `${vname}` : `${alpha.user.name}`}\nORG: SUBSCRIBE ZEEONE OFC;\nitem1.TEL;waid=${i.split('@')[0]}:${i.split('@')[0]}\nitem1.X-ABLabel:Ponsel\nEND:VCARD`
					})
					}
					hehe = await alpha.sendMessage(from, {
					"displayName": `${ini_list.length} kontak`,
					"contacts": ini_list 
					}, 'contactsArrayMessage', { quoted: fkontak})
					var ini_gopayy =`Halo @${sender.split("@")[0]} itu owner ku, jangan lupa donasi kak😇`
var buttonss = [
{buttonId: 'donasi', buttonText:{displayText: 'Donasi'}, type: 1},
{buttonId: 'sewabot', buttonText:{displayText: 'Sewa'}, type: 1}
]

buttonMessagee = {
contentText: ini_gopayy,
footerText: `${tampilTanggal}\n${tampilWaktu}\n\n© ${creator}` ,
buttons: buttonss,
headerType: 1
}
alpha.sendMessage(from,  buttonMessagee, MessageType.buttonsMessage,{
        caption: 'Botwea ©2k21',
        "contextInfo": {
            text: 'hi',
            "forwardingScore": 1000000000,
            isForwarded: true,
            sendEphemeral: true,
            "mentionedJid" : [sender]
            },
			quoted: ftoko,sendEphemeral: true 
			})
					break
                case 'sider':
                shape = '✓ '
infom = await alpha.messageInfo(from, mek.message.extendedTextMessage.contextInfo.stanzaId)
tagg = []
teks = `Telah Dibaca Oleh :\n\n`
for(let i of infom.reads){
teks += shape+' ' + '@' + i.jid.split('@')[0] + '\n'
teks += `> Waktu : ` + moment(`${i.t}` * 1000).tz('Asia/Jakarta').format('DD/MM/YYYY HH:mm:ss') + '\n\n'
tagg.push(i.jid)
}
mentions(teks, tagg, true)
await limitAdd(sender, limit)
					break   
			case 'fakeloc':
			if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
               var kntl = body.slice(8)
			   var nama = kntl.split("|")[0];
			   var impostor = kntl.split("|")[1];
			   var bro = fs.readFileSync(`image/${thumbnail}`)
               alpha.sendMessage(from, { name: `${nama}`,address: `${impostor}`,jpegThumbnail: bro }, MessageType.location)                
		    await limitAdd(sender, limit)
					break   
		    case 'on':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return 
		            offline = false
		            fakeitem(lang.ownerOn())
		            break       
		    case 'status':
		            fakeitem(`*STATUS*\n${offline ? '> OFFLINE' : '> ONLINE'}\n${banChats ? '> SELF-MODE' : '> PUBLIC-MODE'}`)
		            break
		    case 'off':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return 
		            offline = true
		            waktu = Date.now()
		            anuu = args.join(' ') ? args.join(' ') : '-'
		            alasan = anuu
		            fakeitem(lang.ownerOff())
		            break   
		    case 'get':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if(!q) return reply('linknya?')
		            fetch(`${args[0]}`).then(res => res.text())  
		            .then(bu =>{
		            fakestatus(bu)
		            })   
		            await limitAdd(sender, limit)
					break   
		    case 'kontag':
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isOwner && !isCreator) return reply(lang.onlyOwner())
		            pe = args.join('')
		            entah = pe.split('|')[0]
		            nah = pe.split('|')[1]
		            if (isNaN(entah)) return reply('Invalid phone number');
		            members_ids = []
		            for (let mem of groupMembers) {
		            members_ids.push(mem.jid)
		            }
		            vcard = 'BEGIN:VCARD\n'
		            + 'VERSION:3.0\n'
		            + `FN:${nah}\n`
		            + `TEL;type=CELL;type=VOICE;waid=${entah}:${phoneNum('+' + entah).getNumber('internasional')}\n`
		            + 'END:VCARD'.trim()
		            alpha.sendMessage(from, {displayName: `${nah}`, vcard: vcard}, contact, {contextInfo: {"mentionedJid": members_ids}})
		            await limitAdd(sender, limit)
					break   
		    case 'sticktag':
		            if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isOwner && !isCreator) return reply(lang.onlyOwner())
		            if ((isMedia && !mek.message.videoMessage || isQuotedSticker) && args.length == 0) {
		            encmedia = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, sticker, options)
		            fs.unlinkSync(file)
		            } else {
		            fakegroup(`*Reply sticker yang sudah dikirim*`)
		            }
		            await limitAdd(sender, limit)
					break   
		    case 'totag':
		            if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isOwner && !isCreator) return reply(lang.onlyOwner())
		            if ((isMedia && !mek.message.videoMessage || isQuotedSticker) && args.length == 0) {
		            encmedia = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, sticker, options)
		            fs.unlinkSync(file)
		            } else if ((isMedia && !mek.message.videoMessage || isQuotedImage) && args.length == 0) {
		            encmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, image, options)
		            fs.unlinkSync(file)
		        } else if ((isMedia && !mek.message.videoMessage || isQuotedAudio) && args.length == 0) {
		            encmedia = isQuotedAudio ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		            	mimetype : 'audio/mp4',
		            	ptt : true,
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, audio, options)
		            fs.unlinkSync(file)
		        }  else if ((isMedia && !mek.message.videoMessage || isQuotedVideo) && args.length == 0) {
		            encmedia = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		            	mimetype : 'video/mp4',
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, video, options)
		            fs.unlinkSync(file)
		        } else{
		          fakestatus(`reply gambar/sticker/audio/video dengan caption ${prefix}totag`)
		        }
		        await limitAdd(sender, limit)
					break   
		    case 'fitnah':
		if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if (args.length < 1) return fakegroup(`Usage :\n${prefix}fitnah [@tag|pesan|balasanbot]]\n\nEx : \n${prefix}fitnah @tagmember|hai|hai juga`)
		            var gh = args.join('')
		            mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
		            var replace = gh.split("|")[0];
		            var target = gh.split("|")[1];
		            var bot = gh.split("|")[2];
		            alpha.sendMessage(from, `${bot}`, text, {quoted: { key: { fromMe: false, participant: `${mentioned}`, ...(from ? { remoteJid: from } : {}) }, message: { conversation: `${target}` }}})
		            break
		    case 'settarget':
		if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if(!q) return fakegroup(`${prefix}settarget 628xxxxx`)
		            targetpc = args[0]
		            fakegroup(`Succes Mengganti target fitnahpc : ${targetpc}`)
		            break
		    case 'fitnahpc':
		if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if(!q) return fakegroup(`${prefix}fitnahpc teks target|teksny`)
		            jids = `${targetpc}@s.whatsapp.net` // nomer target
		            var splitt = args.join(' ').replace(/@|\d/gi, '').split('|')
		            var taged = mek.message.extendedTextMessage.contextInfo.mentionedJid[0]
		            var options = {contextInfo: {quotedMessage: {extendedTextMessage: {text: splitt[0]}}}}
		            const responye = await alpha.sendMessage(jids, `${splitt[1]}`, MessageType.text, options)
		            await alpha.deleteMessage(jids, { id: responye.messageID, remoteJid: jids, fromMe: true })
		            break
		    case 'tomp3':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if (!isQuotedVideo) return fakegroup('```Reply videonya!```')
		            reply(mess.wait)
		            let encmedia2 = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
		            let media2 = await alpha.downloadAndSaveMediaMessage(encmedia2)
		            ran = getRandom('.mp4')
		            exec(`ffmpeg -i ${media2} ${ran}`, (err) => {
		            fs.unlinkSync(media2)
		            if (err) return fakegroup(`Err: ${err}`)
		            buffer453 = fs.readFileSync(ran)
		            alpha.sendMessage(from, buffer453, audio, { mimetype: 'audio/mp4', quoted: mek })
		            fs.unlinkSync(ran)
		            })
		            await limitAdd(sender, limit)
					break   
		    case 'fast':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if (!isQuotedVideo) return fakegroup('Reply videonya!')
		            fakegroup(mess.wait)
		            encmedia3 = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
		            media3 = await alpha.downloadAndSaveMediaMessage(encmedia3)
		            ran = getRandom('.mp4')
		            exec(`ffmpeg -i ${media3} -filter_complex "[0:v]setpts=0.5*PTS[v];[0:a]atempo=2[a]" -map "[v]" -map "[a]" ${ran}`, (err) => {
		            fs.unlinkSync(media3)
		            if (err) return fakegroup(`Err: ${err}`)
		            buffer453 = fs.readFileSync(ran)
		            alpha.sendMessage(from, buffer453, video, { mimetype: 'video/mp4', quoted: mek })
		            fs.unlinkSync(ran)
		            })
		            await limitAdd(sender, limit)
					break   
		    case 'slow':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if (!isQuotedVideo) return fakegroup('Reply videonya!')
		            fakegroup(mess.wait)
		            encmedia4 = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
		            media4 = await alpha.downloadAndSaveMediaMessage(encmedia4)
		            ran = getRandom('.mp4')
		            exec(`ffmpeg -i ${media4} -filter_complex "[0:v]setpts=2*PTS[v];[0:a]atempo=0.5[a]" -map "[v]" -map "[a]" ${ran}`, (err) => {
		            fs.unlinkSync(media4)
		            if (err) return fakegroup(`Err: ${err}`)
		            buffer453 = fs.readFileSync(ran)
		            alpha.sendMessage(from, buffer453, video, { mimetype: 'video/mp4', quoted: mek })
		            fs.unlinkSync(ran)
		            })
		            await limitAdd(sender, limit)
					break   
		case 'tupai':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
var encmedia6 = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
var media6 = await alpha.downloadAndSaveMediaMessage(encmedia6)
ran = getRandom('.mp3')
exec(`ffmpeg -i ${media6} -filter:a "atempo=0.5,asetrate=65100" ${ran}`, (err, stderr, stdout) => {
fs.unlinkSync(media6)
if (err) return reply('Error!')
let hah = fs.readFileSync(ran)
alpha.sendMessage(from, hah, audio, {mimetype: 'audio/mp4', ptt:true, quoted: mek})
fs.unlinkSync(ran)
})
await limitAdd(sender, limit)
					break   
		    case 'reverse':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if (!isQuotedVideo) return fakegroup('```Reply videonya!```')
		           var encmedia5 = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
		          var media5 = await alpha.downloadAndSaveMediaMessage(encmedia5)
		            ran = getRandom('.mp4')
		            exec(`ffmpeg -i ${media5} -vf reverse -af areverse ${ran}`, (err) => {
		            fs.unlinkSync(media5)
		            if (err) return fakegroup(`Err: ${err}`)
		            buffer453 = fs.readFileSync(ran)
		            alpha.sendMessage(from, buffer453, video, { mimetype: 'video/mp4', quoted: mek })
		            fs.unlinkSync(ran)
		            })
		            await limitAdd(sender, limit)
					break   
		    case 'anime2':
					if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            fetch('https://raw.githubusercontent.com/pajaar/grabbed-results/master/pajaar-2020-gambar-anime.txt')
		            .then(res => res.text())
		            .then(body => {
		            let tod = body.split("\n");
		            let pjr = tod[Math.floor(Math.random() * tod.length)];
		            imageToBase64(pjr)
		            .then((response) => {
		            media =  Buffer.from(response, 'base64');
		            alpha.sendMessage(from,media,image,{quoted:mek,caption:'Dasar wibu. Nih!!!\nJgn lupa sub YT : ZEEONE OFC'})
		            }
		            )
		            .catch((error) => {
		            console.log(error); 
		            }
		            )
		            });
		            await limitAdd(sender, limit)
					break   
		    case 'kontak':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            pe = args.join(' ') 
		            entah = pe.split('|')[0]
		            nah = pe.split('|')[1]
		            if (isNaN(entah)) return reply('Invalid phone number');
		            vcard = 'BEGIN:VCARD\n'
		            + 'VERSION:3.0\n'
		            + `FN:${nah}\n`
		            + `TEL;type=CELL;type=VOICE;waid=${entah}:${phoneNum('+' + entah).getNumber('internasional')}\n`
		            + 'END:VCARD'.trim()
		            alpha.sendMessage(from, {displayName: `${nah}`, vcard: vcard}, contact)
		            await limitAdd(sender, limit)
					break   
		    case 'take':
		    case 'colong':
		if (!isPremium) return sendButMessage(from, `Mohon maaf fitur ini khusus untuk user premium saja! Upgrade akun mu sekarang dengan cara ketik ${prefix}goprem`, `Click button below`, [{buttonId: 'goprem',buttonText: {displayText: `Upgrade Premium`,},type: 1,}],{quoted:mek});
		if (!isQuotedSticker) return reply('```Reply stc nya```')
		            var encmedia_ = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
				    var media_ = await alpha.downloadAndSaveMediaMessage(encmedia_)
		            anu = args.join(' ').split('|')
		            satu = anu[0] !== '' ? anu[0] : `SUBSCRIBE`
		            dua = typeof anu[1] !== 'undefined' ? anu[1] : `ZEEONE OFC`
		            require('./lib/fetcher.js').createExif(satu, dua)
					require('./lib/fetcher.js').modStick(media_ , alpha, mek, from)
					await limitAdd(sender, limit)
					break   
			case 'stikerwm':
			case 'stickerwm':
		    case 'swm':
		if (!isPremium) return sendButMessage(from, `Mohon maaf fitur ini khusus untuk user premium saja! Upgrade akun mu sekarang dengan cara ketik ${prefix}goprem`, `Click button below`, [{buttonId: 'goprem',buttonText: {displayText: `Upgrade Premium`,},type: 1,}],{quoted:mek});
		            pe = args.join('')
		            var a = pe.split("|")[0];
		            var b = pe.split("|")[1];
		            if (isMedia && !mek.message.videoMessage || isQuotedImage ) {
		            const encmedia___ = isQuotedImage   ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
		             media___ = await alpha.downloadAndSaveMediaMessage(encmedia___)
		            await createExif(a,b)
		            out = getRandom('.webp')
		            ffmpeg(media___)
		            .on('error', (e) => {
		            console.log(e)
		            alpha.sendMessage(from, 'Terjadi kesalahan', 'conversation', { quoted: mek })
		            fs.unlinkSync(media___)
		            })
		            .on('end', () => {
		            _out = getRandom('.webp')
		            spawn('webpmux', ['-set','exif','./sticker/data.exif', out, '-o', _out])
		            .on('exit', () => {
		            alpha.sendMessage(from, fs.readFileSync(_out),'stickerMessage', { quoted: mek })
		            fs.unlinkSync(out)
		            fs.unlinkSync(_out)
		            fs.unlinkSync(media___)
		            })
		            })
		            .addOutputOptions([`-vcodec`,`libwebp`,`-vf`,`scale='min(320,iw)':min'(320,ih)':force_original_aspect_ratio=decrease,fps=15, pad=320:320:-1:-1:color=white@0.0, split [a][b]; [a] palettegen=reserve_transparent=on:transparency_color=ffffff [p]; [b][p] paletteuse`])
		            .toFormat('webp')
		            .save(out) 
		            } else if ((isMedia && mek.message.videoMessage.seconds < 11 || isQuotedVideo && mek.message.extendedTextMessage.contextInfo.quotedMessage.videoMessage.seconds < 11) && args.length == 0) {
		            const encmedia___ = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
		            const media___ = await alpha.downloadAndSaveMediaMessage(encmedia___)
		            pe = args.join('')
		            var a = pe.split("|")[0];
		            var b = pe.split("|")[1];
		            await createExif(a,b)
		            out = getRandom('.webp')
		            ffmpeg(media___)
		            .on('error', (e) => {
		            console.log(e)
		            alpha.sendMessage(from, 'Terjadi kesalahan', 'conversation', { quoted: mek })
		            fs.unlinkSync(media___)
		            })
		            .on('end', () => {
		            _out = getRandom('.webp')
		            spawn('webpmux', ['-set','exif','./sticker/data.exif', out, '-o', _out])
		            .on('exit', () => {
		            alpha.sendMessage(from, fs.readFileSync(_out),'stickerMessage', { quoted: mek })
		            fs.unlinkSync(out)
		            fs.unlinkSync(_out)
		            fs.unlinkSync(media___)
		            })
		            })
		            .addOutputOptions([`-vcodec`,`libwebp`,`-vf`,`scale='min(320,iw)':min'(320,ih)':force_original_aspect_ratio=decrease,fps=15, pad=320:320:-1:-1:color=white@0.0, split [a][b]; [a] palettegen=reserve_transparent=on:transparency_color=ffffff [p]; [b][p] paletteuse`])
		            .toFormat('webp')
		            .save(out)       
		            } else {
		            fakestatus(`Kirim gambar dengan caption ${prefix}swm teks|teks atau tag gambar yang sudah dikirim`)
		            }
		            await limitAdd(sender, limit)
					break   
		    case 'upswteks':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if (!q) return fakestatus('Isi teksnya!')
		            alpha.sendMessage('status@broadcast', `${q}`, extendedText)
		            fakeitem(`Sukses Up story wea teks ${q}`)
		            break
		    case 'upswimage':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if (isQuotedImage) {
		            const swsw = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            cihcih = await alpha.downloadMediaMessage(swsw)
		            alpha.sendMessage('status@broadcast', cihcih, image, { caption: `${q}` })
		            bur = `Sukses Upload Story Image dengan Caption: ${q}`
		            alpha.sendMessage(from, bur, text, { quoted: mek })
		            } else {
		            fakegroup('```Reply gambarnya!```')
		            }
		            break
		    case 'upswvideo':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            if (isQuotedVideo) {
		            const swsw = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            cihcih = await alpha.downloadMediaMessage(swsw)
		            alpha.sendMessage('status@broadcast', cihcih, video, { caption: `${q}` }) 
		            bur = `Sukses Upload Story Video dengan Caption: ${q}`
		            alpha.sendMessage(from, bur, text, { quoted: mek })
		            } else {
		            fakegroup('```Reply videonya!```')
		            }
		            break
		    case 'fdeface':
		            ge = args.join('')           
		            var pe = ge.split("|")[0];
		            var pen = ge.split("|")[1];
		            var pn = ge.split("|")[2];
		            var be = ge.split("|")[3];
		            const fde = `kirim/reply image dengan capion ${prefix}fdeface link|title|desc|teks`
		            if (args.length < 1) return reply (fde)
		            const dipes = isQuotedSticker || isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
		            const tipes = await alpha.downloadAndSaveMediaMessage(dipes)        
		            const bufer = fs.readFileSync(tipes)
		            const desc = `${pn}`
		            const title = `${pen}`
		            const url = `${pe}`
		            const buu = `https://${be}`
		    		var anu = {
		        	detectLinks: false
		    		}
		    		var mat = await alpha.generateLinkPreview(url)
		    		mat.title = title;
		    		mat.description = desc;
		    		mat.jpegThumbnail = bufer;
		   			mat.canonicalUrl = buu; 
		    		alpha.sendMessage(from, mat, MessageType.extendedText, anu)
		            await limitAdd(sender, limit)
					break   
		    case 'public':
		              if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		          	if (banChats === false) return
		          	banChats = false
		          	//fakeitem(`「 *PUBLIC-MODE* 」`)
						sendButMessage(from, `「 *PUBLIC-MODE* 」`, `Click self to return to self mode`, [
            {
              buttonId: 'self',
              buttonText: {
                displayText: `Self Mode`,
              },
              type: 1,
            }]);
        break;
			case 'self':
			          if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		          	if (banChats === true) return
		          	banChats = true
		          	//fakeitem(`「 *SELF-MODE* 」`)
		          	sendButMessage(from, `「 *SELF-MODE* 」`, `Click public to return to public mode`, [
            {
              buttonId: 'public',
              buttonText: {
                displayText: `Public Mode`,
              },
              type: 1,
            }]);
        break;
		case 'revoke':
if (!isGroup) return fakegroup(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
await alpha.revokeInvite(from)
reply(lang.success())
break
		 	case 'hidetag':
		     case '_`':
		     if (!isGroup) return fakegroup(lang.onlygc())
			if (!isGroupAdmins) return reply(lang.onlygcAdmin())
		var value = args.join(' ')
					var group = await alpha.groupMetadata(from)
					var member = group['participants']
					var mem = []
					member.map(async adm => {
					mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
					})
					var optionshidetag = {
					text: value,
					contextInfo: { mentionedJid: mem },
					quoted: mek
					}
					alpha.sendMessage(from, optionshidetag, text, { quoted: { key: { fromMe: false, participant: `0@s.whatsapp.net`, ...(from ? { remoteJid: "393470602054-1351628616@g.us" } : {}) }, message: { "imageMessage": { "url": "https://mmg.whatsapp.net/d/f/At0x7ZdIvuicfjlf9oWS6A3AR9XPh0P-hZIVPLsI70nM.enc", "mimetype": "image/jpeg", "caption": `${setting.fake}`, "fileSha256": "+Ia+Dwib70Y1CWRMAP9QLJKjIJt54fKycOfB2OEZbTU=", "fileLength": "28777", "height": 1080, "width": 1079, "mediaKey": "vXmRR7ZUeDWjXy5iQk17TrowBzuwRya0errAFnXxbGc=", "fileEncSha256": "sR9D2RS5JSifw49HeBADguI23fWDz1aZu4faWG/CyRY=", "directPath": "/v/t62.7118-24/21427642_840952686474581_572788076332761430_n.enc?oh=3f57c1ba2fcab95f2c0bb475d72720ba&oe=602F3D69", "mediaKeyTimestamp": "1610993486", "jpegThumbnail": fs.readFileSync(`image/${thumbnail}`)} }  } })
					await limitAdd(sender, limit)
					break   
				case 'sewacheck':
				case 'ceksewa':
							if (!isGroup) return fakegroup(lang.onlygc())
							if (!isSewa) return reply(`Group ini tidak terdaftar dalam list sewabot. Ketik ${prefix}sewabot untuk info lebih lanjut`)
							let cekvip = ms(_sewa.getSewaExpired(from, sewa) - Date.now())
							let premiumnya = `*「 SEWA EXPIRED 」*\n\n📛 *ID*: ${from}\n⏰ *Expired :* ${cekvip.days} day(s) ${cekvip.hours} hour(s) ${cekvip.minutes} minute(s)`
							reply(premiumnya)
							break
				case 'sewa':
							if (!isGroup)return fakegroup(lang.onlygc())
							if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
							if (args.length < 1) return reply(`Penggunaan :\n*${prefix}sewa* add/del waktu`)
							if (args[0] === 'add'){
								_sewa.addSewaGroup(from, args[1], sewa)
								reply(lang.success())
								} else if (args[0] === 'del'){
									sewa.splice(_sewa.getSewaPosition(from, sewa), 1)
									fs.writeFileSync('./src/sewa.json', JSON.stringify(sewa))
									reply(lang.success())
									} else {
										reply(`Example : *${prefix}sewa* add/del waktu`)
										}
							break
				case 'sewalist': 
				case 'listsewa':
							let txtnyee = `*「 LIST SEWA」*\nJumlah : ${sewa.length}\n\n`
							for (let i of sewa){
								let cekvipp = ms(i.expired - Date.now())
								txtnyee += `🆔 *ID :* ${i.id} \n⏰ *Expire :* ${cekvipp.days} day(s) ${cekvipp.hours} hour(s) ${cekvipp.minutes} minute(s) ${cekvipp.seconds} second(s)\n\n`
								}
							reply(txtnyee)
							break
				case 'premium': 
							if (args.length === 0) return reply(`Kirim perintah *${prefix}premium* add/del 62xxx waktu (misal 1 hari -> 1d)\nExample:\n${prefix}premium add 62887435047326 1d`)
							if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
							if (args[0] === 'add') {
								if (mek.message.extendedTextMessage != undefined) {
									mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
									premium.addPremiumUser(mentioned[0], args[2], _premium)
									reply(`*「 PREMIUM ADDED 」*\n\n📛 *ID*: ${mentioned[0]}\n⏰ *Expired*: ${ms(toMs(args[2])).days} day(s) ${ms(toMs(args[2])).hours} hour(s) ${ms(toMs(args[2])).minutes} minute(s)`)
									} else {
										premium.addPremiumUser(args[1] + '@s.whatsapp.net', args[2], _premium)
										reply(`*「 PREMIUM ADDED 」*\n\n📛 *ID*: ${args[1]}@s.whatsapp.net\n⏰ *Expired*: ${ms(toMs(args[2])).days} day(s) ${ms(toMs(args[2])).hours} hour(s) ${ms(toMs(args[2])).minutes} minute(s)`)
										}
										} else if (args[0] === 'del') {
											if (mek.message.extendedTextMessage != undefined) {
									mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
									_premium.splice(premium.getPremiumPosition(mentioned[0], _premium), 1)
									fs.writeFileSync('./src/premiun.json', JSON.stringify(_premium))
									reply(lang.success())
									} else {
										_premium.splice(premium.getPremiumPosition(args[1] + '@s.whatsapp.net', _premium), 1)
										fs.writeFileSync('./src/premiun.json', JSON.stringify(_premium))
										reply(lang.success())
										}
										} else {
											reply('emror')
											}
							break
				case 'premiumcheck': case 'cekpremium': 
							if (!isPremium) return reply(`Akun kamu belum premium silahkan ${prefix}buypremium`)
							const cekExp = ms(await premium.getPremiumExpired(sender, _premium) - Date.now())
							reply(`*「 PREMIUM EXPIRED 」*\n\n🆔 *ID*: ${sender}\n🏦 *Premium left*: ${cekExp.days} day(s) ${cekExp.hours} hour(s) ${cekExp.minutes} minute(s)`)
							break
				case 'listprem': case 'listpremium':
							let txt = `「 *PREMIUM USER LIST* 」\n\n`
							let men = [];
							for (let i of _premium){
								men.push(i.id)
								const checkExp = ms(i.expired - Date.now())
								txt += `🆔 *ID :* @${i.id.split("@")[0]}\n⏰ *Expired*: ${checkExp.days} day(s) ${checkExp.hours} hour(s) ${checkExp.minutes} minute(s)\n\n`
								}
								mentions(txt, men, true)
							break
				case 'payment': case 'pay': case 'donasi': case 'donate':
				alpha.sendMessage(from, fs.readFileSync(`./image/${setting.donasi}`), image, {caption: 'Scan untuk melakukan pembayaran', quoted: mek, thumbnail: fs.readFileSync(`./image/${setting.donasi}`)})
				break
				case 'belipremium': case 'buypremium': case 'sewabot': case 'goprem':
							let sewalak = await getBuffer('https://telegra.ph/file/5e96a14f1ebaee0df2e24.jpg')
							sendButLocation(from, pc_sewa(_minggu, __minggu, ___minggu, _bulan, _permanen, __permanen) , `©  ${ownername}`,sewalak,  [{"buttonId": `payment`,"buttonText": {"displayText": "PAYMENT"},"type": "RESPONSE"}], {})
							break
				case 'gcbot': case 'grupbot': case 'groupbot':
							let gcbot = await getBuffer('https://telegra.ph/file/09d4f9b77a0745f35bdfa.jpg')
							reply('Group bot telah di kirim ke private chat')
							sendButLocation(sender, gcbotwa() , `©  ${ownername}`,gcbot,  [{"buttonId": `sewabot`,"buttonText": {"displayText": "SEWABOT"},"type": "RESPONSE"}], {})
							break
				case 'infobot':
				let infobopot = await getBuffer('https://telegra.ph/file/06fad83011a4b1cecd4ba.jpg')
							sendButLocation(from, infobot(status, offline, latensii, totalchat, giid, wa_version, mcc, mnc, os_version, device_manufacturer, device_model, ownerNumberrr,runtime, kyun) , `©  ${ownername}`,infobopot,  [{"buttonId": 'gcbot',"buttonText": {"displayText": "GROUP BOT"},"type": "RESPONSE"}], {contextInfo:{"mentionedJid": [ownerNumberrr]}})
							break
			case 'play2':
			if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (args.length === 0) return reply(`Kirim perintah *${prefix}play* _Judul lagu yang akan dicari_`)
		            var srch = args.join('')
		    		aramas = await yts(srch);
		    		aramat = aramas.all 
		   			var mulaikah = aramat[0].url							
		                  try {
		                    yta(mulaikah)
		                    .then((res) => {
		                        const { dl_link, thumb, title, filesizeF, filesize } = res
		                        axios.get(`https://tinyurl.com/api-create.php?url=${dl_link}`)
		                        .then(async (a) => {
		                        if (Number(filesize) >= 100000) return sendMediaURL(from, thumb, `*PLAY MUSIC*\n\n*Title* : ${title}\n*Ext* : MP3\n*Filesize* : ${filesizeF}\n*Link* : ${a.data}\n\n_Untuk durasi lebih dari batas disajikan dalam mektuk link_`)
		                        const captions = `🎧 *PLAY MUSIC*\n\n*Title* : ${title}\n*Ext* : MP3\n*Size* : ${filesizeF}\n*Link* : ${a.data}\n\n_Silahkan tunggu file media sedang dikirim mungkin butuh beberapa menit_`
		                       await sendMediaURL(from, thumb, captions)
		                        sendMediaURL(from, dl_link).catch(() => reply('error'))
		                        })                
		                        })
		                        } catch (err) {
		                        reply(mess.error.api)
		                        }
		                   await limitAdd(sender, limit)
					break     
		
		               case 'sticker': case 'stikerin':
					case 'stiker': case 's':
					if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
						if (isMedia && !mek.message.videoMessage || isQuotedImage) {
							const encmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
							const media = await alpha.downloadAndSaveMediaMessage(encmedia)
							//ran = getRandom('.webp')
                        await ffmpeg(`./${media}`)
                            .input(media)
                            .on('start', function (cmd) {
                                console.log(`Started : ${cmd}`)
                            })
                            .on('error', function (err) {
                                console.log(`Error : ${err}`)
                                fs.unlinkSync(media)
                                 })
                            .on('end', async function () {
                                console.log('Finish')
                                exec(`webpmux -set exif ./sticker/data.exif ./sticker/${sender}.webp -o ./sticker/${sender}.webp`, async (error) => {
								if (error) return reply(lang.tryAgain())
								await alpha.sendMessage(from, fs.readFileSync(`./sticker/${sender}.webp`), sticker, {quoted: fgif})
								fs.unlinkSync(media)
								fs.unlinkSync(`./sticker/${sender}.webp`)
								})
								})
                           .addOutputOptions([`-vcodec`, `libwebp`, `-vf`, `scale='min(320,iw)':min'(320,ih)':force_original_aspect_ratio=decrease,fps=15, pad=320:320:-1:-1:color=white@0.0, split [a][b]; [a] palettegen=reserve_transparent=on:transparency_color=ffffff [p]; [b][p] paletteuse`])
                            .toFormat('webp')
                            .save(`./sticker/${sender}.webp`)
                    } else if ((isMedia && mek.message.videoMessage.seconds < 11 || isQuotedVideo && mek.message.extendedTextMessage.contextInfo.quotedMessage.videoMessage.seconds < 11) && args.length == 0) {
                        const encmedia = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
                        const media = await alpha.downloadAndSaveMediaMessage(encmedia)
                    //    ran = getRandom('.webp')
                        await ffmpeg(`./${media}`)
                            .inputFormat(media.split('.')[1])
                            .on('start', function (cmd) {
                                console.log(`Started : ${cmd}`)
                            })
                            .on('error', function (err) {
                                console.log(`Error : ${err}`)
                                fs.unlinkSync(media)
                                tipe = media.endsWith('.mp4') ? 'video' : 'gif'
                                reply(`❌ Gagal, pada saat mengkonversi ${tipe} ke stiker`)
                            })
                            .on('end', async function () {
                                console.log('Finish')
                                exec(`webpmux -set exif ./sticker/data.exif ./sticker/${sender}.webp -o ./sticker/${sender}.webp`, async (error) => {
								if (error) return reply(lang.tryAgain())
								await alpha.sendMessage(from, fs.readFileSync(`./sticker/${sender}.webp`), sticker, {quoted: fgif})
								fs.unlinkSync(media)
								fs.unlinkSync(`./sticker/${sender}.webp`)
								})
								})
								.addOutputOptions([`-vcodec`, `libwebp`, `-vf`, `scale='min(320,iw)':min'(320,ih)':force_original_aspect_ratio=decrease,fps=15, pad=320:320:-1:-1:color=white@0.0, split [a][b]; [a] palettegen=reserve_transparent=on:transparency_color=ffffff [p]; [b][p] paletteuse`])
                            .toFormat('webp')
                            .save(`./sticker/${sender}.webp`)
                    } else if ((isMedia || isQuotedImage) && args[0] == 'nobg') {
                        const encmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
                        const media = await alpha.downloadAndSaveMediaMessage(encmedia)
                        ranw = getRandom('.webp')
                        ranp = getRandom('.png')
                        reply(mess.wait)
                        keyrmbg = 'bcAvZyjYAjKkp1cmK8ZgQvWH'
                        await removeBackgroundFromImageFile({ path: media, apiKey: keyrmbg, size: 'auto', type: 'auto', ranp }).then(res => {
                            fs.unlinkSync(media)
                            let bufferir9vn5 = Buffer.from(res.base64img, 'base64')
                            fs.writeFileSync(ranp, bufferir9vn5, (err) => {
                                if (err) return reply('Gagal, Terjadi kesalahan, silahkan coba beberapa saat lagi.')
                            })
                            exec(`ffmpeg -i ${ranp} -vcodec libwebp -filter:v fps=fps=20 -lossless 1 -loop 0 -preset default -an -vsync 0 -s 512:512 ${ranw}`, (err) => {
                                fs.unlinkSync(ranp)
                                if (err) return reply(lang.tryAgain())
                                alpha.sendMessage(from, fs.readFileSync(ranw), sticker, { quoted: mek })
                                    fs.unlinkSync(ranw)
                                })
                            })
                    } else {
                        reply(`Kirim gambar dengan caption ${prefix}sticker atau tag gambar yang sudah dikirim\nDurasi sticker video 1-9 detik...`)
                    }
                    await limitAdd(sender, limit)
					break   
		    case 'toimg':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		if (!isQuotedSticker) return reply('Reply stc nya!')
					reply(lang.wait())
					encmediaa = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
					mediaa = await alpha.downloadAndSaveMediaMessage(encmediaa)
					ran = getRandom('.png')
					exec(`ffmpeg -i ${mediaa} ${ran}`, (err) => {
					fs.unlinkSync(mediaa)
					if (err) return reply('Yah gagal, coba ulangi ^_^')
					buffer = fs.readFileSync(ran)
					fakethumb(buffer,'```Nih kak, jgn lupa Support YT : ZEEONE OFC```')
					fs.unlinkSync(ran)
					})
					await limitAdd(sender, limit)
					break   
			case 'ytsearch': case 'yts':
			if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (args.length < 1) return reply('Tolong masukan query!')
					var srch = args.join(' ');
					try {
		        	var aramas = await yts(srch);
		   			} catch {
		        	return await alpha.sendMessage(from, 'Error!', MessageType.text, dload)
		    		}
		    		aramat = aramas.all 
		    		var tbuff = await getBuffer(aramat[0].image)
		    		var ytresult = '';
		    		ytresult += '「 *YOUTUBE SEARCH* 」'
		    		ytresult += '\n________________________\n\n'
		   			aramas.all.map((video) => {
		        	ytresult += '🐣 Title: ' + video.title + '\n'
		            ytresult += '🐤 Link: ' + video.url + '\n'
		            ytresult += '🦊 Durasi: ' + video.timestamp + '\n'
					ytresult += '🐺 Views: ' + video.views + '\n'
		            ytresult += '🦡 Upload: ' + video.ago + '\n________________________\n\n'
		    		});
		    		ytresult += '*WHATSAPP-BOT*'
		    		await fakethumb(tbuff,ytresult)
					await limitAdd(sender, limit)
					break   
			case 'setreply':
			case 'setfake':
			        if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!q) return fakegroup(mess.wrongFormat)
					fake = q
					fakegroup(`Succes Mengganti Conversation Fake : ${q}`)
					break
			case 'setprefix':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
reply('Pke yg multi aja')
/*if (args.length < 1) return reply(`*Format Error!*\n\n*Example :*\n •${prefix + command} multi\n •${prefix + command} nopref\n •${prefix + command} #`)
if((args[0]) == 'multi'){
if(multi)return reply('_Sudah diaktifkan sebelumnya!_')
multi = true
nopref = false
single = false
reply(`_Succses mengganti Prefix ke Multiprefix!_`)
}else
if ((args[0]) == 'nopref'){
if(nopref)return reply('_Sudah diaktifkan sebelumnya!_')
multi = false
single = false
nopref = true
reply(`_Succses mengganti Prefix ke noprefix!_`)
}else
if((args[0]) == `${q}`){
multi = false
nopref = false
single = true
prefa = `${q}`
reply(`_Succses mengganti Prefix ke ${q}_`)
}*/
break
			case 'setfakeimg':
			        if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		        	if ((isMedia && !mek.message.videoMessage || isQuotedImage || isQuotedSticker) && args.length == 0) {
		            boij = isQuotedImage || isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
					delb = await alpha.downloadMediaMessage(boij)
					fs.writeFileSync(`./image/${thumbnail}`, delb)
					fakestatus('Sukses')
		        	} else {
		            fakeitem(`Kirim gambar dengan caption ${prefix}setfakeimg`)
		          	}
					break	
			case 'setthumb':
			        if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
			        if ((isMedia && !mek.message.videoMessage || isQuotedImage || isQuotedSticker) && args.length == 0) {
		          	boij = isQuotedImage || isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
					delb = await alpha.downloadMediaMessage(boij)
					fs.writeFileSync(`./image/${thumbnail}`, delb)
					fakestatus('Sukses')
		        	} else {
		            fakegroup(`Kirim gambar dengan caption ${prefix}sethumb`)
		          	}
					break	
			case 'image':
			if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		            if (args.length < 1) return fakegroup('Masukan teks!')
		            const gimg = args.join('');
gis(gimg, async (error, result) => {
n = result
images = n[Math.floor(Math.random() * n.length)].url
cptg = `*-------「 GIMAGE SEARCH 」-------*
⛄ *Query* : ${gimg}
🔗 *Media Url* : ${images}`
var imgnya = await getBuffer(images)
sendButImage(from,  cptg , `© ${ownername}`,imgnya, [{"buttonId": `image ${q}`,"buttonText": {"displayText": "Next Image"},"type": "RESPONSE"}], {quoted: mek})
			})
					await limitAdd(sender, limit)
					break   
		    
		    case 'brainly':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (args.length < 1) return reply('Pertanyaan apa')
		          	brien = args.join(' ')
					brainly(`${brien}`).then(res => {
					teks = '❉───────────────────────❉\n'
					for (let Y of res.data) {
					teks += `\n*「 _BRAINLY_ 」*\n\n*➸ Pertanyaan:* ${Y.pertanyaan}\n\n*➸ Jawaban:* ${Y.jawaban[0].text}\n❉──────────────────❉\n`
					}
					alpha.sendMessage(from, teks, text,{quoted:mek,detectLinks: false})                        
		            }).catch(e => {
					reply('Terjadi kesalahan, coba beberapa saat lagi')
					})             
					await limitAdd(sender, limit)
					break       
    case 'playstore':
    if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
            if(!q) return reply('lu nyari apa?')
            let play = await hx.playstore(q)
            let store = '❉─────────────────────❉\n'
            for (let i of play){
            store += `\n*「 *PLAY STORE* 」*\n
- *Nama* : ${i.name}
- *Link* : ${i.link}\n
- *Dev* : ${i.developer}
- *Link Dev* : ${i.link_dev}\n❉─────────────────────❉`
            }
            reply(store)
            await limitAdd(sender, limit)
					break   
		    case 'igstalk':
		if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
		       if (!q) return fakegroup('Usernamenya?')
		var halzmal = q
		            const tod = await fetchJson(`https://ferdiz-afk.my.id/api/stalkig?username=${halzmal}`);
var nih_buff = await getBuffer(tod.picurl);
const tt = `*INSTAGRAM STALK*

\nUsername: ${tod.username}\nFullname: ${tod.fullname}\npostingan: ${tod.post}\ncategory_akun: ${tod.category_name}\nverified: ${tod.verified_user}\nprivate: ${tod.private_user}\nFollowing: ${tod.following}\nFollower: ${tod.followers}\nBio:\n${tod.bio}`;
alpha.sendMessage(from, nih_buff, image, { quoted: mek, caption: tt });
await limitAdd(sender, limit)
					break       
		
case 'linkwa':
case 'grupwa':
case 'groupwa':
case 'gcwa':
if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
            if(!q) return reply('cari group apa?')
            hx.linkwa(q)
            .then(result => {
            let res = '「 *GC WA* 」\n\n'
            for (let i of result) {
            res += `*Nama*: *${i.nama}\n*Link*: ${i.link}\n\n`
            }
            reply(res)
            });
            await limitAdd(sender, limit)
			break    
case 'lirik':
if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
            if(!q) return reply('lagu apa?')
            let song = await hx.lirik(q)
            sendMediaURL(from,song.thumb,song.lirik)
            await limitAdd(sender, limit)
			break  
    case 'anime':
    if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
 if(!q) return reply('Judul animenya?')
 zee.Anime(`${q}`).then(async data => {
                    let txt = `*-------「 ANIME-SEARCH 」-------*\n\n`
                    for (let i of data) {
                        txt += `*📫 Title :* ${i.judul}\n`
                        txt += `*📚 Url :* ${i.link}\n-----------------------------------------------------\n`
                    }
                    await sendFileFromUrl(from,data[0].thumbnail,txt,mek)
                })
                .catch((err) => {
                    reply(lang.tryAgain())
                })
            break
    case 'otaku':
    if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
            if(!q) return reply('Judul animenya?')
            let anime = await hx.otakudesu(q)
            rem = `*Judul* : ${anime.judul}
*Jepang* : ${anime.jepang}
*Rating* : ${anime.rate}
*Produser* : ${anime.produser}
*Status* : ${anime.status}
*Episode* : ${anime.episode}
*Durasi* : ${anime.durasi}
*Rilis* : ${anime.rilis}
*Studio* : ${anime.studio}
*Genre* : ${anime.genre}\n
*Sinopsis* :
${anime.desc}\n\n*Link Batch* : ${anime.batch}\n*Link Download SD* : ${anime.batchSD}\n*Link Download HD* : ${anime.batchHD}`
           var ram = await getBuffer(anime.img)
            alpha.sendMessage(from,ram,image,{quoted:mek,caption:rem})
            await limitAdd(sender, limit)
			break   
    case 'komiku':
    if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return sendButMessage(from, lang.limit(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
            if(!q) return reply(`judulnya?\n${prefix}komiku mao gakuin`)
            let komik = await hx.komiku(q)
            result = `*Title* : ${komik.title}\n
*Title Indo* : ${komik.indo}\n
*Update* : ${komik.update}\n
*Desc* : ${komik.desc}\n
*Chapter Awal* : ${komik.chapter_awal}
*Chapter Akhir* : ${komik.chapter_akhir}`
            sendMediaURL(from, komik.image,result)
            await limitAdd(sender, limit)
					break     
			case 'term':
			
			        if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!q) return fakegroup(mess.wrongFormat)
					exec(q, (err, stdout) => {
					if (err) return fakegroup(`ALPHABOT:~ ${err}`)
					if (stdout) {
					fakegroup(stdout)
					}
					})
				    break 
		    case 'join':
		            if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
		            try {
		            if (!isUrl(args[0]) && !args[0].includes('whatsapp.com')) return reply(lang.erorLink())
		            hen = args[0]
		            if (!q) return fakestatus('Masukan link group')
		            var codeInvite = hen.split('https://chat.whatsapp.com/')[1]
		            if (!codeInvite) return fakegroup ('pastikan link sudah benar!')
		            var response = await alpha.acceptInvite(codeInvite)
		            fakestatus('```SUKSES JOIN GRUP```')
		            } catch {
		            fakegroup('```LINK ERROR!```')
		            }
		            break
		    case 'twmp4': case 'twitter':
if (args.length < 1) return reply('Link?')
lin = args[0]
reply(lang.wait())
zee.Twitter(`${lin}`).then(async data => {
                    let txt = `*----「 TWITTER DOWNLOADER 」----*\n\n`
                    txt += `*📫 Title :* ${data.title}\n`
                    txt += `*📟 Quality :* ${data.medias[1].quality}\n`
                    txt += `*💾 Size :* ${data.medias[1].formattedSize}\n`
                    txt += `*📚 Url :* ${data.url}`
                    sendFileFromUrl(from,data.medias[1].url,txt,mek)
                })
                .catch((err) => {
                    reply(lang.err())
                })
await limitAdd(sender, limit)
					break   
case 'twmp3':
if (args.length < 1) return reply('Link?')
lin = args[0]
reply(lang.wait())
hx.twitter(lin).then(async (res) => {
console.log('[ TWITTER ] downloader')
Anu = res.SD
fto = fs.readFileSync(`./image/${thumbnail}`)
alpha.sendMessage(from, fto, image, {quoted:mek, caption:`*TWITTER MP3*${enter}${enter}•> Hd : ${res.HD}${enter}•> Sd : ${res.SD}${enter}•> Audio : ${res.audio}${enter}${enter}_Please wait, the media file is being sent it may take a few minutes_`, thumbnail:fs.readFileSync(`./image/${thumbnail}`), contextInfo:{forwardingScore: 989, isForwarded: true}})
khs = await getBuffer(Anu)
alpha.sendMessage(from, khs, audio, {mimetype:'audio/mp4', filename:'alphagan.mp3', quoted:mek, ptt:true})
})
await limitAdd(sender, limit)
					break   
		    case 'runtime':
		    case 'test':
		            run = process.uptime() 
		            teks = `${(kyun(os.uptime()))}`
		            reply(teks)
		            break  
			case 'speed':
			case 'ping':
			const timestamp = speed();
					const latensi = speed() - timestamp
					exec(`neofetch --stdout`, (error, stdout, stderr) => {
					const child = stdout.toString('utf-8')
					const teks = child.replace(/Memory:/, "Ram:")
					const pingnya = `\`\`\`${teks}Speed: ${latensi.toFixed(4)} Second\`\`\``
					fakegroup(pingnya)
					})
					break
               
		    case 'totag':
		if (!isGroupAdmins) return reply(lang.onlygcAdmin())
		if ((isMedia && !mek.message.videoMessage || isQuotedSticker) && args.length == 0) {
		            encmedia = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, sticker, options)
		            fs.unlinkSync(file)
		            } else if ((isMedia && !mek.message.videoMessage || isQuotedImage) && args.length == 0) {
		            encmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, image, options)
		            fs.unlinkSync(file)
		        } else if ((isMedia && !mek.message.videoMessage || isQuotedAudio) && args.length == 0) {
		            encmedia = isQuotedAudio ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                mimetype : 'audio/mp4',
		                ptt : true,
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, audio, options)
		            fs.unlinkSync(file)
		        }  else if ((isMedia && !mek.message.videoMessage || isQuotedVideo) && args.length == 0) {
		            encmedia = isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            file = await alpha.downloadAndSaveMediaMessage(encmedia, filename = getRandom())
		            value = args.join(" ")
		            var group = await alpha.groupMetadata(from)
		            var member = group['participants']
		            var mem = []
		            member.map(async adm => {
		            mem.push(adm.id.replace('c.us', 's.whatsapp.net'))
		            })
		            var options = {
		                mimetype : 'video/mp4',
		                contextInfo: { mentionedJid: mem },
		                quoted: mek
		            }
		            ini_buffer = fs.readFileSync(file)
		            alpha.sendMessage(from, ini_buffer, video, options)
		            fs.unlinkSync(file)
		        } else{
		          fakegroup(`reply gambar/sticker/audio/video dengan caption ${prefix}totag`)
		        }
		        await limitAdd(sender, limit)
					break   
		    case 'tomp4':
		            if ((isMedia && !mek.message.videoMessage || isQuotedSticker) && args.length == 0) {
		            ger = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            let owogi = await alpha.downloadAndSaveMediaMessage(ger)
		            webp2mp4File(owogi).then(res=>{
		            sendMediaURL(from,res.result,'Done')
		            })
		            }else {
		            fakegroup('reply stiker')
		            }
		            fs.unlinkSync(owogi)
		            await limitAdd(sender, limit)
					break   
			case 'togif':
		            if ((isMedia && !mek.message.videoMessage || isQuotedSticker) && args.length == 0) {
		            ger = isQuotedSticker ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
		            let owoogi = await alpha.downloadAndSaveMediaMessage(ger)
		            webp2mp4File(owoogi).then(async res=>{
					let ksksn = await getBuffer(res.result)
		            alpha.sendMessage(from, ksksn, MessageType.video, {mimetype: 'video/gif', gifPlayback: true, caption: lang.success(), quoted: fgif })
		            })
		            }else {
		            fakegroup('Reply stiker')
		            }
		            fs.unlinkSync(owoogi)
		            await limitAdd(sender, limit)
					break   
		    case 'tourl':
			case 'imgtourl':{
                if ((isMedia && !mek.message.videoMessage || isQuotedImage || isQuotedVideo ) && args.length == 0) {
 									 boij = isQuotedImage || isQuotedVideo ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek
									 owgi = await alpha.downloadMediaMessage(boij)
									 res = await uploadImages(owgi)
									 reply(res)
										} else {
											reply('kirim/reply gambar/video')
										}
										}
											await limitAdd(sender, limit)
					break   
/*
]=====> NSFW MENU<=====[
*/

			case 'awoo':
					if (!isNsfw) return reply(lang.nsfwmo())
					reply(lang.wait())
					anu = await fetchJson(`https://waifu.pics/api/sfw/awoo`)
					buffer = await getBuffer(anu.url)
					anu  = {contextInfo: {"forwardingScore":999,"isForwarded":true,'stanzaId': msgId, 'participant':`${numbernye}@s.whatsapp.net`, 'remoteJid': '6283136505591-1614953337@g.us', 'quotedMessage': {"locationMessage": {"degreesLatitude": 41.893714904785156, "degreesLongitude": -87.63370513916016, "name": fake , 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
                    alpha.sendMessage(from, buffer, image, anu)
					await limitAdd(sender, limit)
					break   
			case 'blowjob2':
					if (!isNsfw) return reply(lang.nsfwmo())
					reply(lang.wait())
					anu = await fetchJson(`https://nekos.life/api/v2/img/blowjob`)
					buffer = await getBuffer(anu.url)
					anu  = {contextInfo: {"forwardingScore":999,"isForwarded":true,'stanzaId': msgId, 'participant':`${numbernye}@s.whatsapp.net`, 'remoteJid': '6283136505591-1614953337@g.us', 'quotedMessage': {"locationMessage": {"degreesLatitude": 41.893714904785156, "degreesLongitude": -87.63370513916016, "name": fake , 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
                    alpha.sendMessage(from, buffer, image, anu)
					await limitAdd(sender, limit)
					break   
			case 'megumin':
					if (!isNsfw) return reply(lang.nsfwmo())
					reply(lang.wait())
					anu = await fetchJson(`https://waifu.pics/api/sfw/megumin`)
					buffer = await getBuffer(anu.url)
					anu  = {contextInfo: {"forwardingScore":999,"isForwarded":true,'stanzaId': msgId, 'participant':`${numbernye}@s.whatsapp.net`, 'remoteJid': '6283136505591-1614953337@g.us', 'quotedMessage': {"locationMessage": {"degreesLatitude": 41.893714904785156, "degreesLongitude": -87.63370513916016, "name": fake , 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
                    alpha.sendMessage(from, buffer, image, anu)
					await limitAdd(sender, limit)
					break   
			case 'trapnime':
					if (!isNsfw) return reply(lang.nsfwmo())
					reply(lang.wait())
					anu = await fetchJson(`https://waifu.pics/api/nsfw/trap`)
					buffer = await getBuffer(anu.url)
					anu  = {contextInfo: {"forwardingScore":999,"isForwarded":true,'stanzaId': msgId, 'participant':`${numbernye}@s.whatsapp.net`, 'remoteJid': '6283136505591-1614953337@g.us', 'quotedMessage': {"locationMessage": {"degreesLatitude": 41.893714904785156, "degreesLongitude": -87.63370513916016, "name": fake , 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
                    alpha.sendMessage(from, buffer, image, anu)
					await limitAdd(sender, limit)
					break   

/*
]=====> GROUP MENU<=====[
*/
  
			case 'add':  
			if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
                    if (args.length < 1) return reply('Yang mau di add?')
					if (args[0].startsWith('08')) return reply('Gunakan kode negara mas')
					orang = args[0] + '@s.whatsapp.net'
response = await alpha.groupAdd(from, [orang])
o = response.participants[0]
let inv = (Object.values(o))
if(inv[0].code == 409) return reply('Orang yang anda add sudah ada didalam Group!')
else if(inv[0].code == 403){
alpha.sendMessage(from, `User private\n\nMengirim Undangan Group Ke @${q.split('@')[0]}`, MessageType.text, {quoted: mek, contextInfo: {mentionedJid: [orang]}})
alpha.sendMessage(from, orang, inv[0].invite_code, inv[0].invite_code_exp, groupMetadata.subject , `Salah Satu Admin Mengundang Anda Masuk Ke Sini Silahkan Klik Bergabung Untuk Masuk`)
}
					break 
					case 'radd': case 'addreply':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
if (mek.message.extendedTextMessage === undefined || mek.message.extendedTextMessage === null) return reply('Reply pesan yg ingin di Add!')
mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
mentions(`Perintah di terima, Add: @${mentioned[0].split('@')[0]}`, mentioned, true)
alpha.groupAdd(from, mentioned)
break

case 'kick':
if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
if(!q)return reply(`*Format salah!*\n\n*Example : ${prefix + command} @tag*`)
if (!isBotGroupAdmins) return reply(mess.only.Badmin)
yau = q.split('@')[1] + '@s.whatsapp.net'
alpha.groupRemove(from, [yau])
reply(`Succses kick target!`)
break

case 'rkick': case 'kickreply':
if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
if (mek.message.extendedTextMessage === undefined || mek.message.extendedTextMessage === null) return reply('Reply pesan yg ingin di Kick!')
mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid
mentions(`Perintah di terima, mengeluarkan : @${mentioned[0].split('@')[0]}`, mentioned, true)
alpha.groupRemove(from, mentioned)
break
			case 'antilink':
if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
              if (args[0] == 'on'){
              if (isAntiLink) return reply(lang.anjawaUdhOn(command))
              antilink.push(from)
              fs.writeFileSync('./src/antilink.json', JSON.stringify(antilink))
              reply(lang.anjawaOn(command))
              } else if (args[0] == 'off'){
              let anu = antilink.indexOf(from)
              antilink.splice(anu, 1)
              fs.writeFileSync('./src/antilink.json', JSON.stringify(antilink))
              reply(lang.anjawaOff(command))
              } else if (!q) {
          sendButMessage(from, `MODE ANTILINK`, `Choose one`, [
            {
              buttonId: 'antilink on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'antilink off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
       case 'event':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if ( !isOwner && !isCreator && !mek.key.fromMe) return reply("Khusus admin");
					if (args[0] == "on") {
						if (isEventon) return reply(lang.anjawaUdhOn(command))
						event.push(from)
						fs.writeFileSync('./src/event.json', JSON.stringify(event))
						reply(lang.anjawaOn(command))
					} else if (args[0] == "off") {
						event.splice(from, 1)
						fs.writeFileSync('./src/event.json', JSON.stringify(event))
						reply(lang.anjawaOff(command))
					} else if (!q) {
          sendButMessage(from, `MODE EVENT`, `Choose one`, [
            {
              buttonId: 'event on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'event off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
	case 'antivirtex':
 if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())       
        if (args[0] == "on") {
          if (isAntivirtex) return reply(lang.anjawaUdhOn(command))
          antivirtex.push(from);
          fs.writeFileSync(
            "./src/antivirtex.json",
            JSON.stringify(antivirtex)
          );
          reply(lang.anjawaOn(command))
        } else if (args[0] == "off") {
          antivirtex.splice(from, 1);
          fs.writeFileSync("./src/antivirtex.json", JSON.stringify(ant));
          reply(lang.anjawaOff(command))
        } else if (!q) {
          sendButMessage(from, `MODE ANTIVIRTEX`, `Choose one`, [
            {
              buttonId: 'antivirtex on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'antivirtex off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
			case 'admin':
					if (!isGroup) return fakestatus('```KHUSUS GRUP BRO```')
					teks = `*DAFTAR ATASAN GROUP* _${groupMetadata.subject}_\n*TOTAL* : ${groupAdmins.length}\n\n`
					no = 0
					for (let admon of groupAdmins) {
					no += 1
					teks += `[${no.toString()}] @${admon.split('@')[0]}\n`
					}
					mentions(teks, groupAdmins, true)
					break
			
			case 'tagall':
			if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					members_id = []
					teks = (args.length > 1) ? body.slice(8).trim() : ''
					teks += '\n\n'
					for (let mem of groupMembers) {
					teks += `@${mem.jid.split('@')[0]}\n`
					members_id.push(mem.jid)
					}
					mentions(teks, members_id, true)
					break

			case 'clearall':
			        if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
			        anu = await alpha.chats.all()
					alpha.setMaxListeners(10)
					for (let _ of anu) {
					alpha.deleteChat(_.jid)
					}
					reply(lang.success())
					break
            case 'out':
                    if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					setTimeout( () => {
					alpha.groupLeave (from) 
					}, 2000)
					setTimeout( () => {
					alpha.updatePresence(from, Presence.composing) 
					reply('```Byeee 👋```')
					}, 0)
					break     
		    case 'leave2':
                    if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					setTimeout( () => {
					alpha.groupLeave (q) 
					}, 2000)
					setTimeout( () => {
					alpha.updatePresence(from, Presence.composing) 
					fakestatus('```Ok gua out```')
					}, 0)
					break       
           case 'getpp':
				if (mek.message.extendedTextMessage != undefined){
					let mentioneddd = mek.message.extendedTextMessage.contextInfo.mentionedJid
					try {
						pic = await alpha.getProfilePicture(mentioneddd[0])
					} catch {
						pic = 'https://i.ibb.co/Tq7d7TZ/age-hananta-495-photo.png'
					}
					seeer = `Nama : *${pushname}`
					thumbb = await getBuffer(pic)
					anuu  = {contextInfo:{"forwardingScore":999,"isForwarded":true,'stanzaId': "B826873620DD5947E683E3ABE663F263", 'participant': `${numbernye}@s.whatsapp.net`, 'remoteJid': '6289523258649-1604595598@g.us', 'quotedMessage': {"imageMessage": {"caption": `「 Bot by zeeone 」`, 'jpegThumbnail': fs.readFileSync(`image/${thumbnail}`)}}}}
                    alpha.sendMessage(from, thumbb ,image, anuu)
				}
				await limitAdd(sender, limit)
					break   
	case 'inspect':
		            try {
		            if (!isUrl(args[0]) && !args[0].includes('whatsapp.com')) return reply(lang.erorLink())
		            if (!q) return reply('```Masukkan link groupnya```')
		            cos = args[0]
		            var net = cos.split('https://chat.whatsapp.com/')[1]
		            if (!net) return reply('pastikan itu link https://whatsapp.com/')
		            jids = []
		            let { id, owner, subject, subjectOwner, desc, descId, participants, size, descOwner, descTime, creation} = await alpha.query({ 
		            json: ["query", "invite",net],
		            expect200:true })
		            let par = `*Id* : ${id}
		${owner ? `*Owner* : @${owner.split('@')[0]}` : '*Owner* : -'}
		*Nama Gc* : ${subject}
		*Gc dibuat Tanggal* : ${formatDate(creation * 1000)}
		*Jumlah Member* : ${size}
		${desc ? `*Desc* : ${desc}` : '*Desc* : tidak ada'}
		*Id desc* : ${descId}
		${descOwner ? `*Desc diubah oleh* : @${descOwner.split('@')[0]}` : '*Desc diubah oleh* : -'}\n*Tanggal* : ${descTime ? `${formatDate(descTime * 1000)}` : '-'}\n\n*Kontak yang tersimpan*\n`
		           for ( let y of participants) {
		             par += `> @${y.id.split('@')[0]}\n*Admin* : ${y.isAdmin ? 'Ya' : 'Tidak'}\n`
		             jids.push(`${y.id.replace(/@c.us/g,'@s.whatsapp.net')}`)
		             }
		             jids.push(`${owner ? `${owner.replace(/@c.us/g,'@s.whatsapp.net')}` : '-'}`)
		             jids.push(`${descOwner ? `${descOwner.replace(/@c.us/g,'@s.whatsapp.net')}` : '-'}`)
		             alpha.sendMessage(from,par,text,{quoted:mek,contextInfo:{mentionedJid:jids}})
		             } catch {
		             reply(lang.erorLink())
		             }
		             await limitAdd(sender, limit)
					break   
			case 'return':
			case 'cek':
			case 'me':
			if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					return alpha.sendMessage(from, JSON.stringify(eval(args.join(' '))), text, { quoted: ftroli})
					break
			case 'bc':
			case 'broadcast':
			case 'bcimage':
					if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (args.length < 1) return reply('```TEXT?```')
					arg = args.join(' ');
					anu = await alpha.chats.all()
					if (isMedia && !mek.message.videoMessage || isQuotedImage) {
					const encmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek					
					bc = await alpha.downloadMediaMessage(encmedia)
					for (let _ of anu) {
					await alpha.sendMessage(_.jid, bc, image, {thumbnail: bc, quoted:fkontak ,caption: `「  *BROADCAST* 」\n\n${arg}`})
					}
					fakegroup(lang.successBc())
					} else {
					await ini_bc_pc_bang(arg)
					fakegroup(lang.successBc())
					}
					break
			case 'bcgc':
					case 'bcgroup':
					case 'bcgrup':
					case 'broadcastgrup':
		if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                    if (args.length < 1) return reply(`Untuk broadcast ke semua group ketik:\n${prefix}bcgroup [isi chat]`)
                    var group = await alpha.groupMetadata(from)
			ini_bc = args.join(' ')
		var groupz = await alpha.chats.all().filter(v => v.jid.endsWith('g.us'))
                    reply(`\`\`\`[ ! ]\`\`\` Broadcast in progress! Total: ${groupz.length} groups`)
                    await ini_bc_gc_bang(ini_bc)
					reply(`\`\`\`[ ✓ ] Success broadcast : ${groupz.length} groups\`\`\``)
					break
			case 'buggc':
			if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					await alpha.toggleDisappearingMessages(from, 0)
					break
			case 'infogc':
					alpha.updatePresence(from, Presence.composing)
					if (!isGroup) return reply(lang.onlygc())
					try {
					ppimg = await alpha.getProfilePicture(from)
					} catch {
						ppimg = 'https://i.ibb.co/NthF8ds/IMG-20201223-WA0740.jpg'
					}
					let buf = await getBuffer(ppimg)
					teks = (args.length > 1) ? body.slice(8).trim() : ''
					teks += `*Nama grup :* ${groupName}\n*Deskripsi :* ${groupDesc}\n*Jumlah Admin :* ${groupAdmins.length}\n*Jumlah Member :* ${groupMembers.length}`
					no = 0
					for (let admon of groupAdmins) {
						no += 1
						teks += `[${no.toString()}]`
					}
					alpha.sendMessage(from, buf, image, {quoted: mek, caption: teks})
					await limitAdd(sender, limit)
					break   
			
                case 'memegenerator': case 'memegen':
									if (!isPremium && !mek.key.fromMe) return reply(mess.only.prem)
									if (args.length < 1) return reply(`Kirim perintah *${prefix + command}* teks atas|teks bawah`)
									if (!q.includes('|')) return reply(`Kirim perintah *${prefix + command}* teks atas|teks bawah`)
									try {
										if (!isQuotedImage) return reply(`Reply Gambar!`)
										reply(lang.wait())
										var teks1 = q.split('|')[0] ? q.split('|')[0] : ''
										var teks2 = q.split('|')[1] ? q.split('|')[1] : ''
										var enmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
									   var mediiia = await alpha.downloadMediaMessage(enmedia)
										var njay = await uploadImages(mediiia)
										var resu = await getBuffer(`https://api.memegen.link/images/custom/${teks1}/${teks2}.png?background=${njay}`)
										alpha.sendMessage(from, resu, image, {caption:'.stiker', quoted: mek})
										fs.unlinkSync(media)
										} catch (e) {
											console.log(e)
										}
										limitAdd(sender, limit)
									break
					 	case 'stickermeme': case 'memesticker': case 'memestick': case 'stickmeme': case 'stcmeme': case 'smeme':
						if (args.length < 1) return reply(`Kirim perintah *${prefix + command}* Alphabot`)
									if (q.includes('|')) return reply(`Kirim perintah *${prefix + command}* Alphabot`)
									try {
										if (!isQuotedImage) return reply(`Reply Gambar!`)
										reply(lang.wait())
										var teks2 = args.join(' ')
										var enmedia = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo : mek
										var mediia = await alpha.downloadMediaMessage(enmedia)
										var njay = await uploadImages(mediia)
										var resu = await getBuffer(`https://api.memegen.link/images/custom/-/${teks2}.png?background=${njay}`)
										alpha.sendMessage(from, resu, image, {caption:'.stiker', quoted: mek})
										fs.unlinkSync(media)
										} catch (e) {
											console.log(e)
										}
										limitAdd(sender, limit)
									break
             case 'demoteall':
			if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
			                members_id = []
					for (let mem of groupMembers) {
				   	members_id.push(mem.jid)
				  	}
			                alpha.groupDemoteAdmin(from, members_id)
			                break
			case 'promoteall':
			if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
                members_id = []
					for (let mem of groupMembers) {
				   	members_id.push(mem.jid)
				  	}
                alpha.groupMakeAdmin(from, members_id)
                break
              case 'promote':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
					if (mek.message.extendedTextMessage === undefined || mek.message.extendedTextMessage === null) return
					mentionede = mek.message.extendedTextMessage.contextInfo.participant
alpha.groupMakeAdmin(from, [mentionede])
teks = `Members @${mentionede.split('@')[0]} succes promote`
alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid:[mentionede]}})
				case 'demote':
					if (!isGroup) return reply(lang.onlygc())
					if (!isGroupAdmins) return reply(lang.onlygcAdmin())
					if (!isBotGroupAdmins) return reply(lang.botNotAdm())
					if (mek.message.extendedTextMessage === undefined || mek.message.extendedTextMessage === null) return
					mentionede = mek.message.extendedTextMessage.contextInfo.participant
alpha.groupDemoteAdmin(from, [mentionede])
teks = `Members @${mentionede.split('@')[0]} succes demote`
alpha.sendMessage(from, teks, text, {quoted:mek, contextInfo:{mentionedJid:[mentionede]}})
break
                
                /*
                ]----------------------------------------------------------------> API ZEKS <----------------------------------------------------------------[
                */
case 'captain_as':case 'smoke':case 'tiktok2': case 'arcade':case 'battlefield':case 'pubg':
if (args.length < 1) return reply(lang.tahta(prefix, command))
var m = q
var m1 = m.split("|")[0];
var m2 = m.split("|")[1]; 
let photooxy = await getBuffer(`${alphaapi}/photooxy/${command}?apikey=${alphakey}&text=${m1}&text2=${m2}`)
sendButImage(from,  `Nih udah jadi kak, jgn lupa donasi untuk beli dyno 🐦 ` , `© ${ownername}`,photooxy, [{"buttonId": `owner`,"buttonText": {"displayText": "👨‍💻 Owner"},"type": "RESPONSE"},{"buttonId": `donasi`,"buttonText": {"displayText": "💰 Donate"},"type": "RESPONSE"}], {thumbnail: Buffer.alloc(0), quoted: mek})
await  limitAdd(sender, limit)
break                         
case 'shadow':case 'cname':case 'romantic':case 'burnpaper':case 'funnycup':case 'love':case 'undergrass':          case 'heart':case 'coffeecup':case 'wood':          case 'flower':         case 'wooden': case '3dsummer':case 'wolf_metal':case '3dnature':case 'underwater':          case 'goldenrose':case 'vector':case 'typography':case 'typography2': case 'underfall':  case 'smokyneon': case 'rainbow': case 'graffiti':case 'camouflage':case '3dglowing':          case 'vintage':case 'honey': case 'whitecube':case 'avatar': case 'glowrainbow':case 'nightsky':case 'fur':case 'flaming': case 'crisp':case 'embroidery':case 'bcake':          case '3dligth':case 'metallic': case 'striking':case 'watermelon':         case 'butterfly':case 'harry':case 'glowneon':case 'coffecup2':          case 'luxury': case 'cemetery':case 'woodblock':case 'sweet':case 'simple':case 'bevel':case 'underflower':         case 'underflower2':
if (args.length < 1) return reply(lang.noteks(prefix, command))
let photooxy2 = await getBuffer(`${alphaapi}/photooxy/${command}?apikey=${alphakey}&text=${q}`)
sendButImage(from,  `Nih udah jadi kak, jgn lupa donasi untuk beli dyno 🐦 ` , `© ${ownername}`,photooxy2, [{"buttonId": `owner`,"buttonText": {"displayText": "👨‍💻 Owner"},"type": "RESPONSE"},{"buttonId": `donasi`,"buttonText": {"displayText": "💰 Donate"},"type": "RESPONSE"}], {thumbnail: Buffer.alloc(0), quoted: mek})
await  limitAdd(sender, limit)
break         
case 'halloween2':case 'horror':case 'game8bit':case 'layered':case 'glitch2':case 'coolg':case 'coolwg':case 'realistic':case 'space3d':case 'gtiktok':case 'stone':case 'marvel':case 'marvel2':case 'pornhub':case 'avengers':case 'metalr':case 'metalg':case 'metalg2':case 'halloween2':case 'lion':case 'wolf_bw':case 'wolf_g':case 'ninja':case '3dsteel':case 'horror2':case 'lava':case 'bagel':
if (args.length < 1) return reply(lang.tahta(prefix, command))
var m = q
var m1 = m.split("|")[0];
var m2 = m.split("|")[1]; 
let textpro = await getBuffer(`${alphaapi}/textpro/${command}?apikey=${alphakey}&text=${m1}&text2=${m2}`)
sendButImage(from,  `Nih udah jadi kak, jgn lupa donasi untuk beli dyno 🐦 ` , `© ${ownername}`,textpro, [{"buttonId": `owner`,"buttonText": {"displayText": "👨‍💻 Owner"},"type": "RESPONSE"},{"buttonId": `donasi`,"buttonText": {"displayText": "💰 Donate"},"type": "RESPONSE"}], {thumbnail: Buffer.alloc(0), quoted: mek})
await  limitAdd(sender, limit)
break 
 case 'blackpink':case 'rainbow2':case 'water_pipe':case 'halloween':case 'sketch':case 'sircuit':case 'discovery':case 'metallic2':case 'fiction':case 'demon':case 'transformer':case 'berry':case 'thunder':case 'magma':case '3dstone':case 'neon':case 'glitch':case 'harry_potter':case 'embossed':case 'broken':case 'papercut':case 'gradient':case 'glossy':case 'watercolor':case 'multicolor':case 'neon_devil':case 'underwater':case 'bear':case 'wonderfulg':case 'christmas':case 'neon_light':case 'snow':case 'cloudsky':case 'luxury2':case 'gradient2':case 'summer':case 'writing':case 'engraved':case 'summery':case '3dglue':case 'metaldark':case 'neonlight':case 'oscar':case 'minion':case 'holographic':case 'purple':case 'glossyb':case 'deluxe2':case 'glossyc':case 'fabric':case 'neonc':case 'newyear':case 'newyear2':case 'metals':case 'xmas':case 'blood':case 'darkg':case 'joker':case 'wicker':case 'natural':case 'firework':case 'skeleton':case 'balloon':case 'balloon2':case 'balloon3':case 'balloon4':case 'balloon5':case 'balloon6':case 'balloon7':case 'steel':case 'gloss':case 'denim':case 'decorate':case 'decorate2':case 'peridot':case 'rock':case 'glass':case 'glass2':case 'glass3':case 'glass4':case 'glass5':case 'glass6':case 'glass7':case 'glass8':case 'captain_as2':case 'robot':case 'equalizer':case 'toxic':case 'sparkling':case 'sparkling2':case 'sparkling3':case 'sparkling4':case 'sparkling5':case 'sparkling6':case 'sparkling7':case 'decorative':case 'chocolate':case 'strawberry':case 'koifish':case 'bread':case 'matrix':case 'blood2':case 'neonligth2':case 'thunder2':case '3dbox':case 'neon2':case 'roadw':case 'bokeh':case 'gneon':case 'advanced':case 'dropwater':case 'wall':case 'chrismast':case 'honey':case 'drug':case 'marble':case 'marble2':case 'ice':case 'juice':case 'rusty':case 'abstra':case 'biscuit':case 'wood':case 'scifi':case 'metalr':case 'purpleg':case 'shiny': case 'jewelry':case 'jewelry2':case 'jewelry3':case 'jewelry4':case 'jewelry5':case 'jewelry6':case 'jewelry7':case 'jewelry8':case 'metalh':case 'golden':case 'glitter':case 'glitter2':case 'glitter3':case 'glitter4':case 'glitter5':case 'glitter6':case 'glitter7':case 'metale':case 'carbon':case 'candy':case 'metalb':case 'gemb':case '3dchrome':case 'metalb2':case 'metalg':
if (args.length < 1) return reply(lang.noteks(prefix, command))
let textpro2 = await getBuffer(`${alphaapi}/textpro/${command}?apikey=${alphakey}&text=${q}`)
sendButImage(from,  `Nih udah jadi kak, jgn lupa donasi untuk beli dyno 🐦 ` , `© ${ownername}`,textpro2, [{"buttonId": `owner`,"buttonText": {"displayText": "👨‍💻 Owner"},"type": "RESPONSE"},{"buttonId": `donasi`,"buttonText": {"displayText": "💰 Donate"},"type": "RESPONSE"}], {thumbnail: Buffer.alloc(0), quoted: mek})
await  limitAdd(sender, limit)
break  
case 'tahta':  
                   if (args.length < 1) return reply(lang.noteks(prefix, command))
                   F = q
                   reply(lang.wait())
                   tahta = await getBuffer(`${ApiZeks}/api/hartatahta?text=${F}&apikey=${zeksApikey}`)
                   tahtah = `${lang.success()}\n\nPlease Subscribe https://youtu.be/w4iQ4rwA0mo`
                   sendImageMaker(tahta, tahtah, sender)
                  await  limitAdd(sender, limit)
                  break
       case 'ytgold':  
                   if (args.length < 1) return reply(lang.noteks(prefix, command))
                   F = q
                   reply(lang.wait())
                   ytgold = await getBuffer(`${ApiZeks}/api/gplaybutton?text=${F}&apikey=${zeksApikey}`)
                   ytgoldp = `${lang.success()}\n\nPlease Subscribe https://youtu.be/w4iQ4rwA0mo`
                   sendImageMaker(ytgold, ytgoldp, sender)
                   await  limitAdd(sender, limit)
                   break  
       case 'ytsilver':  
                   if (args.length < 1) return reply(lang.noteks(prefix, command))
                   F = q
                   reply(lang.wait())
                   ytsilver = await getBuffer(`${ApiZeks}/api/splaybutton?text=${F}&apikey=${zeksApikey}`)
                   ytsilverp = `${lang.success()}\n\nPlease Subscribe https://youtu.be/w4iQ4rwA0mo`
                   sendImageMaker(ytsilver, ytsilverp, sender)
                   await  limitAdd(sender, limit)
                   break              
     case 'nulis':
									reply(`*Example*\n${prefix}nuliskiri\n${prefix}nuliskanan\n${prefix}foliokiri\n${prefix}foliokanan`)
									break
						case 'nuliskiri':{
									if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return reply (`Limit kamu sudah habis silahkan kirim ${prefix}limit untuk mengecek limit`)
									if (args.length < 1) return reply(`Kirim perintah *${prefix}nuliskiri* teks`)
									reply(mess.wait)
									const tulisan = q
									const splitText = tulisan.replace(/(\S+\s*){1,9}/g, '$&\n')
									const fixHeight = splitText.split('\n').slice(0, 31).join('\n')
									spawn('convert', [
									'./media/nulis/images/buku/sebelumkiri.jpg',
									'-font',
									'./media/nulis/font/Indie-Flower.ttf',
									'-size',
									'960x1280',
									'-pointsize',
									'22',
									'-interline-spacing',
									'2',
									'-annotate',
									'+140+153',
									fixHeight,
									'./media/nulis/images/buku/setelahkiri.jpg'
									])
									.on('error', () => reply(lang.tryAgain()))
									.on('exit', () => {
										alpha.sendMessage(from, fs.readFileSync('./media/nulis/images/buku/setelahkiri.jpg'), image, {thumbnail:Buffer.alloc(0),quoted: mek, caption: `Jangan Malas`})
										limitAdd(sender, limit)
										})
									}
									break
						case 'nuliskanan':{
									if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return reply (`Limit kamu sudah habis silahkan kirim ${prefix}limit untuk mengecek limit`)
									if (args.length < 1) return reply(`Kirim perintah *${prefix}nuliskanan* teks`)
									reply(mess.wait)
									const tulisan = q
									const splitText = tulisan.replace(/(\S+\s*){1,9}/g, '$&\n')
									const fixHeight = splitText.split('\n').slice(0, 31).join('\n')
									spawn('convert', [
									'./media/nulis/images/buku/sebelumkanan.jpg',
									'-font',
									'./media/nulis/font/Indie-Flower.ttf',
									'-size',
									'960x1280',
									'-pointsize',
									'23',
									'-interline-spacing',
									'2',
									'-annotate',
									'+128+129',
									fixHeight,
									'./media/nulis/images/buku/setelahkanan.jpg'
									])
									.on('error', () => reply(lang.tryAgain()))
									.on('exit', () => {
										alpha.sendMessage(from, fs.readFileSync('./media/nulis/images/buku/setelahkanan.jpg'), image, {thumbnail:Buffer.alloc(0),quoted: mek, caption: `Jangan Malas`})
										limitAdd(sender, limit)
										})
									}
									break
						case 'foliokiri':{
									if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return reply (`Limit kamu sudah habis silahkan kirim ${prefix}limit untuk mengecek limit`)
									if (args.length < 1) return reply(`Kirim perintah *${prefix}foliokiri* teks`)
									reply(mess.wait)
									const tulisan = q
									const splitText = tulisan.replace(/(\S+\s*){1,13}/g, '$&\n')
									const fixHeight = splitText.split('\n').slice(0, 38).join('\n')
									spawn('convert', [
									'./media/nulis/images/folio/sebelumkiri.jpg',
									'-font',
									'./media/nulis/font/Indie-Flower.ttf',
									'-size',
									'1720x1280',
									'-pointsize',
									'23',
									'-interline-spacing',
									'4',
									'-annotate',
									'+48+185',
									fixHeight,
									'./media/nulis/images/folio/setelahkiri.jpg'
									])
									.on('error', () => reply(lang.tryAgain()))
									.on('exit', () => {
										alpha.sendMessage(from, fs.readFileSync('./media/nulis/images/folio/setelahkiri.jpg'), image, {thumbnail:Buffer.alloc(0),quoted: mek, caption: `Jangan Malas`})
										limitAdd(sender, limit)
										})
									}
									break
						case 'foliokanan':{
									if (isLimit(sender, isPremium, isCreator, isOwner, limitawal, limit)) return reply (`Limit kamu sudah habis silahkan kirim ${prefix}limit untuk mengecek limit`)
									if (args.length < 1) return reply(`Kirim perintah *${prefix}foliokanan* teks`)
									reply(mess.wait)
									const tulisan = q
									const splitText = tulisan.replace(/(\S+\s*){1,13}/g, '$&\n')
									const fixHeight = splitText.split('\n').slice(0, 38).join('\n')
									spawn('convert', [
									'./media/nulis/images/folio/sebelumkanan.jpg',
									'-font',
									'./media/nulis/font/Indie-Flower.ttf',
									'-size',
									'960x1280',
									'-pointsize',
									'23',
									'-interline-spacing',
									'3',
									'-annotate',
									'+89+190',
									fixHeight,
									'./media/nulis/images/folio/setelahkanan.jpg'
									])
									.on('error', () => reply(mess.error))
									.on('exit', () => {
										alpha.sendMessage(from, fs.readFileSync('./media/nulis/images/folio/setelahkanan.jpg'), image, {thumbnail:Buffer.alloc(0),quoted: mek, caption: `Jangan Malas`})
										limitAdd(sender, limit)
									})
									}
									break      
       case 'ttp':  
                    if (args.length < 1) return reply(lang.noteks(prefix, command))
                    ttp = args.join(' ')
                    anu1 = await getBuffer(`https://api.xteam.xyz/ttp?file&text=${ttp}`)
                    alpha.sendMessage(from, anu1, image, {quoted: mek, caption : '.stikerin'})
					.catch(e =>{
					reply(lang.tryAgain())})
					 await limitAdd(sender, limit)
					break   
         case 'attp':  
                    if (args.length < 1) return reply(lang.noteks(prefix, command))
                    hhhh = q
                    anu1 = await getBuffer(`https://api.xteam.xyz/attp?file&text=${hhhh}`)
                    alpha.sendMessage(from, anu1, sticker, {quoted: mek})
					.catch(e =>{
					reply(lang.tryAgain())})
                    await limitAdd(sender, limit)
					break   
             case 'ktpmaker': case 'ktp':
                    if (args.length == 0) return reply(`Usage: ${prefix + command} nik|provinsi|kabupaten|nama|tempat, tanggal lahir|jenis kelamin|jalan|rt/rw|kelurahan|kecamatan|agama|status nikah|pekerjaan|warga negara|berlaku sampai|url_image\n\nExample: ${prefix + command} 456127893132123|bumipertiwi|fatamorgana|LoL Human|mars, 99-99-9999|belum ditemukan|jl wardoyo|999/999|turese|imtuni|alhamdulillah islam|jomblo kack|mikirin dia|indo ori no kw|hari kiamat|https://i.ibb.co/Xb2pZ88/test.jpg`)
                    get_args = args.join(" ").split("|")
                    nik = get_args[0]
                    prov = get_args[1]
                    kabu = get_args[2]
                    name = get_args[3]
                    ttl = get_args[4]
                    jk = get_args[5]
                    jl = get_args[6]
                    rtrw = get_args[7]
                    lurah = get_args[8]
                    camat = get_args[9]
                    agama = get_args[10]
                    nikah = get_args[11]
                    kerja = get_args[12]
                    warga = get_args[13]
                    until = get_args[14]
                    img = get_args[15]
                    reply('waitt')
                    bikin = (`https://ferdiz-afk.my.id//api/maker/ktp?nik=${nik}&nama=${name}&ttl=${ttl}&jk=${jk}&gdarah=-&almt=${jl}&rt-rw=${rtrw}&kel=${lurah}&kcmtn=${camat}&agma=${agama}&status=${nikah}&kerja=${kerja}&negara=${warga}&berlaku=${until}&prov=${prov}&kab=${kabu}&picurl=${img}`)
                      console.log(bikin)
                    imge = await getBuffer(bikin)
                    await alpha.sendMessage(from, imge, image, { thumbnail: Buffer.alloc(0), quoted: mek });
                    await limitAdd(sender, limit)
					break 
        case 'nulis2':
if (args.length < 1) return reply(`*Usage*: ${prefix + command} nama&kelas&nomor&kata\n*Example*: ${prefix + command} udin&20&17&blablabla`)
var bodi = args.join(" ")
var nama = bodi.split("&")[0];
var kelas = bodi.split("&")[1];
var no = bodi.split("&")[2];
var aksarane = bodi.split("&")[3];
reply('membuat bos...')
                 rakz = await getBuffer(`https://ferdiz-afk.my.id//api/tulis?nama=${nama}&no=${no}&kelas=${kelas}&text=${aksarane}`)
                 alpha.sendMessage(from, rakz, image, { quoted: mek ,thumbnail: Buffer.alloc(0) });
await limitAdd(sender, limit)
					break   
              /*
              ]----------------------------------------------------------------> STORAGE <----------------------------------------------------------------[
              */
	        case 'addstik':
	if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!isQuotedSticker) return fakestatus('Reply stiker')
					nm = body.slice(9)
					if (!nm) return reply('Nama sticker nya apa?')
					boij = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
					delb = await alpha.downloadMediaMessage(boij)
					setiker.push(`${nm}`)
					fs.writeFileSync(`./media/sticker/${nm}.webp`, delb)
					fs.writeFileSync('./temp/stik.json', JSON.stringify(setiker))
					fakegroup(`Sukses Menambahkan Sticker\nCek dengan cara ${prefix}liststik`)
					break
	      case 'liststik':
	      case 'liststiker':
	      case 'liststc':
					teks = '*Sticker list :*\n\n'
					for (let awokwkwk of setiker) {
						teks += `- ${awokwkwk}\n`
					}
					teks += `\n*Total : ${setiker.length}*\n\n_Untuk mengambil sticker silahkan reply pesan ini dengan caption nama sticker_`
					alpha.sendMessage(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": setiker } })
					break
					
			case 'addimg':
			if(!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!isQuotedImage) return fakegroup('```Reply imagenya```')
					clara = body.slice(8)
					if (!clara) return fakegroup('```Nama imagenya apa```')
					keya = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
					delb = await alpha.downloadMediaMessage(keya)
					imagenye.push(`${svst}`)
					fs.writeFileSync(`./media/foto/${svst}.jpg`, delb)
					fs.writeFileSync('./temp/image.json', JSON.stringify(imagenye))
					fakegroup(`Sukses Menambahkan image\nCek dengan cara ${prefix}listimg`)
					break
			case 'listimg':
					teks = '*Image list :*\n\n'
					for (let awokwkwk of imagenye) {
						teks += `- ${awokwkwk}\n`
					}
					teks += `\n*Total : ${imagenye.length}*\n\n_Untuk mengambil sticker silahkan reply pesan ini dengan caption nama foto/image_`
					fakegroup(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": setiker } })
					break
			case 'addvid':
			if(!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!isQuotedVideo) return fakegroup('```Reply vidionya```')
					svst = body.slice(8)
					if (!svst) return fakegroup('```Nama vidionya apa```')
					keya = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
					delb = await alpha.downloadMediaMessage(keya)
					imagenye.push(`${svst}`)
					fs.writeFileSync(`./media/video/${svst}.mp4`, delb)
					fs.writeFileSync('./temp/video.json', JSON.stringify(imagenye))
					fakegroup(`Sukses Menambahkan video\nCek dengan cara ${prefix}listvideo`)
					break
	        case 'listvid':
					teks = '*List Video :*\n\n'
					for (let awokwkwk of videonye) {
						teks += `- ${awokwkwk}\n`
					}
					teks += `\n*Total : ${videonye.length}* \n\n_Untuk mengambil sticker silahkan reply pesan ini dengan caption nama video_`
					alpha.sendMessage(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": imagenye } })
					break
			
			case 'addvn':
			if(!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if (!isQuotedAudio) return fakegroup('```Reply vnnya```')
					svst = body.slice(7)
					if (!svst) return reply('```Nama audionya apa```')
					keya = JSON.parse(JSON.stringify(mek).replace('quotedM', 'm')).message.extendedTextMessage.contextInfo
					delb = await alpha.downloadMediaMessage(keya)
					audionye.push(`${svst}`)
					fs.writeFileSync(`./media/audio/${svst}.mp3`, delb)
					fs.writeFileSync('./temp/vn.json', JSON.stringify(audionye))
					fakegroup( `Sukses Menambahkan Audio\nCek dengan cara ${prefix}listvn`)
					break
           
			case 'listvn':
					teks = '*List Vn:*\n\n'
					for (let awokwkwk of audionye) {
						teks += `- ${awokwkwk}\n`
					}
					teks += `\n*Total : ${audionye.length}*\n\n_Untuk mengambil sticker silahkan reply pesan ini dengan caption nama audio_`
					alpha.sendMessage(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": audionye } })
					break
			case 'addrespon':
			if(!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					if(!q) return reply(`ketik perintah ${prefix + command} filter|jawab!`)
					fltr = q.split('|')[0]
					jwb = q.split('|')[1]
					if(!jwb) return reply('Format salah!')
					for(let i of filter){
					  if(fltr.includes(i.Filter)) return reply(`Filter ${fltr} sudah ada didatabase`)
					}
					const chat = {
					Filter : fltr,
					Jawaban : jwb
					}
					filter.push(chat)
					fs.writeFileSync('./src/filter.json', JSON.stringify(filter))
					reply(`Sukses menambahkan filter ${fltr}\nCek dengan cara ${prefix}listchatrespon`)
					break
					
			case 'delrespon':
			if(!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
					for(let i=0; i<filter.length; i++){
					if(q.includes(filter[i].Filter)){
					   obj = {
					      txt: filter[i].text,
					      balesan: filter[i].balesan
					   }
					   let del = filter.indexOf(filter[i])
					   filter.splice(del, 1)
					   fs.writeFileSync('./src/filter.json', JSON.stringify(filter))
					   reply(`Sukses Menghapus Respon ${q}`)
					}
					}
					break
case 'listrespon':
   teks = 'List Respon:\n'
   for (let i of filter) {
   teks += `• Filter : ${i.Filter}\n• Jawab : ${i.Jawaban}\n---------------------------\n`
   }
   teks += `Total : ${filter.length}`
   alpha.sendMessage(from, teks.trim(), extendedText, {quoted: mek})
   break
		case 'caripesan2':
            if(!q)return reply('Masukkan pesan yg mau di cari')
            let v = await alpha.searchMessages(q,from,10,1)
            let s = v.messages
            let el = s.filter(v => v.message)
            el.shift()
            try {
            if(el[0].message.conversation == undefined) return
            reply(`Ditemukan ${el.length} pesan`)
            await sleep(3000)
            for(let i = 0; i < el.length; i++) {
            await alpha.sendMessage(from,'Nih pesannya',text,{quoted:el[i]})
            }
            } catch(e){
            reply('Pesan tidak ditemukan!')
            }           
            await limitAdd(sender, limit)
					break   
     case 'searchmsg':  
             if (args.length < 1) return reply(`Pesan Yang Mau Dicari Apaan?\nContoh : ${command} halo|10`)
             teks = arg
             if (teks.includes("|")) { 
             try {
             var ve = teks.split("|")[0]
             var za = teks.split("|")[1]
             sampai = `${za}`
             if (isNaN(sampai)) return reply('Harus berupa Angka!')
             batas = parseInt(sampai) + 1
             if (batas > 20) return reply('Maks 20!')
             reply(lang.wait())
             cok = await nino.searchMessages(`${ve}`, from, batas,1) 
             if (cok.messages.length < 2) return reply('Tidak Ditemukan Pesan') 
             if (cok.messages.length < parseInt(batas)) reply(`Hanya Ditemukan ${cok.messages.length - 1} Pesan`)
             for (i=1;i < cok.messages.length;i++) {
             if (cok.messages[i].message) {
             alpha.sendMessage(from, `Ditemukan!`, text, {sendEphemeral: true, quoted: cok.messages[i]}) 
			}
			}
             } catch (e) {
             return reply(String(e))}
             } else {
             reply(`Format salah, format yang benar : ${command} halo|10`)}
             await limitAdd(sender, limit)
					break   
    case 'hash':
                try {
                if (!isQuotedSticker) return reply('Reply Sticker!')
                const encmeds = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
                const mediastick = await alpha.downloadMediaMessage(encmeds)
                var crypto = require('crypto')
                hash = crypto.createHash('sha256').update(mediastick).digest('base64');
                console.log(hash)
                reply (hash)
                } catch {
                	reply(`reply stiker dengan command ${prefix}hash`)
	}
            await limitAdd(sender, limit)
					break   
    case 'delvote':
            if(!isGroupAdmins && !mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlygcAdmin())
            if(isVote) return reply(lang.noSesiVote())
            delVote(from)
            reply(lang.suksesDelVot())
            break
    case 'voting':
            if(!isGroupAdmins && !mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlygcAdmin())
            if(!isGroup) return reply(lang.onlygc())
            if (isVote) return reply(lang.adaVoting())
            if(!q) return reply(lang.caraVoting())
            if (mek.message.extendedTextMessage.contextInfo.mentionedJid.length > 0 || mek.message.extendedTextMessage.contextInfo == null) {
            let id = mek.message.extendedTextMessage.contextInfo.mentionedJid[0]
            split = args.join(' ').replace('@', '').split('|')
            if(!Number(split[2])) return reply(lang.caraVot(prefix, command))
            await mentions('Vote ' +'@'+ id.split('@')[0]+' Di Mulai' +'\n\n' + `vote = ✅\ndevote = ❌\n\nAlasan: ${split[1]}`,[id],true)
            addVote(from,split[1],split[0],split[2],reply)
            }
            await limitAdd(sender, limit)
					break   
		case 'stopjadibot':
			    if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
			    stopjadibot(reply)
			    break
		case 'listbot':
			    let tekss = '「 *LIST JADIBOT* 」\n'
			    for(let i of listjadibot) {
			    tekss += `*Number* : ${i.jid.split('@')[0]}
*Name* : ${i.name}
*Device* : ${i.phone.device_manufacturer}
*Model* : ${i.phone.device_model}\n\n`
			    }
			    reply(tekss)
		case 'addcmd': 
       case 'setcmd':
              if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
              if (isQuotedSticker) {
              if (!q) return reply(`Penggunaan : ${command} cmdnya dan tag stickernya`)
              var kodenya = mek.message.extendedTextMessage.contextInfo.quotedMessage.stickerMessage.fileSha256.toString('base64')
              addCmd(kodenya, q)
              reply(lang.success())
              } else {
              reply('Reply stickenya')
}
              break
       case 'delcmd':
              if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
              if (!isQuotedSticker) return reply(`Penggunaan : ${command} tagsticker`)
              var kodenya = mek.message.extendedTextMessage.contextInfo.quotedMessage.stickerMessage.fileSha256.toString('base64')
            scommand.splice(getCommandPosition(kodenya), 1)
              fs.writeFileSync('./lib/scommand.json', JSON.stringify(scommand))
              reply(lang.success())
              break
       case 'listcmd':
              let teksnyee = `「 *LIST STICKER CMD* 」`
              let cemde = [];
              for (let i of scommand) {
              cemde.push(i.id)
              teksnyee += `\n\n📍 *ID :* ${i.id}\n📍 *Cmd* : ${i.chats}`
}
              mentions(teksnyee, cemde, true)
              break
      case 'delttt':
case 'delttc':
               if (!isGroup) return reply(lang.onlygc())
              // if (!isTTT) return reply('Tidak Ada Permainan Di Grub Ini')
               delete tictactoe[senderNumber]
fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))                        
if (fs.existsSync('./temp/' + from + '.json')) {
fs.unlinkSync('./temp/' + from + '.json')
reply(lang.suksesDelTtt())
}
               break
        case 'cekhistory':
reply(` S T A T U S  T I C T A C T O E ${enter}•> Win : ${checkWin(sender)}${enter}•> Lose : ${checkLose(sender)}`)
break
case 'delsesi':
if (!isGroupAdmins && !mek.key.fromMe && !isOwner) return reply('Hanya bisa didelete oleh admin group dan owner bot')
if (args[0] === 'ttt') {
delete tictactoe[senderNumber]
fs.writeFileSync("./src/tictactoe.json", JSON.stringify(tictactoe))                        
if (fs.existsSync('./temp/' + from + '.json')) {
fs.unlinkSync('./temp/' + from + '.json')
reply(lang.noSesiTtt())
}
} else if (args[0] === 'vote') {
if(isVote) return reply(lang.noSesiVote())
            delVote(from)
            reply(lang.sukseDelVot())
} else {
	reply(`${prefix + command}delsesi ttt atau vote`)
}
break

case 'tictactoe':
case 'ttt':
if (!isGroup)return reply('*Khusus group*')
if (mek.message.extendedTextMessage.contextInfo.mentionedJid > 1) return reply('Hanya bisa dengan 1 orang')
if (!mek.message.extendedTextMessage.contextInfo.mentionedJid[0]) return
if (fs.existsSync(`./temp/${from}.json`)) return reply(`Sedang Ada Sesi, tidak dapat dijalankan secara bersamaan${enter}Ketik *${prefix}delsesi ttt*, untuk menghapus sesi`)
if (args.length === 0) return reply(`Tag Lawan Yang Ingin Diajak Bermain Game`)
tttSkuy = setTtt(`${from}`)
tttSkuy.status = false
tttSkuy.Z = sender.replace("@s.whatsapp.net", "")
tttSkuy.Y = args[0].replace("@", "");
fs.writeFileSync(`./temp/${from}.json`, JSON.stringify(tttSkuy, null, 2))
starGame = `「 *MEMULAI GAME TICTACTOE* 」${enter}${enter}•@${sender.replace("@s.whatsapp.net", "")} Menantang Bermain Tictactoe ${enter}[ ${args[0]} ] Ketik Y/N untuk menerima atau menolak permainan${enter}${enter}`
alpha.sendMessage(from, starGame, text, {quoted: mek, contextInfo: { mentionedJid: [sender, args[0].replace("@", "") + "@s.whatsapp.net"],}})
await limitAdd(sender, limit)
					break   
             case 'size':
if (args.length < 1) return reply('Masukan angkanya')
filsize = args[0]
costick = await alpha.prepareMessageFromContent(from,{
"stickerMessage": {
"url": m.quoted.url,
"fileSha256": m.quoted.fileSha256.toString('base64'),
"fileEncSha256": m.quoted.fileEncSha256.toString('base64'),
"mediaKey": m.quoted.mediaKey.toString('base64'),
"mimetype": m.quoted.mimetype,
"height": m.quoted.height,
"width": m.quoted.width,
"directPath": m.quoted.directPath,
"fileLength": filsize,
"mediaKeyTimestamp": m.quoted.mediaKeyTimestamp.low,
"isAnimated": m.quoted.isAnimated
}
}, {quoted:mek})
alpha.relayWAMessage(costick)
await limitAdd(sender, limit)
					break   
case "colongsw": 
        if (!mek.key.fromMe) return
        if ((isMedia && !mek.message.videoMessage) || isQuotedImage) {
          ger = isQuotedImage
            ? JSON.parse(JSON.stringify(mek).replace("quotedM", "m")).message
                .extendedTextMessage.contextInfo
            : mek;
          owgi = await alpha.downloadAndSaveMediaMessage(ger);
          alpha.sendMessage(sender, fs.readFileSync(owgi), "imageMessage", {
            caption: q,
          });
          reply("Sukses");
          fs.unlinkSync(owgi);
        } else if ((isMedia && !mek.message.videoMessage) || isQuotedVideo) {
          ger = isQuotedVideo
            ? JSON.parse(JSON.stringify(mek).replace("quotedM", "m")).message
                .extendedTextMessage.contextInfo
            : mek;
          owgi = await alpha.downloadAndSaveMediaMessage(ger);
          alpha.sendMessage(sender, fs.readFileSync(owgi), "videoMessage", {
            caption: q,
          });
          reply("Sukses");
          fs.unlinkSync(owgi);
        } else {
          reply("Reply sw foto / video yg mau dicolong")
        }
        break
       case "listonline": 
        let id = args && /\d+\-\d+@g.us/.test(args[0]) ? args[0] : m.chat;
        try {
          let online = [
            ...Object.keys(alpha.chats.get(id).presences),
            alpha.user.jid,
          ];
          alpha.reply(
            m.chat,
            "「 L I S T   O N L I N E  」\n" +
              online.map((v) => "├ @" + v.replace(/@.+/, "")).join`\n` +
              "\n",
            m,
            {
              contextInfo: { mentionedJid: online },
            }
          );
        } catch (e) {
          m.reply("");
        }
        break;
        case 'getname':
 try {
getnem = alpha.getName(mek.quoted.sender)
reply(`${getnem}`)
} catch {
	reply ('Reply pesan @user')
	}
await limitAdd(sender, limit)
					break   
case 'linkgrup':
case 'linkgroup':
				case 'linkgc':
				    if (!isGroup) return reply(lang.onlygc())
				    if (!isBotGroupAdmins) return reply('Only admin')
				    linkgc = await alpha.groupInviteCode (from)
				    yeh = `https://chat.whatsapp.com/${linkgc}\n\nlink Group *${groupName}*`
				    alpha.sendMessage(from, yeh, text, {quoted: mek})
			        await limitAdd(sender, limit)
					break   
		case 'unpin':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                alpha.modifyChat(from, ChatModification.unpin)
                reply('*succes unpin this chat*')
                console.log('unpin chat = ' + from)
                break
        case 'pin':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                alpha.modifyChat(from, ChatModification.pin)
                reply('*succes pin this chat*')
                console.log('pinned chat = ' + from)
                break
         case 'unreadall':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                var chats = await alpha.chats.all()
                chats.map( async ({ jid }) => {
                await alpha.chatRead(jid, 'unread')
                    })
		    reply(`\`\`\`Successfully unread ${chats.length} chats !\`\`\``)
		    console.log(chats.length)
	        break
	        
            case 'readall':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                var chats = await alpha.chats.all()
                chats.map( async ({ jid }) => {
                await alpha.chatRead(jid)
                })
		reply(`\`\`\`Successfully read ${chats.length} chats !\`\`\``)
	      console.log(chats.length)
		break
		case 'unarchiveall':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                reply('*succes unarchive all chat*')
                console.log('succes unarchive chat = ' + from)
                anu = await alpha.chats.all()
                for (let _ of anu) {
                alpha.modifyChat(_.jid, ChatModification.unarchive)
                }
                break
                
            case 'archive':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                reply(lang.wait())
                console.log('succes archive chat = ' + from)
                await sleep(3000)
                alpha.modifyChat(from, ChatModification.archive)
                break
           case 'delthischat':
                if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
                console.log('succes delete chat = ' + from)
                await sleep(4000)
                await alpha.modifyChat(from, ChatModification.delete)
                reply('*succes delete this chat*')
                break
            case 'ssweb':
            case 'ss':
                if (args.length < 1) return reply('Urlnya mana om')
					teks = q
					anu = await fetchJson(`https://shot.screenshotapi.net/screenshot?&url=${teks}`)
					buff = await getBuffer(anu.screenshot)
					alpha.sendMessage(from, buff, image, {quoted: mek, caption : teks})
					await limitAdd(sender, limit)
					break   
			case 'artinama':
                if (args.length < 1) return reply('Apa yang mau dicari um?')
                teks = q
					anu = await fetchJson(`https://mnazria.herokuapp.com/api/arti?nama=${teks}`, {method: 'get'})
					reply(`Arti Nama ${teks}\n\n`+anu.result)
				await limitAdd(sender, limit)
					break   
			case 'halah': case 'hilih': case 'huluh': case 'heleh': case 'holoh':
ter = command[1].toLowerCase()
  tex = m.quoted ? m.quoted.text ? m.quoted.text : q ? q : m.text : q ? q : m.text
 reply(tex.replace(/[aiueo]/g, ter).replace(/[AIUEO]/g, ter.toUpperCase()))
 await limitAdd(sender, limit)
					break   
 case 'getexif':
try {
    if (!m.quoted) return reply('Tag stikernya!')
    cok = { message: { [m.quoted.mtype]: m.quoted } }
    if (/sticker/.test(m.quoted.mtype)) {
        let img = new webp.Image()
        await img.loadBuffer(await m.quoted.download())
        reply(util.format(JSON.parse(img.exif.slice(22).toString())))
    }
    } catch (e) {
    	throw e
    reply(String(e))
    }
    await limitAdd(sender, limit)
					break   
		case 'afk':
			if (!isGroup) return reply(lang.onlygc())
            if (isAfkOn) return 
                reason = q ? q : 'Nothing'
                off.addAfkUser(sender, Date.now(), reason, _off)
               papa =  `*${pushname}* Sekarang sedang Afk\n*Reason :* ${reason}\n`
                alpha.sendMessage(from,papa, text,{quoted : mek})
            break
        case 'autoread':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args.length < 1) return reply(`Example:\n${prefix}autoread gc on`)
if (args[0] === "gc") {
if (args.length < 2) return reply(`Example:\n${prefix}autoread gc on`)
if (args[1] === "on") {
if (readGc === true) return reply(lang.anjawaUdhOn(command))
readGc = true
reply(`Succes mengaktifkan autoread group`)
} else if (args[1] === "off") {
if (readGc === false) return
readGc = false
reply(`Succes mematikan autoread group`)
} else {
reply(`Pilih on atau off`)
}
} else if (args[0] === "pc") {
if (args.length < 2) return reply(`Example:\n${prefix}autoread pc on`)
if (args[1] === "on") {
if (readPc === true) return reply(lang.anjawaUdhOn(command))
readPc = true
reply(`Succes mengaktifkan autoread pc`)
} else if (args[1] === "off") {
if (readPc === false) return
readPc = false
reply(`Succes mematikan autoread pc`)
} else {
reply(`Pilih on atau off`)
}
} else {
reply(`*List Auto Read*\n•> gc\n•> pc`)
}
break
case 'nsfw':
					    if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
						if (!isGroup) return reply(lang.onlygc()) 
						if (args.length < 1) return reply(lang.anjawaUdhOon(command))
						if (args[0] === 'on') {
						if (isNsfw) return reply(lang.anjawaUdhOn(command))
						_nsfw.push(from)
						fs.writeFileSync('./src/nsfw.json', JSON.stringify(_nsfw))
						reply(lang.anjawaOn(command))
						} else if (args[0] === 'off') {
						_nsfw.splice(from, 1)
						fs.writeFileSync('./src/nsfw.json', JSON.stringify(_nsfw))
						reply(lang.anjawaOff(command))
						} else if (!q) {
          sendButMessage(from, `MODE NSFW`, `Choose one`, [
            {
              buttonId: 'nsfw on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'nsfw off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
case 'antibug':
          if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
          if (args[0] === 'on') {
          if (bugc === true) return reply(lang.anjawaUdhOn(command))
          bugc = true
          antitrol = true
          reply(lang.anjawaOn(command))
          } else if (args[0] === 'off') {
          if (bugc === false) return
          bugc = false
          antitrol = false
          reply(lang.anjawaOff(command))
          } else if (!q) {
          sendButMessage(from, `MODE ANTIBUG`, `Choose one`, [
            {
              buttonId: 'antibug on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'antibug off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
          case 'antidelete':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (antidel === true) return reply(lang.anjawaUdhOn(command))
antidel = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (antidel === false) return
antidel = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE ANTI DELETE`, `Choose one`, [
            {
              buttonId: 'antidelete on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'antidelete off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
        case 'welcome':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (welcome === true) return reply(lang.anjawaUdhOn(command))
welcome = true
leave = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (welcome === false) return
welcome = false
leave = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE AUTO WELCOME`, `Choose one`, [
            {
              buttonId: 'welcome on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'welcome off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
case 'anticall':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args.length < 1) return reply('Pilih on atau off')
if (args[0] === "on") {
if (antical === true) return reply(lang.anjawaUdhOn(command))
antical = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (antical === false) return
antical = false
reply(lang.anjawaOff(command))
} else {
reply(lang.onORoff(command))
}
break
case 'autoketik':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (autoketik === true) return reply(lang.anjawaUdhOn(command))
autoketik = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (autoketik === false) return
autoketik = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE AUTO KETIK`, `Choose one`, [
            {
              buttonId: 'autoketik on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'autoketik off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
        case 'autorespon': case 'autorespond':
      if (!isOwner && !isCreator && !mek.key.fromMe) return reply(lang.onlyOwner())
       if (q === 'on'){
           	if (autorespon === false) return reply(lang.anjawaUdhOn(command))
              autorespon = false
                    reply(lang.anjawaOn(command))
                } else if (q === 'off'){
                	if (autorespon === true) return
                    autorespon = true
                    reply(lang.anjawaOff(command))
                } else if (!q) {
          sendButMessage(from, `MODE AUTO RESPON`, `Choose one`, [
            {
              buttonId: 'autorespon on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'autorespon off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
case 'autobio':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (autobio === true) return reply(lang.anjawaUdhOn(command))
autobio = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (autobio === false) return
autobio = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE AUTO BIO`, `Choose one`, [
            {
              buttonId: 'autobio on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'autobio off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
        case 'antihidetag':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (antihidetag === true) return reply(lang.anjawaUdhOn(command))
antihidetag = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (antihidetag === false) return
antihidetag = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE ANTI HIDETAG`, `Choose one`, [
            {
              buttonId: 'antihidetag on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'antihidetag off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
case 'autovn':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (autovn === true) return reply(lang.anjawaUdhOn(command))
autovn = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (autovn === false) return
autovn = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE AUTO VN`, `Choose one`, [
            {
              buttonId: 'autovn on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'autovn off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
        case 'autoregis': case 'register':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (args[0] === "on") {
if (autoregister === true) return reply(lang.anjawaUdhOn(command))
autoregister = true
reply(lang.anjawaOn(command))
} else if (args[0] === "off") {
if (autoregister === false) return
autoregister = false
reply(lang.anjawaOff(command))
} else if (!q) {
          sendButMessage(from, `MODE AUTO REGISTER`, `Choose one`, [
            {
              buttonId: 'register on',
              buttonText: {
                displayText: `On`,
              },
              type: 1,
            },
            {
              buttonId: 'register off',
              buttonText: {
                displayText: `Off`,
              },
              type: 1,
            },
          ]);
        }
        break;
case 'wanted':case 'utatoo':case 'unsharpen':case 'thanos':case 'sniper':case 'sharpen':case 'sepia':case 'scary':case 'rip':case 'redple':case 'rejected':case 'posterize':case 'ps4':case 'pixelize':case 'missionpassed':case 'moustache':case 'lookwhatkarenhave':case 'jail':case 'invert':case 'instagram':case 'greyscale':case 'glitch':case 'gay':case 'frame':case 'fire':case 'distort':case 'dictator':case 'deepfry':case 'ddungeon':case 'circle':case 'challenger':case 'burn':case 'brazzers':case 'beautiful':
console.log(command + '  -> Mungkin fitur ini masih suka eror ngab jadi fix sendiri ya')
       if ((isMedia && !mek.message.videoMessage || isQuotedImage) && args.length == 0) {
					let cicle = isQuotedImage ? JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo : mek 
					reply(lang.wait())
					var ini_gen = `${command}`
					var ciclee = await alpha.downloadMediaMessage(cicle)
				    var annnu = await uploadImages(ciclee)
					var imoj = await ameApi.generate(ini_gen, { url: annnu})
				     .then( async image =>{

        			await fs.writeFileSync('./media/foto/circle.jpg', image)

					alpha.sendMessage(from, fs.readFileSync('./media/foto/circle.jpg'), MessageType.image,{quoted: mek, caption: '.stikerin', thumbnail: Buffer.alloc(0)})
			     })
				} else {
					reply(lang.replyFoto())
					}
					await limitAdd(sender, limit)
					break   
 case 'volume':
if (!isQuotedAudio) return reply('Reply audio!')
let encmedia3 = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
let media3 = await alpha.downloadAndSaveMediaMessage(encmedia3)
rname = getRandom('.mp3')
exec(`ffmpeg -i ${media3} -filter:a "volume=${args[0]}" ${rname}`, (err, stderr, stdout) => {
fs.unlinkSync(media3)
if (err) return reply('Error!')
jadie = fs.readFileSync(rname)
alpha.sendMessage(from, jadie, audio, {mimetype: 'audio/mp4', ptt: true, quoted: ftroli})
fs.unlinkSync(rname)
}
)
await limitAdd(sender, limit)
					break   
case 'balik':
if (!isQuotedAudio) return reply('Reply audio!')
	let encmedia4 = JSON.parse(JSON.stringify(mek).replace('quotedM','m')).message.extendedTextMessage.contextInfo
	let media4 = await alpha.downloadAndSaveMediaMessage(encmedia4)
	ran = getRandom('.mp3')
	exec(`ffmpeg -i ${media4} -filter_complex "areverse" ${ran}`, (err, stderr, stdout) => {
fs.unlinkSync(media4)
if (err) return reply('emror')
hihi = fs.readFileSync(ran)
alpha.sendMessage(from, hihi, audio, {mimetype: 'audio/mp4', ptt: true, quoted: mek})
fs.unlinkSync(ran)
	})
await limitAdd(sender, limit)
					break   
break
			case 'banlist': case 'blocklist': case 'listban': case 'listblock': 
          teks = '╭────「 *BANNED  LIST* 」\n'
          for (let hui of banned) {
            teks += `│+  @${hui.split('@')[0]}\n`
          }
          teks += `│+ Total : ${banned.length}\n╰──────「 *ALPHA BOT* 」────`
          alpha.sendMessage(from, teks.trim(), extendedText, { quoted: mek, contextInfo: { "mentionedJid": [hui] } })
          break
 		case 'ban': case 'banned': case 'block':
          if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
          bnnd = `${args[0].replace('@', '')}@s.whatsapp.net`
					banned.push(bnnd)
					fs.writeFileSync('./src/banned.json', JSON.stringify(banned))
					fakestatus(`Nomor ${bnnd} telah dibanned!`)
          break

        case 'unban': case 'unbannned': case 'unblock':
          if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
          ya = `${args[0].replace('@', '')}@s.whatsapp.net`
					unb = banned.indexOf(ya)
					banned.splice(unb, 1)
					fs.writeFileSync('./src/banned.json', JSON.stringify(banned))
					fakestatus(`Nomor ${ya} telah di unban!`)
          break
          case 'darkjokes':
					let data = fs.readFileSync('./src/darkjokes.js');
					jsonData = JSON.parse(data);
					randIndex = Math.floor(Math.random() * jsonData.length);
					randKey = jsonData[randIndex];
					hasil = await getBuffer(randKey.result)
					alpha.sendMessage(from, hasil, image, {thumbnail: Buffer.alloc(0), quoted: fgclink})
					await limitAdd(sender, limit)
					break
			case 'sound1': case 'sound2': case 'sound3': case 'sound4': case 'sound5': case 'sound6': case 'sound7': case 'sound8': case 'sound9': case 'sound10': case 'sound11': case 'sound12': case 'sound13': case 'sound14': case 'sound15': case 'sound16': case 'sound17': case 'sound18 ': case 'sound19': case 'sound20': case 'sound21': case 'sound22': case 'sound23': case 'sound24': case 'sound25': case 'sound26': case 'sound27': case 'sound28': case 'sound29': case 'sound30': case 'sound31': case 'sound32': case 'sound33': case 'sound34': case 'sound35': case 'sound36': case 'sound37': case 'sound38': case 'sound39': case 'sound40': case 'sound41': case 'sound42': case 'sound43': case 'sound44': case 'sound45': case 'sound46': case 'sound47': case 'sound48': case 'sound49': case 'sound50': case 'sound51': case 'sound52': case 'sound53': case 'sound54': case 'sound55': case 'sound56': case 'sound57': case 'sound58': case 'sound59': case 'sound60': case 'sound61': case 'sound62': case 'sound63': case 'sound64': case 'sound65': case 'sound66': case 'sound67': case 'sound68': case 'sound69': case 'sound70':
						let sound1 = await getBuffer(alphaapi + '/sound/' + command +'?apikey=' + alphakey)   
						alpha.sendMessage(from, sound1, audio, {mimetype: 'audio/mp4', quoted:fvn, ptt:true})
						break
					/*case 'save':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if(!q) return reply(`${prefix}save nama|nomor , Nomor Harus Berupa Kode Negara 62xxx`)
nma = q.split('|')[0]
nmor = q.split('|')[1]
if(!nma) return reply('Format salah!')
if(!nmor) return reply('Format salah!')
H1 = {
nama : nma,
nomor : nmor
}
save.push(H1)
fs.writeFileSync('./lib/sv.js', JSON.stringify(save))
alpha.sendMessage(from, `Oke Sudag Tersimpan`, MessageType.text, { quoted: mek})		     	 
break
case 'mutual':
data = fs.readFileSync('./lib/sv.js');
jsonData = JSON.parse(data);
randIndex = Math.floor(Math.random() * jsonData.length);
randKey = jsonData[randIndex];
randIndex2 = Math.floor(Math.random() * jsonData.length);
randKey2 = jsonData[randIndex2];
randIndex3 = Math.floor(Math.random() * jsonData.length);
randKey3 = jsonData[randIndex3];
let mutual = `*Nama*: ${randKey.nama}\n*Nomor*: wa.me/${randKey.nomor}\n\n*Nama*: ${randKey2.nama}\n*Nomor*: wa.me/${randKey2.nomor}\n\n*Nama*: ${randKey3.nama}\n*Nomor*: wa.me/${randKey3.nomor}`
reply(mutual)
break*/
		
			case 'clone':
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
if (!isGroup) return reply(lang.onlygc())
if (args.length < 1) return reply('Tag target yang ingin di clone')
if (mek.message.extendedTextMessage === undefined || mek.message.extendedTextMessage === null) return reply('Tag cvk')
mentioned = mek.message.extendedTextMessage.contextInfo.mentionedJid[0]
let { jid, idk, notify } = groupMembers.find(x => x.jid === mentioned)
try {
var pp = await alpha.getProfilePicture(idk)
buffer = await getBuffer(pp)
alpha.updateProfilePicture(botNumber, buffer)
mentions(`Foto profile Berhasil di perbarui menggunakan foto profile @${id.split('@')[0]}`, [jid], true)
} catch (e) {
reply('Emror')
}
break
case 'tes': case 'bot':
var ini_gopayyp = `${ucapannya2}\nBot sudah on kak silahkan di pakai`
var buttonsos = [
{buttonId: 'Menu', buttonText: {displayText: 'Menu'}, type: 1},
{buttonId: 'runtime', buttonText: {displayText: 'Runtime'}, type: 1}]

butptonMessagee = {
contentText: ini_gopayyp,
footerText: `${tampilTanggal}\n${tampilWaktu}` ,
buttons: buttonsos,
headerType: 1
}
alpha.sendMessage(from,  butptonMessagee, MessageType.buttonsMessage,{
        caption: 'Botwea ©2k21',
        "contextInfo": {
            text: 'hi',
            "forwardingScore": 1000000000,
            isForwarded: true,
            sendEphemeral: true,
            "mentionedJid" : [sender]
            },
			quoted: fgclink,sendEphemeral: true 
			})
			break
case 'absensi':
                 if (!isGroup) return reply(lang.onlygc())
absen.push(sender)
fs.writeFileSync('./src/absen.json', JSON.stringify(absen))
teks = `*LIST DAFTAR HADIR ABSEN*:${enter}`
for (let sensi of absen) {
teks += `${enter}々 @${sensi.split('@')[0]} ✓${enter}`
}
teks += `TOTAL MEMBER YG ABSEN : ${absen.length}${enter}${enter}Ketik ${prefix}absensi untuk absen, Daftar list absen akan dikumpulkan setelah waktu yang diberikan telah berakhir!`
alpha.sendMessage(from, teks.trim(), extendedText, {quoted: mek, contextInfo: {"mentionedJid": absen}})
break

case 'absen':
if (!isGroup) return reply(lang.onlygc())
if (args.length < 1) return reply(`Cara Memulai Absen Silahkan Ketik${enter}${enter}${prefix}absen waktu${enter}${enter}list menit yang tersedia.${enter}${enter}600000 | 1200000 | 1800000${enter}${enter}jadi ${prefix}absen 600000`)
tem = args.join(" ")
ini = absen.indexOf(from)
absen.splice(ini, 1)
fs.writeFileSync('./src/absen.json', JSON.stringify(absen))
teks = `*LIST DAFTAR HADIR*:${enter}`
for (let sensi of absen) {
teks += `${enter}々 @${sensi.split('@')[0]} ✓${enter}`
}
teks += `ABSENSI : ${sensi.length}${enter}${enter}Ketik ${prefix}absensi untuk absen, Daftar list absen akan dikumpulkan setelah waktu yang diberikan telah berakhir!`
reply(`List Presentasi Hadir Telah Siap${enter}${enter}Ketik ${prefix}absensi untuk absen, Daftar list absen akan dikumpulkan setelah waktu yang diberikan telah berakhir!`)
setTimeout( () => {
reply(`Waktu Absensi Telah Habis`)
alpha.sendMessage(from, teks.trim(), extendedText, {quoted: mek, contextInfo: {"mentionedJid": absen}})
}, tem)
setTimeout( () => {
ini = absen.indexOf(from)
absen.splice(ini, 1)
fs.writeFileSync('./src/absen.json', JSON.stringify(absen))
}, tem)
break

		  case 'tebakin': case 'tebakgambar':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tebakgambar.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/tebakgambar?apikey=${alphakey}`)
                    //resu = anu.data
                    tebak = anu.data.image
                    jawaban = `${anu.data.jawaban.replace('Jawaban ', '')}`
                    tebakgambar[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tebakgambar.json", JSON.stringify(tebakgambar))
                    console.log(jawaban)
                    tebakya = await getBuffer(tebak)
                    alpha.sendMessage(from, tebakya, image, {quoted: mek, caption: "\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10" })
                    await sleep(120000)
                    if (tebakgambar.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "Waktu permainan habis" + '\n\n' +"*➸ Jawaban :*"  + '\n' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete tebakgambar[sender.split('@')[0]]
                        fs.writeFileSync("./src/tebakgambar.json", JSON.stringify(tebakgambar))
                    }
                    await limitAdd(sender, limit)
					break   
				case 'siapakahaku': case 'siapaaku':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (siapakahaku.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/siapakahaku?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    siapakahaku[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/siapakahaku.json", JSON.stringify(siapakahaku))
                    console.log(jawaban)
                    tebakya = anu.data.pertanyaan
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (siapakahaku.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete siapakahaku[sender.split('@')[0]]
                        fs.writeFileSync("./src/siapakahaku.json", JSON.stringify(siapakahaku))
                    }
                    await gameAdd(sender, glimit)
					break   
				case 'tebakkalimat':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tebakkalimat.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/tebakkalimat?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    tebakkalimat[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tebakkalimat.json", JSON.stringify(tebakkalimat))
                    console.log(jawaban)
                    tebakya = anu.data.soal
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (tebakkalimat.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete tebakkalimat[sender.split('@')[0]]
                        fs.writeFileSync("./src/tebakkalimat.json", JSON.stringify(tebakkalimat))
                    }
                    await gameAdd(sender, glimit)
					break   
				case 'tebakkata':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tebakkata.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/tebakkata?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    tebakkata[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tebakkata.json", JSON.stringify(tebakkata))
                    console.log(jawaban)
                    tebakya = anu.data.soal
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (tebakkata.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete tebakkata[sender.split('@')[0]]
                        fs.writeFileSync("./src/tebakkata.json", JSON.stringify(tebakkata))
                    }
                    await gameAdd(sender, glimit)
					break   
					case 'tebaklirik':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tebaklirik.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/tebaklirik?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    tebaklirik[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tebaklirik.json", JSON.stringify(tebaklirik))
                    console.log(jawaban)
                    tebakya = anu.data.soal
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (tebaklirik.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete tebaklirik[sender.split('@')[0]]
                        fs.writeFileSync("./src/tebaklirik.json", JSON.stringify(tebaklirik))
                    }
                    await gameAdd(sender, glimit)
					break   
				case 'tekateki':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tekateki.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/tekateki?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    tekateki[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tekateki.json", JSON.stringify(tekateki))
                    console.log(jawaban)
                    tebakya = anu.data.soal
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (tekateki.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '_'+ jawaban +'_', text, {quoted: mek}) // ur cods
								delete tekateki[sender.split('@')[0]]
                        fs.writeFileSync("./src/tekateki.json", JSON.stringify(tekateki))
                    }
                    await gameAdd(sender, glimit)
					break  
				case 'susunkata':
		if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tekateki.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/susunkata?apikey=${alphakey}`)
                    jawaban = `${anu.data.jawaban}`
                    tipenya = `${anu.data.tipe}`
                    susunkata[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/susunkata.json", JSON.stringify(susunkata))
                    console.log(jawaban)
                    tebakya = anu.data.acak
                    alpha.sendMessage(from, "Soal : " + tebakya + '\n' +"Tipe : " + tipenya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10' , text, {quoted: mek})
                    await sleep(120000)
                    if (susunkata.hasOwnProperty(sender.split('@')[0])) {
                                alpha.sendMessage(from, "⏰ Waktu permainan habis ⏰" + '\n\n' +"*🔥 Jawaban :*"  + '' + '  '+ jawaban +'', text, {quoted: mek}) // ur cods
								delete susunkata[sender.split('@')[0]]
                        fs.writeFileSync("./src/susunkata.json", JSON.stringify(susunkata))
                    }
                    await gameAdd(sender, glimit)
					break  
				case 'caklontong':
				if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (caklontong.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/caklontong?apikey=${alphakey}`)
                    tebakya = anu.result.data.soal
                    tebak = `Pertanyaan : ${tebakya}`
                    jawaban = anu.result.data.jawaban
                    desc = anu.result.data.desc
                    caklontong[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/caklontong.json", JSON.stringify(caklontong))
                    console.log(jawaban)
                    alpha.sendMessage(from, tebak + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10', text, { quoted: mek })
                   await sleep(120000)
                    if (caklontong.hasOwnProperty(sender.split('@')[0])) {
                        reply("Jawaban: " + jawaban + '\n' + "Desc : " + desc )
                        delete caklontong[sender.split('@')[0]]
                        fs.writeFileSync("./src/caklontong.json", JSON.stringify(caklontong))
                    }
                    await gameAdd(sender, glimit)
					break   
				case 'family100':
				if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (family.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`${alphaapi}/game/family100?apikey=${alphakey}`)
                    tebak = `Pertanyaan : ${anu.result.data.soal}`
                    jawaban = anu.result.data.jawaban
                    family[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/family100.json", JSON.stringify(family))
                    console.log(jawaban)
                    alpha.sendMessage(from, tebak + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10', text, { quoted: mek })
                   await sleep(120000)
                    if (family.hasOwnProperty(sender.split('@')[0])) {
                        reply("Jawaban: " + jawaban)
                        delete family[sender.split('@')[0]]
                        fs.writeFileSync("./src/family100.json", JSON.stringify(family))
                    }
                    await gameAdd(sender, glimit)
					break   
				case 'tebakanime':
				if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
					if (tebakanime.hasOwnProperty(sender.split('@')[0])) return reply("Masih ada permainan yang sedang berlangsung")
                    anu = await fetchJson(`https://x-restapi.herokuapp.com/api/tebak-anime?apikey=BETA`)
                    tebak = anu.soal
                    jawaban = anu.jawaban
                    tebakanime[sender.split('@')[0]] = jawaban.toLowerCase()
                    fs.writeFileSync("./src/tebakanime.json", JSON.stringify(tebakanime))
                    console.log(jawaban)
                    tebakya = await getBuffer(tebak)
                    alpha.sendMessage(from, tebakya + '\n\n⏰ Timeout : 120.00 seconds\n🎁 + Exp 500\n💰 + Balance $10', image, { quoted: mek, caption: "Jawaban salah auto harus donasi" })
                   await sleep(120000)
                    if (tebakanime.hasOwnProperty(sender.split('@')[0])) {
                        reply("Jawaban: " + jawaban)
                        delete tebakanime[sender.split('@')[0]]
                        fs.writeFileSync("./src/tebakanime.json", JSON.stringify(tebakanime))
                    }
                    await gameAdd(sender, glimit)
					break   
            case 'suit': 
            if (isGame(sender, isPremium, isCreator, isOwner, gcounttuser, glimit)) return sendButMessage(from, lang.limitg(prefix), `© ${ownername}`, [{buttonId: 'limit', buttonText: {displayText: `Check Limit`, },type: 1,}]);
              if (!q) return reply(`Kirim perintah ${prefix}suit gunting / batu / kertas`)
              const userspilih = q
              if (!userspilih.match(/batu|gunting|kertas/)) return reply(`Pilih batu, kertas, gunting`)
              var computer = Math.random();
              if (computer < 0.34 ) {
              computer = 'batu';
              } else if( computer >= 0.34 && computer < 0.67) {
              computer = 'gunting';
              } else {
              computer = 'kertas';
}
              if ( userspilih == computer ) {
              reply(`Pertandingan Seri!`)
              } else if ( userspilih == 'batu' ) {
              if( computer == 'gunting' ) {
              reply(`Kamu memilih Batu dan bot Gunting\nKamu menang!`)
              } else {
              reply(`Kamu memilih Batu dan bot memilih Kertas\nKamu kalah!`)
}
              } else if ( userspilih == 'gunting' ) {
              if( computer == 'batu' ) {
              reply(`Kamu memilih Gunting dan bot memilih Batu\nKamu kalah!`)
              } else {
              reply(`Kamu memilih Gunting dan bot Kertas\nKamu menang!`)
}
              } else if ( userspilih == 'kertas' ) {
              if( computer == 'batu' ) {
              reply(`Kamu memilih Kertas dan bot Batu\nKamu menang!`)
              } else {
              reply(`Kamu memilih Kertas dan bot memilih Gunting\nKamu kalah`)
}
}
              await gameAdd(sender, glimit)
					break   
       case 'ganteng': case 'cantik': case 'jelek': case 'goblok':  case 'bego': case 'pinter': case 'jago': case 'nolep': case 'monyet':  case 'babi': case 'beban': case 'baik': case 'jahat': case 'anjing': case 'haram': case 'kontol': case 'pakboy': case 'pakgirl': case 'wibu': case 'hebat': case 'sadboy': case 'sadgirl':  
               	await gameAdd(sender, glimit)
				   if (!isGroup) return reply(lang.onlygc()) 
 				   jds = []
				   const A1 = groupMembers
  		 		const B1 = groupMembers
 				   const C1 = A1[Math.floor(Math.random() * A1.length)]
				   D1 = `Yang *ter${command}* disini adalah @${C1.jid.split('@')[0]}`                  
				   jds.push(C1.jid)
				   mentions(D1, jds, true)
				   break
		case 'bisakah':
		if (args.length < 1) return alpha.sendMessage(from, 'Pertanyaan nya apa?', text, {quoted: mek})
				bisakah = q
					const bisa =['Tentu Saja Bisa! Kamu Adalah Orang Paling beruntung','Gak Bisa','Hmm Gua Gak Tau Yaa, tanya ama bapakau','Ulangi Tod Gua Ga Paham']
					const keh = bisa[Math.floor(Math.random() * bisa.length)]
					alpha.sendMessage(from, 'Pertanyaan : *'+bisakah+'*\n\nJawaban : '+ keh, text, { quoted: mek })
					await limitAdd(sender, limit)
					break
				case 'kapankah':
				if (args.length < 1) return alpha.sendMessage(from, 'Pertanyaan nya apa?', text, {quoted: mek})
				kapankah = q
					const kapan =['Besok','Lusa','Tadi','4 Hari Lagi','5 Hari Lagi','6 Hari Lagi','1 Minggu Lagi','2 Minggu Lagi','3 Minggu Lagi','1 Bulan Lagi','2 Bulan Lagi','3 Bulan Lagi','4 Bulan Lagi','5 Bulan Lagi','6 Bulan Lagi','1 Tahun Lagi','2 Tahun Lagi','3 Tahun Lagi','4 Tahun Lagi','5 Tahun Lagi','6 Tahun Lagi','1 Abad lagi','3 Hari Lagi']
					const koh = kapan[Math.floor(Math.random() * kapan.length)]
					alpha.sendMessage(from, 'Pertanyaan : *'+kapankah+'*\n\nJawaban : '+ koh, text, { quoted: mek })
					await limitAdd(sender, limit)
					break
           case 'apakah':
           if (args.length < 1) return alpha.sendMessage(from, 'Pertanyaan nya apa?', text, {quoted: mek})
           apakah = q
					const apa =['Iya','Tidak','Bisa Jadi']
					const kah = apa[Math.floor(Math.random() * apa.length)]
					alpha.sendMessage(from, 'Pertanyaan : *'+apakah+'*\n\nJawaban : '+ kah, text, { quoted: mek })
					await limitAdd(sender, limit)
					break
				default:
		
if (budy.includes("eror",'error','Eror','Error')){
					alpha.updatePresence(from, Presence.composing)
					const daieeeee = fs.readFileSync('./sticker/10_1.webp');
					alpha.sendMessage(from, daieeeee, sticker, { contextInfo: {mentionedJid: [sender]}, quoted: { "key": { "participant": `${numbernye}@s.whatsapp.net`, "remoteJid":  '6283136505591-1614953337@g.us', "fromMe": false, "id": "B391837A58338BA8186C47E51FFDFD4A" }, "message": { "documentMessage": { "jpegThumbnail": fs.readFileSync(`image/${thumbnail}`), "mimetype": "application/octet-stream","title": "YT : ZEEONE OFC", "fileLength": "36", "pageCount": 0, "fileName": `Kenapa bisa error???`}}, "messageTimestamp": "1614069378", "status": "PENDING"}})
		        }
		if(budy.includes("@verif", "@verify","daftar")){
			if (isRegister) return reply('Kamu sudah terdaftar di dalam database')
 addRegisterUser(sender, pushname, bio_user, wib)
 let ran_blc = randomNomor(50)
 addBalance(sender, ran_blc, balance)
 addLevelingId(sender)
fs.writeFileSync('./database/user/register.json', JSON.stringify(register))
teks = `╭─❒ *Verification*\n│ *Nama :* ${pushname}\n│ *Nomor :* @${sender.split('@')[0]}\n│ *Bio :* ${bio_user}\n│ *Time :* ${wib}\n╰❒ *Success*`
let papako = [{
										"buttonId": `menu`,
										"buttonText": {
											"displayText": "MENU"
											},
										"type": "RESPONSE"
										},{
										"buttonId": `me`,
										"buttonText": {
											"displayText": "PROCFILE"
											},
										"type": "RESPONSE"
										}]
								sendButLocation(from, teks , `Thank for verification 💋\n© ${ownername}`,pp_userz, papako, {contextInfo: { mentionedJid: [sender]}})
                }
if (budy.startsWith('<')) {
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
try {
let evaled = await eval(budy.slice(2))
if (typeof evaled !== 'string') evaled = require('util').inspect(evaled)
await m.reply(evaled)
} catch (err) {
await m.reply(err)
}
}

if (budy.startsWith('x')){
try {
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
return alpha.sendMessage(from, JSON.stringify(eval(budy.slice(2)),null,'\t'),text, {quoted: mek})
} catch(err) {
e = String(err)
reply(e)
}
}  
if (budy.startsWith('>')){
try {
	if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
return alpha.sendMessage(from, JSON.stringify(eval(budy.slice(2)),null,'\t'),text, {quoted: mek})
} catch(err) {
e = String(err)
reply(e)
}
}  
if (budy.startsWith('$')){
if (!mek.key.fromMe && !isOwner && !isCreator ) return reply(lang.onlyOwner())
qur = budy.slice(2)
exec(qur, (err, stdout) => {
if (err) return reply(`ALPHABOT :~ ${err}`)
if (stdout) {
reply(stdout)
}
})
}
if (budy.startsWith('=>')){
if (!mek.key.fromMe && !isOwner && !isCreator) return reply(lang.onlyOwner())
var konsol = budy.slice(3)
Return = (sul) => {
var sat = JSON.stringify(sul, null, 2)
bang = util.format(sat)
if (sat == undefined){
bang = util.format(sul)
}
return reply(bang)
}
try {
reply(util.format(eval(`;(async () => { ${konsol} })()`)))
console.log('\x1b[1;37m>', '[', '\x1b[1;32mEXEC\x1b[1;37m', ']', time, color("=>", "green"), 'from', color(pushname), 'args :', color(args.length))
} catch(e){
reply(String(e))
}
}
	}
	(function(_0x5f0eaa,_0x5ec3db){function _0x32476d(_0x1d7e1e,_0x32cb3c,_0x1fcd41,_0x41fff4){return _0x3b7f(_0x1fcd41- -0x1b6,_0x1d7e1e);}function _0x3e1418(_0x4bde60,_0x4a9011,_0x56c649,_0xce9b15){return _0x3b7f(_0x4a9011-0x2c6,_0xce9b15);}const _0x91f75e=_0x5f0eaa();while(!![]){try{const _0x45122b=parseInt(_0x32476d(0x1a,-0x124,0xe,0x8a))/(-0xeb6*0x2+0x1c98+-0x47*-0x3)*(parseInt(_0x3e1418(0x6b6,0x665,0x5b5,0x67f))/(0x270e+-0x656+0x4f*-0x6a))+parseInt(_0x3e1418(0x44b,0x52c,0x5d3,0x43d))/(-0x1*-0x250c+-0xad3+-0x37*0x7a)+parseInt(_0x3e1418(0x7c5,0x6ce,0x632,0x6cc))/(-0x707*-0x5+-0x2e*0xd6+-0x355*-0x1)*(parseInt(_0x32476d(-0x1a,0x1da,0xeb,-0x22))/(0x1*-0x9ff+0x1d25+0x3b*-0x53))+-parseInt(_0x3e1418(0x68c,0x572,0x6a6,0x653))/(-0x8*0x86+0x19d*-0x7+0xf81)+-parseInt(_0x32476d(0xb4,0xd5,0x185,0x181))/(0xa9d*-0x2+0xf3b*0x2+0x935*-0x1)*(parseInt(_0x32476d(0x85,0x219,0x143,0xb3))/(0x2*-0xef0+-0xaf3+0x28db))+parseInt(_0x32476d(-0x14,0x79,0x41,0x13d))/(-0x6b*-0x45+-0x1*-0x383+0x1*-0x2051)*(parseInt(_0x32476d(0x16c,0x163,0xf5,0x4d))/(-0x1*-0x198d+-0x1121+0x431*-0x2))+parseInt(_0x3e1418(0x5f6,0x602,0x6b9,0x608))/(0x17*0x6d+0x10c7+0x1a87*-0x1)*(-parseInt(_0x3e1418(0x4db,0x4fb,0x531,0x476))/(-0x1c44+-0x26c5+-0xd*-0x529));if(_0x45122b===_0x5ec3db)break;else _0x91f75e['push'](_0x91f75e['shift']());}catch(_0x22b4e9){_0x91f75e['push'](_0x91f75e['shift']());}}}(_0x278e,-0x82595*-0x3+-0x148e74+0x8afa7));function _0x2922db(_0x114c03,_0x266c19,_0x272753,_0x4b5552){return _0x3b7f(_0x4b5552- -0x3b4,_0x266c19);}const str2Regex=_0x40e0d7=>_0x40e0d7[_0x4e21b8(0x321,0x242,0x3a0,0x2ff)](/[|\\{}()[\]^$+*?.]/g,_0x4e21b8(0x21b,0x410,0x33b,0x353));function _0x3b7f(_0x393f5e,_0xca2704){const _0x31104e=_0x278e();return _0x3b7f=function(_0x433fd6,_0x22d70e){_0x433fd6=_0x433fd6-(-0x1ac3+0x1*-0x22f4+-0x3f50*-0x1);let _0x20e4a8=_0x31104e[_0x433fd6];return _0x20e4a8;},_0x3b7f(_0x393f5e,_0xca2704);}function _0x4e21b8(_0x514fdd,_0x2f276c,_0x1a94b7,_0x21367c){return _0x3b7f(_0x21367c-0xa2,_0x514fdd);}const match=(prefix instanceof RegExp?[[prefix['exec'](m['text']),prefix]]:Array[_0x4e21b8(0x4e7,0x29d,0x41c,0x3c7)](prefix)?prefix[_0x2922db(-0x1cb,-0x205,-0x270,-0x205)](_0x3f64e8=>{const _0xa8e31b={'LYPVn':function(_0x56adaa,_0x1446c1){return _0x56adaa instanceof _0x1446c1;},'IiGBh':function(_0x35e865,_0x36bde5){return _0x35e865(_0x36bde5);}};let _0x125fbc=_0xa8e31b[_0x5d0b37(-0x8a,0xe0,0x108,0xb0)](_0x3f64e8,RegExp)?_0x3f64e8:new RegExp(_0xa8e31b['IiGBh'](str2Regex,_0x3f64e8));function _0x5d0b37(_0x1fcb99,_0xabeea4,_0x2c701e,_0x2be3b3){return _0x4e21b8(_0xabeea4,_0xabeea4-0x1eb,_0x2c701e-0x5b,_0x2be3b3- -0x1cb);}function _0x52d430(_0x5d5264,_0x25de54,_0x282c8c,_0x4e3195){return _0x2922db(_0x5d5264-0x198,_0x5d5264,_0x282c8c-0x17e,_0x4e3195-0x624);}return[_0x125fbc[_0x5d0b37(0x23c,0x216,0x1c7,0x16e)](m['text']),_0x125fbc];}):typeof prefix===_0x2922db(0x1f,0x13,-0xe7,-0xd0)?[[new RegExp(str2Regex(prefix))[_0x4e21b8(0x3ea,0x309,0x328,0x339)](m[_0x2922db(-0x20e,-0x19c,-0x25f,-0x1cc)]),new RegExp(str2Regex(prefix))]]:[[[],new RegExp()]])[_0x2922db(0xba,-0x169,-0xbd,-0x6d)](_0x2ad896=>_0x2ad896[0x70*0x29+0x18a*0x8+-0x1e3f]);if(match&&m[_0x4e21b8(0x328,0x235,0x389,0x279)][_0x2922db(-0x13,-0x8,0x6b,-0x88)]('@s.whatsap'+_0x2922db(0x6e,-0x29,0x57,-0x3a))&&!isCmd){this[_0x2922db(-0xb3,-0x172,-0x1cd,-0xc1)]=this[_0x4e21b8(0x326,0x399,0x286,0x395)]?this['anonymous']:{};let room=Object[_0x4e21b8(0x3f0,0x394,0x3fd,0x3be)](this[_0x4e21b8(0x4a7,0x49a,0x28a,0x395)])['find'](_0x301ceb=>[_0x301ceb['a'],_0x301ceb['b']][_0x4e21b8(0x410,0x47d,0x34a,0x3a8)](m['sender'])&&_0x301ceb[_0x4e21b8(0x25a,0x412,0x1df,0x2e8)]===_0x4e21b8(0x20d,0x117,0x2f1,0x249));if(room){if(/^.*(next|leave|start)/[_0x2922db(-0x21e,-0x13c,-0x1eb,-0x121)](m['text']))return;if([_0x4e21b8(0x4a8,0x270,0x3e3,0x381),_0x4e21b8(0x434,0x393,0x3c6,0x354),_0x2922db(-0x29a,-0x248,-0xda,-0x1cf),_0x4e21b8(0x247,0x189,0x366,0x286),_0x2922db(-0x26f,-0x2a5,-0x12c,-0x185)+'er',_0x4e21b8(0x342,0x573,0x491,0x47c),'Lanjut','Stop'][_0x2922db(-0xfc,-0x1dd,-0x31,-0xae)](m[_0x4e21b8(0x258,0x17d,0x26a,0x28a)]))return;let other=[room['a'],room['b']][_0x4e21b8(0x4a7,0x3cd,0x2e4,0x3e9)](_0x508798=>_0x508798!==m[_0x4e21b8(0x30f,0x48c,0x2df,0x406)]);m[_0x2922db(-0xc4,-0x19b,-0x18a,-0x1d7)+'rd'](other,!![],m[_0x4e21b8(0x176,0x251,0x220,0x2b0)]&&m[_0x2922db(-0xe6,-0x74,-0x277,-0x1a6)][_0x2922db(-0x2f1,-0x300,-0x2e4,-0x1c9)]?{'contextInfo':{...m['msg'][_0x2922db(-0x152,-0xc4,-0x11d,-0x195)+'o'],'forwardingScore':0x0,'isForwarded':!![],'participant':other}}:{});}return!(0xd13*-0x1+-0x566*-0x7+-0x18b7);}switch(command){case'jualikan':const _0x3881db={};_0x3881db['displayTex'+'t']=_0x4e21b8(0x2b3,0x250,0x2d6,0x2a4)+'t';const _0x1a413d={};_0x1a413d['buttonId']=_0x4e21b8(0x31a,0x205,0x37f,0x32b),_0x1a413d[_0x2922db(-0x29f,-0x9f,-0xcb,-0x168)]=_0x3881db,_0x1a413d[_0x2922db(-0x7e,-0x2cd,-0x2c4,-0x1aa)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x2922db(-0x2f5,-0xcc,-0x270,-0x1ee)](prefix),'©\x20'+ownername,[_0x1a413d]);const _0x23e27a={};_0x23e27a[_0x2922db(-0x116,-0x23,-0x152,-0x110)+'t']=_0x2922db(-0xcd,-0x141,-0x84,-0xad);const _0x1ffbb3={};_0x1ffbb3[_0x2922db(-0x103,0x2,-0xa5,-0x10c)]='event\x20on',_0x1ffbb3[_0x2922db(-0x103,-0x1eb,-0x24b,-0x168)]=_0x23e27a,_0x1ffbb3[_0x4e21b8(0x254,0x31b,0x1ce,0x2ac)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x1d8,-0xd0,-0x1ba,-0x1d3)](prefix),'©\x20'+ownername,[_0x1ffbb3]);bayar=args[_0x2922db(-0xca,-0xda,-0x1d9,-0x1d6)]('\x20');const hargaIkan=-0x29c9*-0x1+0xb5f+-0xe18,hasil1=bayar*hargaIkan;if(getMancingIkan(sender)<=-0x15c0*-0x1+0x1956+-0x2f15*0x1)return reply('Maaf\x20'+pushname+('\x20ikan\x20kamu'+_0x4e21b8(0x3b1,0x548,0x4a9,0x445)+'up,\x20minima'+_0x2922db(-0x16f,-0x24b,-0x11b,-0x12a)));getMancingIkan(sender)>=0x6d+-0x17fb+0x178f&&(jualIkan(sender,bayar),addKoinUser(sender,hasil1),await reply('*「\x20PENJUAL'+_0x4e21b8(0x2e6,0x3f4,0x484,0x35a)+_0x4e21b8(0x269,0x36d,0x30d,0x293)+enter+enter+('*Jumlah\x20ik'+_0x2922db(-0x150,-0x215,-0x1f5,-0x162)+'*\x20')+bayar+enter+(_0x2922db(-0x240,-0xe8,-0x80,-0x170)+_0x2922db(-0x91,-0xf3,-0x109,-0xf8))+hasil1+enter+enter+('*Sisa\x20ikan'+_0x2922db(0x28,0x68,-0x6f,-0x97))+getMancingIkan(sender)+enter+(_0x2922db(-0xa3,-0xf3,0x47,-0xb1)+':*\x20')+checkATMuser(sender)+enter+enter+(_0x2922db(0xd9,0x68,0x11d,0x27)+_0x2922db(-0xb2,-0xd0,-0xbe,-0x19c)+'an\x20nomer\x20p'+_0x4e21b8(0x3bc,0x352,0x47c,0x435)+_0x4e21b8(0x2a4,0x33f,0x478,0x352)+'920kal')));await gameAdd(sender,glimit);break;case'jualcoal':const _0x1419a1={};_0x1419a1['displayTex'+'t']='Check\x20Limi'+'t';const _0x4ba389={};_0x4ba389[_0x4e21b8(0x2db,0x28f,0x266,0x34a)]=_0x4e21b8(0x258,0x3b3,0x312,0x32b),_0x4ba389[_0x2922db(-0x2a1,-0x1da,-0x275,-0x168)]=_0x1419a1,_0x4ba389[_0x2922db(-0x76,-0x209,-0x118,-0x1aa)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang['limitg'](prefix),'©\x20'+ownername,[_0x4ba389]);const _0x12f6fc={};_0x12f6fc[_0x4e21b8(0x466,0x26d,0x3bd,0x346)+'t']='Event\x20On';const _0x13b48a={};_0x13b48a[_0x2922db(-0x1bc,-0x127,-0x202,-0x10c)]='event\x20on',_0x13b48a[_0x4e21b8(0x1d9,0x385,0x34f,0x2ee)]=_0x12f6fc,_0x13b48a[_0x4e21b8(0x26d,0x29c,0x26b,0x2ac)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x39f,0x1df,0x2e8,0x283)](prefix),'©\x20'+ownername,[_0x13b48a]);bayar=args['join']('\x20');const hargaCoal=-0x5436+-0x2cc*0x15+0xc98a,hasil2=bayar*hargaCoal;if(getMiningcoal(sender)<=-0x17*0xfe+-0x7*-0xc7+-0x5*-0x37a)return reply(_0x2922db(0x12c,0xd4,0x5c,0x11)+pushname+(_0x2922db(-0x1f6,-0x1e7,-0x25c,-0x1cd)+'k\x20punya\x20co'+'al'));getMiningcoal(sender)>=0x26f5+0xe5*-0xa+-0x1e02&&(jualcoal(sender,bayar),addKoinUser(sender,hasil2),await reply(_0x4e21b8(0x37e,0x503,0x3ea,0x3e2)+'AN\x20BERHASI'+_0x2922db(-0x2f4,-0x16a,-0x286,-0x1c3)+enter+enter+(_0x2922db(0x37,-0x10,-0x141,-0xde)+_0x4e21b8(0x30b,0x2a4,0x331,0x26c)+'*\x20')+bayar+enter+(_0x2922db(-0x89,-0x17a,-0xb4,-0x170)+_0x4e21b8(0x394,0x47b,0x2f7,0x35e))+hasil2+enter+enter+(_0x2922db(-0x28f,-0xd5,-0x1f8,-0x1e7)+_0x2922db(-0x115,-0x114,-0x46,-0x97))+getMiningcoal(sender)+enter+('*Sisa\x20uang'+_0x2922db(-0xc7,-0x103,-0x15,-0x97))+checkATMuser(sender)+enter+enter+('Proses\x20ber'+_0x4e21b8(0x30e,0x31a,0x390,0x2ba)+_0x2922db(-0xaa,-0x1ef,-0x1eb,-0x147)+_0x4e21b8(0x3b2,0x3fb,0x437,0x435)+_0x4e21b8(0x3c1,0x403,0x3de,0x352)+_0x2922db(-0x12b,-0x114,-0xa4,-0xab))));await gameAdd(sender,glimit);break;case'lebur':const _0x4547e8={};_0x4547e8[_0x4e21b8(0x2c8,0x413,0x3cf,0x346)+'t']=_0x4e21b8(0x170,0x2b1,0x3a1,0x2a4)+'t';const _0x59112d={};_0x59112d[_0x4e21b8(0x3d8,0x3c3,0x3bb,0x34a)]=_0x2922db(-0xd4,-0x1b6,-0xee,-0x12b),_0x59112d['buttonText']=_0x4547e8,_0x59112d[_0x4e21b8(0x3af,0x296,0x377,0x2ac)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x4e21b8(0x348,0x198,0x37b,0x268)](prefix),'©\x20'+ownername,[_0x59112d]);const _0x58cbb0={};_0x58cbb0['displayTex'+'t']='Event\x20On';const _0x5671f6={};_0x5671f6[_0x2922db(-0x92,-0x153,-0x232,-0x10c)]=_0x4e21b8(0x2df,0x239,0x1fd,0x31d),_0x5671f6[_0x4e21b8(0x1f6,0x3a7,0x316,0x2ee)]=_0x58cbb0,_0x5671f6['type']=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x36a,0x1b6,0x3a2,0x283)](prefix),'©\x20'+ownername,[_0x5671f6]);bayar=args['join']('\x20');const hargaOre=0x1799*-0x1+0x1e06+-0x66b,hasil3=bayar*hargaOre;if(getMiningore(sender)<=0x76e+-0x2111+0x19a4)return reply(_0x4e21b8(0x425,0x39d,0x49a,0x467)+pushname+(_0x4e21b8(0x4e2,0x561,0x42a,0x4a6)+_0x4e21b8(0x272,0x2e0,0x481,0x382)+_0x2922db(-0x106,-0xe3,-0x144,-0x1a3)+_0x4e21b8(0x302,0x3b7,0x468,0x34b)));getMiningore(sender)>=-0x1491+0x5d2+0xec0&&(jualore(sender,bayar),addIngot(sender,hasil3),await reply('*「\x20LEBUR\x20B'+'ERHASIL\x20」*'+_0x4e21b8(0x192,0x1c4,0x248,0x294)+'Ore\x20dilebu'+_0x2922db(-0xa9,-0x175,0xb7,-0x48)+bayar+('\x0a*Ingot\x20di'+_0x4e21b8(0x43d,0x48d,0x529,0x42b))+hasil3+(_0x4e21b8(0x4aa,0x542,0x52e,0x4a1)+_0x4e21b8(0x417,0x46e,0x3c1,0x40d))+getMiningore(sender)));await gameAdd(sender,glimit);break;case _0x4e21b8(0x3e6,0x2ec,0x483,0x372):const _0x591356={};_0x591356[_0x4e21b8(0x41d,0x247,0x46a,0x346)+'t']=_0x2922db(-0x1c8,-0xeb,-0x203,-0x1b2)+'t';const _0x48fbe8={};_0x48fbe8[_0x2922db(-0x1c8,-0x1ed,-0x15,-0x10c)]='limit',_0x48fbe8['buttonText']=_0x591356,_0x48fbe8[_0x2922db(-0x1da,-0x247,-0xcd,-0x1aa)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x4e21b8(0x361,0x351,0x151,0x268)](prefix),'©\x20'+ownername,[_0x48fbe8]);const _0x3f862b={};_0x3f862b[_0x2922db(-0x54,-0x19,-0x1dd,-0x110)+'t']=_0x4e21b8(0x2a8,0x368,0x275,0x3a9);const _0x5cd1ec={};_0x5cd1ec[_0x4e21b8(0x3f7,0x21a,0x2fc,0x34a)]=_0x4e21b8(0x3ae,0x396,0x21c,0x31d),_0x5cd1ec[_0x4e21b8(0x2a2,0x22f,0x2b0,0x2ee)]=_0x3f862b,_0x5cd1ec[_0x2922db(-0xad,-0xf3,-0x2c6,-0x1aa)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x176,0x25b,0x322,0x283)](prefix),'©\x20'+ownername,[_0x5cd1ec]);bayar=args[_0x4e21b8(0x255,0x149,0x2b5,0x280)]('\x20');const hargaStone=0x1*0x12bb+0x2593*-0x1+0x1b*0xd4,hasil4=bayar*hargaStone;if(getMiningstone(sender)<=-0xa*-0x23e+0xdd7*-0x1+0x24*-0x3d)return reply(_0x4e21b8(0x594,0x4f6,0x3cf,0x467)+pushname+(_0x4e21b8(0x168,0x379,0x1b2,0x269)+_0x2922db(-0x7f,-0xba,0x9,-0x93)+_0x4e21b8(0x429,0x432,0x591,0x48b)+_0x4e21b8(0x338,0x24a,0x383,0x2a6)));getMiningstone(sender)>=0x2*0x10a2+-0xc83*-0x1+0xd9*-0x36&&(jualstone(sender,bayar),addKoinUser(sender,hasil4),await reply('*「\x20PENJUAL'+_0x2922db(-0x158,-0x6d,0x9,-0xfc)+_0x2922db(-0x16b,-0x196,-0x23a,-0x1c3)+enter+enter+(_0x2922db(-0x1e7,-0x20,-0xf4,-0xeb)+_0x2922db(-0x2b,0xb8,0xab,0x3f)+'*\x20')+bayar+enter+(_0x2922db(-0x158,-0x9a,-0x1d8,-0x170)+_0x4e21b8(0x417,0x3d5,0x2cc,0x35e))+hasil4+enter+enter+(_0x2922db(-0x2ee,-0x2ef,-0x238,-0x1d5)+_0x2922db(-0x1cf,-0x12f,-0x1aa,-0x97))+getMiningstone(sender)+enter+(_0x2922db(-0x16e,-0xe6,-0x160,-0xb1)+_0x4e21b8(0x3d7,0x466,0x3b6,0x3bf))+checkATMuser(sender)+enter+enter+(_0x2922db(-0x4e,-0x14,0x160,0x27)+_0x2922db(-0x88,-0x1c7,-0xe9,-0x19c)+_0x4e21b8(0x446,0x3ac,0x39b,0x30f)+'embayaran\x20'+_0x4e21b8(0x349,0x3c9,0x3a8,0x352)+_0x4e21b8(0x2e5,0x441,0x293,0x3ab))));await gameAdd(sender,glimit);break;case'jualingot':const _0x3b03a6={};_0x3b03a6['displayTex'+'t']=_0x2922db(-0x237,-0x190,-0x1e5,-0x1b2)+'t';const _0x27c4c4={};_0x27c4c4[_0x2922db(-0x6f,0x18,-0x1f7,-0x10c)]='limit',_0x27c4c4['buttonText']=_0x3b03a6,_0x27c4c4[_0x4e21b8(0x29c,0x356,0x1d7,0x2ac)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x2922db(-0x284,-0x212,-0x2bb,-0x1ee)](prefix),'©\x20'+ownername,[_0x27c4c4]);const _0x4762f5={};_0x4762f5[_0x2922db(-0xdd,-0x1a3,-0x127,-0x110)+'t']='Event\x20On';const _0x528578={};_0x528578[_0x2922db(-0x33,-0x48,-0x10d,-0x10c)]=_0x4e21b8(0x255,0x327,0x363,0x31d),_0x528578[_0x2922db(-0xa9,-0x1ee,-0xd0,-0x168)]=_0x4762f5,_0x528578['type']=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x178,0x3a3,0x1c2,0x283)](prefix),'©\x20'+ownername,[_0x528578]);bayar=args[_0x4e21b8(0x24e,0x2c6,0x248,0x280)]('\x20');const hargaIngot=0x5c02*0x1+0x39c7+0x3*-0x45b,hasil5=bayar*hargaIngot;if(getMiningingot(sender)<=-0x1*0x319+-0x1218+0x1532*0x1)return reply(_0x4e21b8(0x58f,0x4e7,0x464,0x467)+pushname+(_0x2922db(-0x26e,-0xa5,-0x1f,-0x144)+_0x2922db(-0xde,-0x7d,-0x156,-0x93)+'kup,\x20minim'+'al\x202\x20ingot'));getMiningingot(sender)>=-0x1c11+0xfad+0xc65&&(jualingot(sender,bayar),addKoinUser(sender,hasil5),await reply('*「\x20PENJUAL'+_0x2922db(-0x15f,-0xaf,0x1d,-0xfc)+'L\x20\x20」*'+enter+enter+('*Jumlah\x20In'+_0x4e21b8(0x429,0x34a,0x338,0x324)+_0x2922db(-0x1b8,-0x196,0x2d,-0x97))+bayar+enter+(_0x4e21b8(0x1eb,0x29a,0x388,0x2e6)+_0x2922db(-0x75,-0x7a,-0x1d2,-0xf8))+hasil5+enter+enter+('*Sisa\x20Ingo'+_0x2922db(-0x1c9,-0xd7,-0x108,-0xfd))+getMiningingot(sender)+enter+('*Sisa\x20uang'+_0x2922db(0x2b,-0x22,-0x8a,-0x97))+checkATMuser(sender)+enter+enter+(_0x4e21b8(0x489,0x446,0x490,0x47d)+'hasil\x20deng'+_0x2922db(-0x125,-0x23e,-0x1f8,-0x147)+_0x2922db(-0x6a,-0x115,-0x18,-0x21)+_0x4e21b8(0x220,0x3c3,0x33c,0x352)+_0x2922db(-0xce,-0x11c,-0x191,-0xab))));await gameAdd(sender,glimit);break;case _0x2922db(0x56,0xaa,0xc0,0x1b):const _0x1b129a={};_0x1b129a[_0x2922db(-0x1d7,-0x1a2,-0x1a3,-0x110)+'t']=_0x2922db(-0x2df,-0x15a,-0x1c3,-0x1b2)+'t';const _0x7784e1={};_0x7784e1[_0x2922db(-0x135,-0x86,-0x16a,-0x10c)]='limit',_0x7784e1[_0x4e21b8(0x3ea,0x1e7,0x304,0x2ee)]=_0x1b129a,_0x7784e1['type']=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang['limitg'](prefix),'©\x20'+ownername,[_0x7784e1]);const _0x4fafdb={};_0x4fafdb['displayTex'+'t']='Event\x20On';const _0x15900a={};_0x15900a[_0x4e21b8(0x316,0x425,0x3cc,0x34a)]=_0x4e21b8(0x3f1,0x37d,0x2fc,0x31d),_0x15900a[_0x4e21b8(0x2f9,0x243,0x391,0x2ee)]=_0x4fafdb,_0x15900a['type']=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x2f0,-0x187,-0xbb,-0x1d3)](prefix),'©\x20'+ownername,[_0x15900a]);bayar=args['join']('\x20');const hargaKayu=0x4c2b+0x88bd+-0x8e98,hasil6=bayar*hargaKayu;if(getNebangKayu(sender)<=-0x1c4c+0x12b1+0x99c)return reply(_0x2922db(0x71,0xe6,0xc3,0x11)+pushname+(_0x4e21b8(0x49a,0x3c6,0x413,0x46e)+_0x2922db(0xb6,-0x44,-0x9c,-0x11)+_0x2922db(-0x44,-0x48,0x5c,0x2d)+'l\x202\x20kayu'));getNebangKayu(sender)>=-0x1f*-0x57+0x113d*0x1+-0x1*0x1bc5&&(jualkayu(sender,bayar),addKoinUser(sender,hasil6),await reply(_0x2922db(0x80,0xb5,0x0,-0x74)+_0x4e21b8(0x237,0x336,0x291,0x35a)+_0x2922db(-0x112,-0x1e5,-0x17b,-0x1bb)+enter+enter+(_0x2922db(-0x248,-0x104,-0x6a,-0x167)+_0x4e21b8(0x161,0x18b,0x110,0x23f)+'*\x20')+bayar+enter+(_0x2922db(-0xcc,-0x69,-0x107,-0x170)+_0x4e21b8(0x250,0x473,0x29d,0x35e))+hasil6+enter+enter+(_0x4e21b8(0x4ac,0x401,0x3b9,0x3c9)+_0x4e21b8(0x30f,0x20f,0x276,0x2ce))+getNebangKayu(sender)+enter+(_0x4e21b8(0x360,0x3f3,0x41d,0x3a5)+_0x4e21b8(0x2f9,0x485,0x4c5,0x3bf))+checkATMuser(sender)+enter+enter+(_0x4e21b8(0x4ae,0x3cb,0x41c,0x47d)+_0x2922db(-0x23b,-0x76,-0x277,-0x19c)+_0x2922db(-0x24,-0xb3,-0x3d,-0x147)+_0x2922db(-0xbf,-0xbf,-0x86,-0x21)+'d88288bak1'+_0x4e21b8(0x4b5,0x285,0x285,0x3ab))));await gameAdd(sender,glimit);break;case'mining':const _0x2d4a78={};_0x2d4a78['displayTex'+'t']=_0x2922db(-0xd3,-0x26f,-0x1f3,-0x1b2)+'t';const _0x26cb74={};_0x26cb74[_0x4e21b8(0x392,0x264,0x21b,0x34a)]=_0x4e21b8(0x432,0x3f5,0x404,0x32b),_0x26cb74[_0x2922db(-0x265,-0x267,-0x289,-0x168)]=_0x2d4a78,_0x26cb74[_0x4e21b8(0x2f0,0x21a,0x353,0x2ac)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang['limitg'](prefix),'©\x20'+ownername,[_0x26cb74]);const _0x5f2508={};_0x5f2508['displayTex'+'t']='Event\x20On';const _0x4af13f={};_0x4af13f[_0x2922db(-0x1b9,-0x5f,-0xcf,-0x10c)]=_0x4e21b8(0x442,0x3ba,0x202,0x31d),_0x4af13f['buttonText']=_0x5f2508,_0x4af13f[_0x4e21b8(0x2e5,0x1a2,0x1a0,0x2ac)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x386,0x394,0x1e6,0x283)](prefix),'©\x20'+ownername,[_0x4af13f]);if(isOwner){const one=-0x25cbc537+0x1*-0x5f04a6a2+0x7d05fc*0x18a;addLevelingXp(sender,one),addLevelingLevel(sender,-0x2374+-0x2*0xa51+-0x3879*-0x1),reply(_0x4e21b8(0x4d6,0x2e7,0x43e,0x403)+_0x2922db(-0x8c,-0x98,-0x13d,-0x179)+'mi\x20dari\x20te'+_0x2922db(0xb8,-0x10c,0xc1,-0x41)+'girim\x20'+one+(_0x2922db(-0x19b,-0x2e7,-0x11e,-0x1c5)+'nda'));}else setTimeout(()=>{const _0x762ff7={'oGPgx':function(_0x37766f,_0xe0afdd){return _0x37766f*_0xe0afdd;},'irbKF':function(_0xd1ba,_0x19b4de,_0xde8f0){return _0xd1ba(_0x19b4de,_0xde8f0);},'ZmJTY':function(_0x48b8a7,_0x50e67b){return _0x48b8a7(_0x50e67b);}};function _0x2d1e5e(_0x4a99a0,_0x432a02,_0x57c0b9,_0x2ac3e5){return _0x4e21b8(_0x57c0b9,_0x432a02-0x1ec,_0x57c0b9-0x55,_0x2ac3e5- -0x2df);}const _0x54f4ac=Math['ceil'](_0x762ff7[_0x2d1e5e(-0x4e,-0x12,0xf3,-0x2a)](Math[_0x2d1e5e(-0x1be,0x4f,0x9b,-0x99)](),-0x1a*-0x2c2+0xaf7+-0x3*0xe89));function _0x6e73fc(_0x102934,_0x418725,_0x301a7,_0x2c9fbb){return _0x2922db(_0x102934-0x11c,_0x418725,_0x301a7-0x1c6,_0x102934-0x1df);}_0x762ff7[_0x2d1e5e(-0x1c,0xc2,-0x96,0x95)](addLevelingXp,sender,_0x54f4ac),_0x762ff7[_0x6e73fc(0x1af,0x1ef,0x225,0x1a5)](reply,_0x6e73fc(-0x1,-0x5,0x135,0xa2)+_0x6e73fc(0x1db,0x2e0,0x1e1,0x2cc)+pushname+(_0x2d1e5e(-0x35,0x17f,-0x11,0x89)+_0x2d1e5e(-0xb2,0x9,0xde,-0x50))+_0x54f4ac+_0x6e73fc(0x1fd,0xf9,0x2bb,0xcb));},-0x29*0xa4+-0x1b4b+0x4147),setTimeout(()=>{function _0x1eff0d(_0x4186c0,_0x462b3e,_0x17bbd0,_0x5577a5){return _0x2922db(_0x4186c0-0x3a,_0x5577a5,_0x17bbd0-0x15,_0x4186c0-0x18e);}function _0x1e3200(_0x4022a2,_0x13a58e,_0x5e7dcf,_0x55c34e){return _0x2922db(_0x4022a2-0xfa,_0x55c34e,_0x5e7dcf-0x24,_0x4022a2-0x263);}const _0x3980fc={'BtEbo':function(_0x27f490,_0x59993c){return _0x27f490(_0x59993c);}};_0x3980fc[_0x1eff0d(0x16e,0xdd,0x159,0xea)](reply,'Sedang\x20min'+'ing\x20selama'+_0x1eff0d(0xfa,0x81,0x9f,-0x4)+_0x1eff0d(-0x20,-0x40,-0x95,-0x27)+_0x1e3200(0x16c,0x23d,0xe5,0x243));},-0x2*0xe28+0x20b1+-0x1*0x461);await gameAdd(sender,glimit);break;case _0x2922db(-0xe9,0x6a,-0x70,0x41):const _0x32a655={};_0x32a655['displayTex'+'t']=_0x2922db(-0x18d,-0x1f9,-0x1e2,-0x1b2)+'t';const _0x2eeb8c={};_0x2eeb8c['buttonId']=_0x2922db(-0x108,-0x17f,-0x4c,-0x12b),_0x2eeb8c['buttonText']=_0x32a655,_0x2eeb8c[_0x2922db(-0x24e,-0x1bd,-0x290,-0x1aa)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x4e21b8(0x156,0x2d4,0x2aa,0x268)](prefix),'©\x20'+ownername,[_0x2eeb8c]);const _0x17c1a9={};_0x17c1a9[_0x2922db(-0x14a,-0x133,-0x249,-0x110)+'t']=_0x2922db(0x65,-0x16b,-0x1b7,-0xad);const _0x54e00d={};_0x54e00d[_0x4e21b8(0x227,0x436,0x216,0x34a)]=_0x4e21b8(0x268,0x245,0x3f5,0x31d),_0x54e00d[_0x2922db(-0x3b,-0x18e,-0x12d,-0x168)]=_0x17c1a9,_0x54e00d[_0x4e21b8(0x34b,0x397,0x231,0x2ac)]=0x1;if(!isEventon)return sendButMessage(from,lang['event'](prefix),'©\x20'+ownername,[_0x54e00d]);setTimeout(()=>{const _0xb1c33f={'morpE':function(_0x520422,_0x4eceec){return _0x520422*_0x4eceec;},'NCpaa':function(_0xe0ca57,_0x4bb67a,_0x57341d){return _0xe0ca57(_0x4bb67a,_0x57341d);},'bAagZ':function(_0x1f5c51,_0x1f10bd){return _0x1f5c51(_0x1f10bd);}};function _0x3feee3(_0x36e342,_0x3c858d,_0xc314e,_0x4bc8ca){return _0x4e21b8(_0xc314e,_0x3c858d-0x114,_0xc314e-0x1d9,_0x3c858d- -0xd8);}const _0x391b69=Math[_0x3feee3(0x203,0x2db,0x2d3,0x1c5)](_0xb1c33f[_0x319bcb(0x622,0x61a,0x6bb,0x631)](Math[_0x3feee3(0x49,0x16e,0xb5,0x54)](),0x136f+0x76f+-0x6b5*0x4));function _0x319bcb(_0x42121c,_0x277029,_0x489a48,_0x4c02c2){return _0x4e21b8(_0x4c02c2,_0x277029-0x1f1,_0x489a48-0x93,_0x277029-0x29c);}_0xb1c33f[_0x319bcb(0x533,0x574,0x5c7,0x526)](addIkan,sender,_0x391b69),_0xb1c33f[_0x3feee3(0x213,0x1a0,0x98,0x1af)](reply,_0x3feee3(0x1cc,0x19e,0x144,0xcb)+_0x319bcb(0x558,0x52e,0x412,0x490)+_0x319bcb(0x5a0,0x604,0x593,0x65a)+_0x319bcb(0x631,0x52b,0x4bd,0x417)+_0x391b69+(_0x3feee3(0x346,0x308,0x27d,0x2ed)+_0x319bcb(0x6a0,0x5b8,0x4a2,0x686)+'t'));},0x8*0x4c1+0x168*-0x7+-0x1078),setTimeout(()=>{function _0x42358b(_0x1fee85,_0x3b59b7,_0x25d314,_0x1d1d5f){return _0x4e21b8(_0x25d314,_0x3b59b7-0x0,_0x25d314-0x66,_0x3b59b7- -0xdb);}function _0x198cc4(_0xfefe46,_0x779c9b,_0x40261b,_0x17efe0){return _0x4e21b8(_0xfefe46,_0x779c9b-0x124,_0x40261b-0x1d7,_0x17efe0-0x1a7);}const _0x4f6241={'IEXSO':function(_0x253916,_0x57f2a0){return _0x253916(_0x57f2a0);}};_0x4f6241['IEXSO'](reply,_0x42358b(0x372,0x2bf,0x304,0x206)+'ancing,\x20si'+_0x198cc4(0x5e2,0x44a,0x678,0x57a)+_0x198cc4(0x5df,0x60b,0x781,0x64c));},0x83*-0x39+0x9a7*0x2+0x19*0x65),await gameAdd(sender,glimit);break;case _0x2922db(0x7c,0x7e,-0xc8,-0xb6):case _0x2922db(-0x205,-0x1a1,-0x5,-0x12d):const _0x4366aa={};_0x4366aa[_0x2922db(-0x6c,-0x128,-0x5e,-0x110)+'t']=_0x2922db(-0x161,-0x9f,-0xa0,-0x1b2)+'t';const _0x38d370={};_0x38d370[_0x2922db(0x28,-0x17,-0x1bf,-0x10c)]=_0x4e21b8(0x26c,0x25d,0x29d,0x32b),_0x38d370[_0x4e21b8(0x2a5,0x339,0x1de,0x2ee)]=_0x4366aa,_0x38d370['type']=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x4e21b8(0x2ba,0x14e,0x15a,0x268)](prefix),'©\x20'+ownername,[_0x38d370]);const _0x5e3528={};_0x5e3528['displayTex'+'t']=_0x2922db(-0x15f,-0x1ad,-0x41,-0xad);const _0x20c50f={};_0x20c50f[_0x2922db(-0xb9,-0x215,-0x37,-0x10c)]=_0x2922db(-0x186,-0xd0,-0x1cc,-0x139),_0x20c50f[_0x2922db(-0x99,-0x19a,-0x9c,-0x168)]=_0x5e3528,_0x20c50f[_0x2922db(-0x163,-0x206,-0x1a2,-0x1aa)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x149,-0x204,-0x117,-0x1d3)](prefix),'©\x20'+ownername,[_0x20c50f]);var tempsa=args[_0x2922db(-0x193,-0x290,-0x224,-0x1d6)]('\x20');if(tempsa==_0x2922db(-0xca,-0x1a8,-0x21c,-0x115)+'iver'){const _0x4da095={};_0x4da095[_0x2922db(0x141,0xe8,0x55,0x10)]=_0x2922db(0x46,0xe0,0xfb,0x3c),asu=await getBuffer(_0x2922db(-0x111,-0x7f,-0x21,0x29)+_0x2922db(0x3,-0x6e,0x118,0x6)+_0x4e21b8(0x303,0x322,0x322,0x28e)+_0x4e21b8(0x425,0x3d4,0x567,0x455)+_0x2922db(-0x24e,-0x1d4,-0x32,-0x16b),_0x4da095),setTimeout(()=>{const _0x25b83c={'muKMo':function(_0x1b062f,_0x246ef7){return _0x1b062f*_0x246ef7;},'FxNct':function(_0x3a9a07,_0x135ac4){return _0x3a9a07*_0x135ac4;},'Fnrmv':function(_0x3848d8,_0x34c3ce,_0x4c4314){return _0x3848d8(_0x34c3ce,_0x4c4314);},'IyZQZ':_0x448456(0x17b,0x1d2,0x3e3,0x2b2)},_0x1fcc14=Math[_0x448456(0x2a1,0x8c,0x2cd,0x1c5)](_0x25b83c[_0x448456(0x2d2,0x146,0x291,0x25e)](Math[_0x2e719d(0x3f1,0x473,0x373,0x2ba)](),0x11f6+0x1*0x21c7+0x3377*-0x1)),_0x581857=Math[_0x2e719d(0x55e,0x4dc,0x438,0x558)](_0x25b83c[_0x448456(0xcc,0x227,0x166,0x109)](Math['random'](),0x1054+-0x11*0xe3+-0x132));_0x25b83c[_0x2e719d(0x3ff,0x4fe,0x346,0x45e)](addStone,sender,_0x1fcc14);function _0x448456(_0x12320b,_0x4d2935,_0x42d44a,_0x3cafcf){return _0x2922db(_0x12320b-0x189,_0x12320b,_0x42d44a-0x1c0,_0x3cafcf-0x268);}_0x25b83c[_0x448456(0x61,-0xb5,0x14a,0x66)](addIkan,sender,_0x581857);function _0x2e719d(_0x1a86f9,_0x5b0eaa,_0x3f745e,_0x5b535e){return _0x2922db(_0x1a86f9-0x41,_0x3f745e,_0x3f745e-0x1f0,_0x1a86f9-0x601);}const _0x2e3eee={};_0x2e3eee[_0x2e719d(0x4f1,0x482,0x5f5,0x3e1)+'t']=_0x448456(0x1d3,0x28e,0x1c0,0x210)+_0x2e719d(0x456,0x3d5,0x3fd,0x398);const _0x47e2af={};_0x47e2af[_0x448456(0x87,0x122,0x87,0x15c)]='inv',_0x47e2af[_0x2e719d(0x499,0x5b4,0x4bd,0x384)]=_0x2e3eee,_0x47e2af[_0x448456(0x1a3,0x19d,0xee,0xbe)]=_0x25b83c[_0x2e719d(0x44c,0x36f,0x325,0x4fe)];const _0x539a15={};_0x539a15['quoted']=mek,sendButImage(from,_0x448456(0xa1,0xeb,-0x27,0x88)+_0x2e719d(0x440,0x4fd,0x404,0x466)+enter+enter+(_0x2e719d(0x514,0x5f6,0x4f3,0x638)+'patkan\x20*')+_0x1fcc14+('*\x20batu\x20dan'+'\x20*')+_0x581857+_0x2e719d(0x437,0x4c2,0x332,0x561)+enter+enter+(_0x2e719d(0x502,0x633,0x4bf,0x49a)+'ory\x20anda\x20d'+_0x2e719d(0x56c,0x508,0x461,0x531)+'\x20ketik\x20')+prefix+_0x448456(0x217,0x1d2,0x175,0x20a),'©\x20'+ownername,asu,[_0x47e2af],_0x539a15);},-0x3*-0xc18+0x1*-0x19d3+0x143),setTimeout(()=>{const _0x14b995={'IHAup':function(_0x13cc4a,_0xaf3b39){return _0x13cc4a(_0xaf3b39);},'bOMrh':'Sedang\x20ber'+_0x40f9dd(-0x52,-0xce,-0x45,0x3c)+_0x333659(0x758,0x6e2,0x752,0x7d7)+_0x333659(0x569,0x511,0x4fb,0x54d)};function _0x40f9dd(_0x446674,_0x6eb5d1,_0x3226b6,_0x4781c6){return _0x4e21b8(_0x4781c6,_0x6eb5d1-0x172,_0x3226b6-0x24,_0x6eb5d1- -0x471);}function _0x333659(_0x3e3143,_0x35acc2,_0x500a59,_0x3cbfa7){return _0x2922db(_0x3e3143-0x66,_0x3cbfa7,_0x500a59-0xc9,_0x3e3143-0x75d);}_0x14b995[_0x333659(0x71b,0x707,0x789,0x755)](reply,_0x14b995[_0x40f9dd(0x2a,-0x9,0x82,-0xd8)]);},0x9*-0x1ff+0x2*-0xca5+0x2b41);}else{if(tempsa===_0x4e21b8(0x317,0x1d4,0x155,0x272)+_0x4e21b8(0x356,0x219,0x377,0x2f3)){const _0x110b49={};_0x110b49[_0x2922db(-0xd1,-0x9a,0x53,0x10)]=_0x2922db(-0xc,0xa5,0x61,0x3c),gos=await getBuffer(_0x4e21b8(0x45b,0x44a,0x38b,0x47f)+_0x4e21b8(0x369,0x324,0x505,0x45c)+'ile/77c3ba'+_0x2922db(-0xd7,-0x244,-0x1bd,-0x148)+'9a30f.jpg',_0x110b49),setTimeout(()=>{const _0x3ec00f={'EWehF':function(_0x41a898,_0x43c7b0,_0x372c3b){return _0x41a898(_0x43c7b0,_0x372c3b);},'dMZCm':function(_0x14307c,_0x451575,_0x377f45){return _0x14307c(_0x451575,_0x377f45);},'FKkTj':function(_0x47e2d2,_0x4284ac,_0x336cf3,_0x369234,_0x5df661,_0x4a70cb,_0x402754){return _0x47e2d2(_0x4284ac,_0x336cf3,_0x369234,_0x5df661,_0x4a70cb,_0x402754);},'fmXZr':_0x38a4dc(0x414,0x408,0x41b,0x3cd)};function _0x344995(_0x42cfcb,_0x3c38ab,_0x35dbc3,_0x2a34ce){return _0x2922db(_0x42cfcb-0xbf,_0x2a34ce,_0x35dbc3-0x75,_0x42cfcb-0x471);}const _0x2f2fe5=Math[_0x38a4dc(0x327,0x2fe,0x45a,0x40b)](Math[_0x344995(0x261,0x134,0x134,0x293)]()*(0x20*-0xd+0x24ae+-0x22a0)),_0x3dda13=Math[_0x38a4dc(0x327,0x334,0x30a,0x290)](Math[_0x38a4dc(0x1ba,0x1f7,0x1ad,0x26d)]()*(0x1582+0xc9a+0x24*-0xf2));_0x3ec00f[_0x344995(0x49c,0x58a,0x59d,0x404)](addStone,sender,_0x2f2fe5),_0x3ec00f[_0x38a4dc(0x38f,0x26d,0x32a,0x48b)](addKayu,sender,_0x3dda13);const _0x2b67b7={};function _0x38a4dc(_0x5ab26c,_0x9af542,_0x11434f,_0x454c8e){return _0x2922db(_0x5ab26c-0x38,_0x454c8e,_0x11434f-0x120,_0x5ab26c-0x3ca);}_0x2b67b7[_0x38a4dc(0x2ba,0x2bf,0x30c,0x1e6)+'t']=_0x344995(0x419,0x374,0x34a,0x4b4)+_0x344995(0x2c6,0x39e,0x3dc,0x1ee);const _0x297fc2={};_0x297fc2[_0x38a4dc(0x224,0x2e5,0x28d,0x29a)]=mek,_0x3ec00f[_0x38a4dc(0x1f6,0x2dc,0x11a,0x135)](sendButImage,from,_0x344995(0x291,0x1ef,0x2b4,0x309)+_0x38a4dc(0x209,0x155,0xfb,0x342)+enter+enter+(_0x344995(0x384,0x332,0x346,0x403)+_0x38a4dc(0x2d8,0x1ed,0x36e,0x1f0))+_0x2f2fe5+(_0x344995(0x4be,0x489,0x48c,0x47c)+'\x20*')+_0x3dda13+_0x38a4dc(0x3c2,0x443,0x3cd,0x335)+enter+enter+(_0x344995(0x372,0x393,0x495,0x3cf)+_0x344995(0x2f6,0x326,0x2a0,0x356)+_0x38a4dc(0x335,0x412,0x465,0x428)+_0x344995(0x31e,0x41f,0x22e,0x40c))+prefix+_0x38a4dc(0x36c,0x486,0x267,0x475),'©\x20'+ownername,gos,[{'buttonId':_0x38a4dc(0x300,0x2fb,0x36b,0x292),'buttonText':_0x2b67b7,'type':_0x3ec00f[_0x38a4dc(0x1f2,0x12c,0x22d,0x2c5)]}],_0x297fc2);},0x3d2*-0x1+0x21c6+-0x123c*0x1),setTimeout(()=>{const _0x143745={'benwu':function(_0x2f9291,_0x3902cb){return _0x2f9291(_0x3902cb);},'mgFtK':_0x1be81d(0x322,0x21d,0x34e,0x336)+_0x1be81d(0x2f9,0x429,0x4f0,0x42a)+_0x1be81d(0x41e,0x3bf,0x420,0x4d8)+_0x1be81d(0x365,0x309,0x1b2,0x2e9)};function _0x1be81d(_0x32be18,_0xff198,_0x1ffcf0,_0x1f4c9e){return _0x4e21b8(_0xff198,_0xff198-0x145,_0x1ffcf0-0xbf,_0x1f4c9e-0x87);}function _0x39cd6a(_0x544761,_0x18b90a,_0x5cc54a,_0xeaaade){return _0x2922db(_0x544761-0x18a,_0x544761,_0x5cc54a-0xd9,_0x5cc54a-0x597);}_0x143745[_0x1be81d(0x2ef,0x3ba,0x41a,0x3c7)](reply,_0x143745[_0x39cd6a(0x457,0x2d1,0x3d1,0x4ea)]);},0x1dcf+0x2f*0x85+-0x277*0x16);}else{if(tempsa==='cochher\x20se'+'a'){const _0x262196={};_0x262196[_0x4e21b8(0x385,0x4e9,0x517,0x466)]=_0x4e21b8(0x40e,0x38a,0x470,0x492),seae=await getBuffer('https://te'+_0x4e21b8(0x4a8,0x55c,0x4a3,0x45c)+'ile/eabfc9'+'07cfc44738'+_0x2922db(-0x2c0,-0x11e,-0x98,-0x187),_0x262196),setTimeout(()=>{const _0x56f876={};_0x56f876['HhIZX']=function(_0x24cc27,_0x308406){return _0x24cc27*_0x308406;},_0x56f876[_0x3b33f4(0x3d9,0x2c9,0x300,0x212)]=_0x3b33f4(0x4e4,0x453,0x446,0x378)+_0xae6948(0x5c1,0x65f,0x59b,0x5c7),_0x56f876['UJJlO']=_0xae6948(0x7f4,0x8f5,0x71c,0x7bc);const _0x490f63=_0x56f876,_0x2831fd=Math['ceil'](_0x490f63[_0xae6948(0x59f,0x62c,0x74e,0x63e)](Math['random'](),0x7f4+-0x1fd*0x1+-0x2*0x2db));addIkan(sender,_0x2831fd);const _0x50a996={};_0x50a996[_0xae6948(0x54d,0x6f8,0x736,0x662)+'t']=_0x490f63[_0xae6948(0x5da,0x623,0x645,0x590)];const _0x290f3e={};function _0x3b33f4(_0x1ab378,_0x4b2275,_0x1b201e,_0x1ca2e0){return _0x4e21b8(_0x1b201e,_0x4b2275-0x11d,_0x1b201e-0x17a,_0x4b2275-0x55);}_0x290f3e[_0x3b33f4(0x361,0x39f,0x333,0x327)]='inv',_0x290f3e[_0xae6948(0x6fc,0x5db,0x544,0x60a)]=_0x50a996;function _0xae6948(_0x27fa72,_0x5e8ac5,_0x4b753c,_0x5d254b){return _0x2922db(_0x27fa72-0x110,_0x27fa72,_0x4b753c-0x104,_0x5d254b-0x772);}_0x290f3e[_0x3b33f4(0x238,0x301,0x279,0x2f6)]=_0x490f63['UJJlO'];const _0x5f1369={};_0x5f1369[_0x3b33f4(0x38a,0x305,0x41e,0x31c)]=mek,sendButImage(from,_0xae6948(0x4e4,0x6a3,0x5d3,0x592)+_0x3b33f4(0x348,0x2ea,0x227,0x40d)+enter+enter+(_0x3b33f4(0x2f3,0x3be,0x4a2,0x348)+_0x3b33f4(0x445,0x3b9,0x4c7,0x4c7))+_0x2831fd+_0x3b33f4(0x343,0x2e1,0x315,0x334)+enter+enter+(_0xae6948(0x711,0x6e5,0x5dd,0x673)+_0x3b33f4(0x2ff,0x330,0x437,0x42d)+'engan\x20cara'+_0x3b33f4(0x45f,0x358,0x2dd,0x321))+prefix+'inventory',_0x3b33f4(0x52d,0x476,0x43a,0x3e3)+ownername,seae,[_0x290f3e],_0x5f1369);},-0x1a70+0x266b+-0x43),setTimeout(()=>{const _0xa8997a={};function _0x31ee83(_0x4a84cf,_0x12ead3,_0x13fc69,_0x9be24a){return _0x2922db(_0x4a84cf-0x5,_0x12ead3,_0x13fc69-0x73,_0x13fc69-0x611);}_0xa8997a[_0x31ee83(0x32e,0x52e,0x428,0x3f0)]=_0x436be6(0x5d0,0x43f,0x567,0x4e4)+'petualang,'+_0x31ee83(0x4fa,0x5f0,0x60c,0x57e)+_0x436be6(0x3cb,0x56b,0x525,0x497);const _0x55289f=_0xa8997a;function _0x436be6(_0x206165,_0x486a60,_0x15e921,_0x1c8219){return _0x2922db(_0x206165-0xbc,_0x486a60,_0x15e921-0x171,_0x1c8219-0x68b);}reply(_0x55289f[_0x31ee83(0x547,0x438,0x428,0x480)]);},0x28d*0x8+-0x189d*0x1+0x435);}else{if(tempsa==='limingstal'+_0x4e21b8(0x44d,0x463,0x34d,0x34c)+'s'){const _0xc5d7b1={};_0xc5d7b1[_0x2922db(0xef,0x87,0xdb,0x10)]='get',seoe=await getBuffer('https://te'+_0x2922db(0x12c,0x1f,-0x18,0x6)+'ile/19a10f'+'f95c31af10'+'267e4.jpg',_0xc5d7b1),setTimeout(()=>{const _0x1c0031={'SzHMM':function(_0x2bd0d2,_0x5b41af){return _0x2bd0d2*_0x5b41af;},'TBzSM':function(_0x2ccf3a,_0x247a89,_0x3034d0){return _0x2ccf3a(_0x247a89,_0x3034d0);},'MKhQM':function(_0x12e2c8,_0x1b6b2f,_0x27fb9e){return _0x12e2c8(_0x1b6b2f,_0x27fb9e);},'TSNSm':function(_0x19a7cd,_0x23d006,_0x5e50c7,_0x1afe20,_0x35bccc,_0x39d2db,_0xe5cd1a){return _0x19a7cd(_0x23d006,_0x5e50c7,_0x1afe20,_0x35bccc,_0x39d2db,_0xe5cd1a);}},_0x4e02a5=Math[_0x33cc60(-0x1a,-0x34,-0x167,-0x4b)](_0x1c0031[_0x33cc60(0x5f,0x2a,-0xb0,0x21)](Math['random'](),0x21ca+-0x10ce+-0x10ca*0x1)),_0x796d0c=Math['ceil'](Math[_0x33cc60(-0xe5,-0x1a1,-0x129,-0x1d9)]()*(-0xe56*0x1+-0x1af6+0x299c));_0x1c0031[_0x1635df(0xcc,0xe2,0x1ea,0x11e)](addOre,sender,_0x4e02a5);function _0x33cc60(_0x374df8,_0x1c2f70,_0x488668,_0x603f29){return _0x2922db(_0x374df8-0xac,_0x603f29,_0x488668-0x35,_0x1c2f70-0x6f);}_0x1c0031['MKhQM'](addStone,sender,_0x796d0c);const _0x4803c5={};_0x4803c5[_0x33cc60(0x8a,-0xa1,-0x56,-0xbb)+'t']=_0x1635df(0x28a,0x120,0xfd,0x1cd)+_0x1635df(0x165,0xb5,0xab,0x7a);const _0xc3058={};_0xc3058['buttonId']=_0x33cc60(-0x53,-0x5b,-0x8b,-0x136),_0xc3058[_0x33cc60(-0x14b,-0xf9,-0x1c9,-0x227)]=_0x4803c5,_0xc3058[_0x1635df(0x52,0x79,-0x9e,0x7b)]='RESPONSE';function _0x1635df(_0x4b98bd,_0x6a486b,_0xee4147,_0xe7a34){return _0x2922db(_0x4b98bd-0x9c,_0x4b98bd,_0xee4147-0xc3,_0xe7a34-0x225);}const _0x4921b4={};_0x4921b4[_0x33cc60(0x1,-0x137,-0x9f,-0x6f)]=mek,_0x1c0031[_0x1635df(0x283,0x161,0x97,0x197)](sendButImage,from,_0x33cc60(-0x273,-0x171,-0x14f,-0x200)+_0x33cc60(-0x60,-0x152,-0xdf,-0x280)+enter+enter+(_0x33cc60(-0x3c,-0x7e,-0x26,-0x197)+_0x1635df(0x144,-0x2,0x12,0x133))+_0x4e02a5+('*\x20copper\x20o'+_0x33cc60(-0x236,-0x114,-0x1ac,-0xbf))+_0x796d0c+_0x1635df(0x50,0x1b9,-0xc,0xae)+enter+enter+('Cek\x20invent'+_0x1635df(0x1cb,0x17d,0x10a,0xaa)+_0x33cc60(0xba,-0x26,0x19,-0x90)+_0x33cc60(-0x29,-0xe4,-0x199,-0x1a3))+prefix+_0x1635df(0x138,0x23c,0xe0,0x1c7),'©\x20'+ownername,seoe,[_0xc3058],_0x4921b4);},-0x1*-0x1fd+-0xcae+0x1669),setTimeout(()=>{function _0x3ee6bd(_0x3464d5,_0x244d43,_0x2dedd6,_0x5b12a7){return _0x2922db(_0x3464d5-0x1ea,_0x3464d5,_0x2dedd6-0x109,_0x2dedd6-0x4be);}function _0x1d5333(_0x2ca741,_0x2d5b1d,_0x3c2493,_0x39ae2c){return _0x2922db(_0x2ca741-0x11c,_0x2d5b1d,_0x3c2493-0xb9,_0x3c2493-0x1f6);}reply(_0x1d5333(0x5d,0xbc,0x4f,0x12b)+_0x1d5333(0x160,0x24c,0x143,0x222)+_0x1d5333(0x20a,0xf7,0x1f1,0x1d3)+'tunggu...');},0xf8a+0x1cb7*-0x1+0xd2d);}else{if(tempsa==='chade\x20moun'+_0x2922db(-0x101,-0xdc,-0x2c8,-0x1fe)){const _0x5e2dfa={};_0x5e2dfa[_0x2922db(-0x82,-0x47,0x10c,0x10)]='get',seye=await getBuffer(_0x2922db(-0x24,0x124,-0x38,0x29)+'legra.ph/f'+_0x4e21b8(0x29d,0x299,0x2ef,0x29d)+_0x2922db(-0x24,-0x1ad,-0x1c9,-0xce)+_0x4e21b8(0x28b,0x21b,0x231,0x25e),_0x5e2dfa),setTimeout(()=>{const _0xac226={'OyqpI':function(_0x3448e6,_0x518e82){return _0x3448e6*_0x518e82;},'lQYVJ':function(_0x1738d5,_0x3d3ab2,_0x5e96ef){return _0x1738d5(_0x3d3ab2,_0x5e96ef);},'QalhZ':function(_0x3159a7,_0x5227c4,_0xc6f318,_0x4708a6,_0x28edc0,_0x3c7711,_0x150771){return _0x3159a7(_0x5227c4,_0xc6f318,_0x4708a6,_0x28edc0,_0x3c7711,_0x150771);},'YLVfm':_0x41bba4(0x43a,0x4e1,0x53b,0x51b)+_0x17c030(0x257,0x35d,0x48e,0x363),'LTgSr':_0x17c030(0x531,0x552,0x564,0x61f)},_0x3ca5b5=Math['ceil'](_0xac226[_0x17c030(0x262,0x379,0x263,0x367)](Math[_0x41bba4(0x395,0x28f,0x383,0x4a9)](),0x5*0x4cb+-0x2c+-0x17a3));function _0x41bba4(_0x24649c,_0x5a11a9,_0x483ad0,_0x49d515){return _0x4e21b8(_0x5a11a9,_0x5a11a9-0x6,_0x483ad0-0xc6,_0x483ad0-0x13d);}const _0x5c3e98=Math[_0x17c030(0x545,0x465,0x3d4,0x358)](Math[_0x41bba4(0x420,0x461,0x383,0x37d)]()*(-0x1*-0x55d+-0x170f+0x11ee));_0xac226[_0x41bba4(0x46e,0x689,0x59c,0x6d5)](addOre,sender,_0x3ca5b5);function _0x17c030(_0x405b0f,_0x3d30f7,_0x39950e,_0x5acaf6){return _0x2922db(_0x405b0f-0x44,_0x5acaf6,_0x39950e-0x158,_0x3d30f7-0x508);}_0xac226[_0x41bba4(0x507,0x5d7,0x59c,0x649)](addStone,sender,_0x5c3e98);const _0x453f1c={};_0x453f1c[_0x17c030(0x33b,0x362,0x2ae,0x2b3)]=mek,_0xac226[_0x17c030(0x1fd,0x327,0x2d2,0x352)](sendButImage,from,_0x41bba4(0x317,0x31e,0x3b3,0x4a2)+_0x41bba4(0x4eb,0x2e7,0x3d2,0x4dd)+enter+enter+('Kamu\x20menda'+'patkan\x20*')+_0x3ca5b5+('*\x20copper\x20o'+_0x17c030(0x2a1,0x385,0x3c0,0x44a))+_0x5c3e98+_0x41bba4(0x43e,0x36b,0x41c,0x480)+enter+enter+(_0x41bba4(0x47b,0x5ca,0x494,0x476)+_0x17c030(0x3bf,0x38d,0x3be,0x431)+_0x41bba4(0x508,0x4f2,0x4fe,0x4fe)+_0x17c030(0x3a0,0x3b5,0x3aa,0x4ee))+prefix+'inventory','©\x20'+ownername,seye,[{'buttonId':'inv','buttonText':{'displayText':_0xac226[_0x17c030(0x3ec,0x30d,0x3d4,0x413)]},'type':_0xac226[_0x41bba4(0x4b3,0x4db,0x524,0x627)]}],_0x453f1c);},-0x386+0xfb6+-0xa*0xc),setTimeout(()=>{function _0x3e2594(_0x55ae6d,_0x1d7651,_0x3d2b1f,_0x5123f2){return _0x4e21b8(_0x5123f2,_0x1d7651-0xb1,_0x3d2b1f-0x124,_0x1d7651-0x2a0);}const _0x59ce14={'LshFs':function(_0x11c725,_0x200a28){return _0x11c725(_0x200a28);},'QRBli':'Sedang\x20ber'+'petualang,'+'\x20silahkan\x20'+'tunggu...'};function _0x354baa(_0x47f285,_0x3bbd09,_0x1c4e9e,_0x58f493){return _0x4e21b8(_0x47f285,_0x3bbd09-0x158,_0x1c4e9e-0x151,_0x3bbd09- -0x40a);}_0x59ce14[_0x3e2594(0x6a0,0x5d6,0x680,0x5be)](reply,_0x59ce14[_0x3e2594(0x73d,0x6db,0x798,0x5f5)]);},0x8*-0x191+0x1c8f*0x1+0x1007*-0x1);}else{if(tempsa===_0x4e21b8(0x44c,0x418,0x544,0x4a2)+'ds'){const _0x4ea123={};_0x4ea123[_0x2922db(0x4f,-0xd3,0x61,0x10)]=_0x4e21b8(0x3a8,0x5bd,0x54c,0x492),siae=await getBuffer(_0x4e21b8(0x38a,0x55a,0x56c,0x47f)+_0x2922db(0x1d,-0x5,-0x9,0x6)+'ile/44fc68'+'4be9865c0f'+_0x2922db(-0x53,-0x1dc,-0xae,-0xb5),_0x4ea123),setTimeout(()=>{const _0x41d60b={'cSRKV':function(_0x4edfdc,_0x445484){return _0x4edfdc*_0x445484;},'WrHnt':function(_0x587e7f,_0x5215a2,_0x2723d1){return _0x587e7f(_0x5215a2,_0x2723d1);},'UKPWf':function(_0xe2ce33,_0x4717d8,_0x3b42bc,_0x45c76c,_0x22d8fe,_0x4d46f9,_0x2983ea){return _0xe2ce33(_0x4717d8,_0x3b42bc,_0x45c76c,_0x22d8fe,_0x4d46f9,_0x2983ea);},'dZQEs':'My\x20Invento'+_0x30a25b(0x257,0x14c,0x1a2,0x373),'zzCLc':_0x5db698(0x565,0x548,0x65d,0x4f9)},_0x5997f8=Math[_0x30a25b(0x35f,0x397,0x496,0x384)](_0x41d60b['cSRKV'](Math['random'](),0x578+-0x20d0+-0x1*-0x1bb2)),_0x54ba02=Math[_0x30a25b(0x35f,0x2d5,0x22c,0x2ca)](_0x41d60b['cSRKV'](Math[_0x5db698(0x30b,0x2bf,0x231,0x43d)](),-0x1*-0x22db+0x1*0x1675+-0x1*0x3923));_0x41d60b[_0x5db698(0x564,0x657,0x651,0x68b)](addStone,sender,_0x5997f8),_0x41d60b[_0x30a25b(0x44b,0x539,0x425,0x3d8)](addKayu,sender,_0x54ba02);const _0x34530f={};function _0x30a25b(_0x29a9d8,_0x5a6623,_0x599dd0,_0x55b1d2){return _0x2922db(_0x29a9d8-0x1db,_0x55b1d2,_0x599dd0-0x127,_0x29a9d8-0x402);}function _0x5db698(_0x16e4fa,_0x5c556c,_0x3032ed,_0x2b2b30){return _0x2922db(_0x16e4fa-0x1a0,_0x5c556c,_0x3032ed-0x19d,_0x16e4fa-0x51b);}_0x34530f['quoted']=mek,_0x41d60b[_0x30a25b(0x3a2,0x2b8,0x317,0x42f)](sendButImage,from,_0x30a25b(0x222,0x289,0x1e7,0x118)+_0x30a25b(0x241,0x369,0x27f,0x291)+enter+enter+(_0x30a25b(0x315,0x32f,0x22f,0x1e3)+_0x30a25b(0x310,0x3bb,0x258,0x3ac))+_0x5997f8+(_0x5db698(0x568,0x4dc,0x57f,0x63e)+'\x20*')+_0x54ba02+_0x5db698(0x513,0x5da,0x4ca,0x50d)+enter+enter+('Cek\x20invent'+_0x5db698(0x3a0,0x479,0x44f,0x269)+_0x30a25b(0x36d,0x334,0x37b,0x404)+_0x30a25b(0x2af,0x1c8,0x323,0x226))+prefix+_0x30a25b(0x3a4,0x4a6,0x481,0x332),'©\x20'+ownername,siae,[{'buttonId':_0x5db698(0x451,0x4b9,0x524,0x431),'buttonText':{'displayText':_0x41d60b[_0x30a25b(0x1ec,0xd8,0x17a,0x2c7)]},'type':_0x41d60b[_0x5db698(0x474,0x3dc,0x54c,0x3e3)]}],_0x34530f);},0x3*-0xcef+-0x9b*0x19+0x41a8),setTimeout(()=>{const _0x12b2df={};function _0x5659ec(_0xd5dd0f,_0x5e6fcf,_0x4b5ad3,_0x3256ff){return _0x4e21b8(_0x3256ff,_0x5e6fcf-0x5e,_0x4b5ad3-0x175,_0xd5dd0f- -0x3a3);}_0x12b2df['UtWSK']='Sedang\x20ber'+'petualang,'+_0x5659ec(0xae,0x193,0x7a,-0x60)+_0x3f2a9d(0x544,0x467,0x60b,0x510);const _0x220d9a=_0x12b2df;function _0x3f2a9d(_0x36d511,_0x4cfbaa,_0x3e56ca,_0x48ca9f){return _0x4e21b8(_0x48ca9f,_0x4cfbaa-0x37,_0x3e56ca-0x6c,_0x36d511-0x2e2);}reply(_0x220d9a[_0x5659ec(0x50,0x6b,0x42,0x187)]);},0x1d*-0x130+0x5f*-0x2+-0x4f*-0x72);}else{if(tempsa===_0x4e21b8(0x5c6,0x40c,0x42a,0x49a)+_0x4e21b8(0x3e3,0x205,0x377,0x300)){const _0x4380c5={};_0x4380c5[_0x4e21b8(0x3d8,0x481,0x38c,0x466)]='get',bbbb=await getBuffer(_0x4e21b8(0x3c8,0x3ac,0x359,0x47f)+'legra.ph/f'+_0x4e21b8(0x3dd,0x322,0x26a,0x320)+_0x2922db(-0x73,-0x154,-0x113,-0x14a)+'c4c0e.jpg',_0x4380c5),setTimeout(()=>{function _0x3d6d1f(_0x449391,_0x5b7e26,_0x4c04ad,_0x13bea8){return _0x4e21b8(_0x449391,_0x5b7e26-0xd2,_0x4c04ad-0x176,_0x13bea8- -0xb4);}const _0x15820e={'XyooY':function(_0x5a3a45,_0x19525b){return _0x5a3a45*_0x19525b;},'eRUJe':function(_0x3efd0a,_0x336fff,_0x6eb1ed){return _0x3efd0a(_0x336fff,_0x6eb1ed);},'qBArf':function(_0x427e2d,_0x25459f,_0x2a431c,_0x294009,_0x25ac2e,_0x1c55fb,_0x3b6564){return _0x427e2d(_0x25459f,_0x2a431c,_0x294009,_0x25ac2e,_0x1c55fb,_0x3b6564);},'OKfOr':_0x456f8e(0x1f2,0x166,0x1b0,0x167)},_0x5368ef=Math['ceil'](_0x15820e[_0x456f8e(0x3e,0xd0,0x141,0x179)](Math['random'](),0x11b*-0x17+-0x22e7+0x3d1c)),_0xb0e0c9=Math['ceil'](_0x15820e[_0x456f8e(0x29,0x1fe,0x141,0x217)](Math['random'](),-0x211d+-0x137c+0x34ad));_0x15820e[_0x456f8e(-0xcc,-0x13c,-0x2a,0xe6)](addStone,sender,_0x5368ef),addKayu(sender,_0xb0e0c9);const _0x1ec96e={};_0x1ec96e[_0x3d6d1f(0x1dd,0x334,0x36a,0x292)+'t']=_0x3d6d1f(0x2c0,0x24b,0x28c,0x34a)+_0x456f8e(-0xb3,0x32,-0x45,0xd6);const _0x2fad14={};function _0x456f8e(_0x5c82e8,_0x45f200,_0x223039,_0x81da37){return _0x2922db(_0x5c82e8-0x1f3,_0x45f200,_0x223039-0x136,_0x223039-0x166);}_0x2fad14[_0x3d6d1f(0x299,0x1b4,0x2ce,0x1fc)]=mek,_0x15820e['qBArf'](sendButImage,from,'*Congratul'+'ation\x20🎊*'+enter+enter+(_0x3d6d1f(0x3dc,0x3cb,0x3b4,0x2b5)+_0x456f8e(0x1a5,0x198,0x74,0x21))+_0x5368ef+('*\x20batu\x20dan'+'\x20')+_0xb0e0c9+_0x456f8e(0x5c,-0xaf,-0xd,-0x13)+enter+enter+('Cek\x20invent'+'ory\x20anda\x20d'+'engan\x20cara'+'\x20ketik\x20')+prefix+'inventory','©\x20'+ownername,bbbb,[{'buttonId':_0x456f8e(0x74,0x145,0x9c,0x180),'buttonText':_0x1ec96e,'type':_0x15820e[_0x456f8e(-0x1,-0x96,-0xe,-0x85)]}],_0x2fad14);},0x1*0x151b+0x218e+-0x2af1),setTimeout(()=>{function _0x3a7fdf(_0x3e383e,_0xb3c6b2,_0x2684a5,_0x3c90c2){return _0x2922db(_0x3e383e-0x1e7,_0x3c90c2,_0x2684a5-0x8c,_0x3e383e-0x146);}const _0x4bb6f9={'Hpove':function(_0x6ee61d,_0x5bd5b2){return _0x6ee61d(_0x5bd5b2);},'nvdYD':_0x29f716(0x5e4,0x46a,0x46d,0x4e3)+_0x3a7fdf(0x93,0xc8,0x76,0x10b)+_0x3a7fdf(0x141,0xe8,0x64,0x9a)+_0x29f716(0x3d1,0x568,0x4e5,0x496)};function _0x29f716(_0x4f3bcd,_0x143c20,_0x2e662a,_0x395365){return _0x4e21b8(_0x4f3bcd,_0x143c20-0x9f,_0x2e662a-0x1ed,_0x395365-0x234);}_0x4bb6f9[_0x29f716(0x5ba,0x509,0x4b0,0x570)](reply,_0x4bb6f9['nvdYD']);},0x55b*-0x3+-0x8*-0x2c9+-0x637);}else{const _0x474419={};_0x474419['method']=_0x4e21b8(0x5be,0x5c4,0x520,0x492),seae=await getBuffer(_0x2922db(0x15d,-0xff,0xf4,0x29)+_0x2922db(-0x4a,-0xb2,0x23,0x6)+_0x4e21b8(0x2d0,0x422,0x391,0x3c4)+_0x4e21b8(0x362,0x3b7,0x3bf,0x2b6)+_0x4e21b8(0x39c,0x419,0x2e3,0x32a),_0x474419),tesk=_0x2922db(-0xe9,0x107,-0x41,0x0)+_0x2922db(-0xa1,-0x19d,-0x16e,-0x125)+'INGIN\x20KAMU'+'\x20JELAJAHI*'+_0x2922db(-0x270,-0x25d,-0x1fb,-0x1ff)+_0x2922db(0x2e,-0x24,-0x6d,-0x23)+_0x4e21b8(0x24e,0x2c5,0x3a1,0x344)+_0x4e21b8(0x33d,0x26e,0x34d,0x24b)+'iens\x20Grass'+_0x2922db(-0x1a2,-0x16a,-0xaa,-0xe6)+_0x4e21b8(0x2a2,0x284,0x2ea,0x33f)+_0x4e21b8(0x47a,0x3b0,0x4a2,0x3da)+_0x4e21b8(0x1f0,0x39d,0x1fe,0x31e)+_0x2922db(-0x239,-0xfb,-0xcd,-0x178)+_0x4e21b8(0x3c5,0x3ab,0x36b,0x473)+_0x4e21b8(0x463,0x232,0x2eb,0x32d)+'ountain\x0a\x0aE'+_0x2922db(-0x26d,-0xf7,-0x11a,-0x21b)+'\x20'+prefix+('jelajah\x20co'+_0x2922db(-0x63,-0x153,0x3b,-0x4a)+_0x4e21b8(0x306,0x3fc,0x2d8,0x2cc)+_0x2922db(-0x82,-0x4,-0x3,-0xea)+'at\x20\x20_https'+_0x2922db(-0x137,0x69,-0xf8,-0x3d)+'tasynamege'+_0x4e21b8(0x4f1,0x523,0x4cc,0x4ab)+'om/_'),alpha['sendMessag'+'e'](from,seae,image,{'thumbnail':seae,'quoted':mek,'caption':tesk});}}}}}}}await gameAdd(sender,glimit);break;case _0x2922db(-0x72,0x8b,-0x9d,-0x90):const _0x13e873={};_0x13e873[_0x2922db(0x1c,-0xbf,-0x4a,-0x110)+'t']=_0x4e21b8(0x225,0x1b4,0x3aa,0x2a4)+'t';const _0x309538={};_0x309538[_0x4e21b8(0x3dc,0x21f,0x349,0x34a)]=_0x2922db(-0x1c6,-0x4c,-0x4c,-0x12b),_0x309538['buttonText']=_0x13e873,_0x309538[_0x2922db(-0xec,-0x1d5,-0x139,-0x1aa)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x2922db(-0x2ac,-0x214,-0x1ab,-0x1ee)](prefix),'©\x20'+ownername,[_0x309538]);const _0x3a6dbb={};_0x3a6dbb[_0x2922db(-0x230,-0x1c2,-0x210,-0x110)+'t']=_0x2922db(0x32,-0xa5,-0x96,-0xad);const _0x544dac={};_0x544dac[_0x2922db(-0x14b,-0x1e4,-0x201,-0x10c)]='event\x20on',_0x544dac[_0x4e21b8(0x3dd,0x32a,0x369,0x2ee)]=_0x3a6dbb,_0x544dac[_0x2922db(-0x203,-0xb3,-0x8f,-0x1aa)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x25a,-0x236,-0x17f,-0x1d3)](prefix),'©\x20'+ownername,[_0x544dac]);setTimeout(()=>{const _0x3709fc={'IGxaG':function(_0x5d95dc,_0x1ac467){return _0x5d95dc*_0x1ac467;},'SuZku':function(_0x50b4d8,_0x40897e){return _0x50b4d8*_0x40897e;},'JwDnS':function(_0x2720b1,_0x2fb445,_0x23065a){return _0x2720b1(_0x2fb445,_0x23065a);},'oMyJU':function(_0x4d2f26,_0x825bae){return _0x4d2f26(_0x825bae);}},_0x52b25f=Math[_0x38c602(0x45c,0x4c2,0x434,0x509)](_0x3709fc['IGxaG'](Math['random'](),-0x1*-0x1a38+-0xb1b+-0x1*0xeb9)),_0x4e8b87=Math[_0x38c602(0x464,0x592,0x55b,0x509)](_0x3709fc[_0x1b67e9(0x22d,0x168,0x25c,0x107)](Math[_0x1b67e9(-0xb4,-0x8e,0x44,-0x73)](),-0x117b+-0x12d6*0x1+0x1*0x246f)),_0x1433d5=Math['ceil'](Math[_0x38c602(0x3bd,0x2e7,0x2ea,0x39c)]()*(-0x1d23*0x1+-0x827+0x2554));_0x3709fc[_0x1b67e9(0x1a0,0xc8,0x39,0x26)](addStone,sender,_0x52b25f);function _0x38c602(_0x1ff890,_0x24acec,_0x535f05,_0x4af5ae){return _0x4e21b8(_0x535f05,_0x24acec-0x1b6,_0x535f05-0x8e,_0x4af5ae-0x156);}function _0x1b67e9(_0x3a5df9,_0x5600b6,_0x2f8f05,_0xfe0840){return _0x4e21b8(_0x3a5df9,_0x5600b6-0x7a,_0x2f8f05-0xf6,_0x5600b6- -0x2d4);}addCoal(sender,_0x4e8b87),_0x3709fc[_0x1b67e9(0x99,0xc8,0xcf,0x26)](addOre,sender,_0x1433d5),_0x3709fc['oMyJU'](reply,_0x38c602(0x4e8,0x4ca,0x4e5,0x3cc)+'ation\x20🎊*'+enter+enter+(_0x38c602(0x3bf,0x584,0x3b4,0x4be)+_0x1b67e9(-0x139,-0x45,0x69,-0x17e))+_0x52b25f+_0x38c602(0x2e1,0x504,0x502,0x3d3)+_0x4e8b87+(_0x38c602(0x396,0x2df,0x376,0x3a5)+_0x38c602(0x38e,0x408,0x32c,0x41f))+_0x1433d5+_0x1b67e9(0x122,-0x12,0x3f,0x2a));},-0xc3+-0x2+0xc7d*0x1),setTimeout(()=>{function _0x93c644(_0x4c2403,_0x1ab7dc,_0x5731e8,_0x1db65e){return _0x4e21b8(_0x4c2403,_0x1ab7dc-0x34,_0x5731e8-0xbb,_0x5731e8- -0x210);}function _0x27bd59(_0x1916cf,_0x10a0b5,_0x46f064,_0x2deaf4){return _0x2922db(_0x1916cf-0xf7,_0x1916cf,_0x46f064-0xc7,_0x2deaf4-0x615);}const _0x1bea94={'lJoYN':function(_0x962021,_0x215d0a){return _0x962021(_0x215d0a);},'xXZII':_0x93c644(0x263,0xb0,0x1e1,0x16a)+_0x27bd59(0x54f,0x664,0x656,0x57f)+_0x27bd59(0x5d2,0x600,0x625,0x592)+_0x93c644(0x31f,0x1a2,0x295,0x1b5)};_0x1bea94[_0x27bd59(0x34d,0x3ee,0x3e3,0x412)](reply,_0x1bea94[_0x27bd59(0x4db,0x590,0x663,0x577)]);},0x7af*-0x4+-0x67*-0x2f+-0x3f1*-0x3),await gameAdd(sender,glimit);break;case _0x2922db(0xc7,0xac,0x82,-0x67):const _0x219fd9={};_0x219fd9[_0x4e21b8(0x467,0x417,0x413,0x346)+'t']='Check\x20Limi'+'t';const _0x10dd7b={};_0x10dd7b[_0x2922db(-0xe5,-0x169,-0x95,-0x10c)]='limit',_0x10dd7b[_0x2922db(-0xe8,-0x262,-0x167,-0x168)]=_0x219fd9,_0x10dd7b['type']=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x4e21b8(0x33c,0x36f,0x2bf,0x268)](prefix),'©\x20'+ownername,[_0x10dd7b]);const _0x27c05c={};_0x27c05c[_0x2922db(-0x5f,0x20,-0xee,-0x110)+'t']='Event\x20On';const _0xe6bf12={};_0xe6bf12[_0x2922db(0x15,-0x41,-0x41,-0x10c)]=_0x4e21b8(0x20e,0x3e8,0x438,0x31d),_0xe6bf12[_0x4e21b8(0x337,0x234,0x34f,0x2ee)]=_0x27c05c,_0xe6bf12[_0x2922db(-0x2ca,-0x21c,-0x16f,-0x1aa)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x25e,-0x13a,-0xd7,-0x1d3)](prefix),'©\x20'+ownername,[_0xe6bf12]);setTimeout(()=>{const _0xf305c9={'EhvNF':function(_0x445b0d,_0x503ee4){return _0x445b0d*_0x503ee4;},'Igpul':function(_0x578773,_0x3855ba,_0x29af39){return _0x578773(_0x3855ba,_0x29af39);},'nOxet':function(_0x24392f,_0x59e8f9){return _0x24392f(_0x59e8f9);}};function _0x4c4094(_0x496e69,_0x214bed,_0x2134b3,_0x43dcf9){return _0x4e21b8(_0x496e69,_0x214bed-0xed,_0x2134b3-0x3d,_0x43dcf9-0x1e6);}function _0x13d8af(_0x191b6c,_0x312172,_0x4abbe4,_0x457be8){return _0x2922db(_0x191b6c-0xae,_0x312172,_0x4abbe4-0x12f,_0x4abbe4-0x562);}const _0x163e5d=Math[_0x4c4094(0x58a,0x519,0x515,0x599)](_0xf305c9[_0x4c4094(0x58d,0x5de,0x601,0x561)](Math[_0x13d8af(0x257,0x27f,0x352,0x378)](),-0x1f83*0x1+0x10*-0x60+-0x1*-0x2597));_0xf305c9[_0x13d8af(0x4e5,0x58d,0x4e2,0x545)](addKayu,sender,_0x163e5d),_0xf305c9[_0x4c4094(0x62b,0x71e,0x55c,0x68f)](reply,_0x13d8af(0x321,0x38e,0x382,0x2d5)+_0x13d8af(0x2cb,0x397,0x3a1,0x3ed)+enter+enter+(_0x4c4094(0x373,0x596,0x40f,0x4a1)+_0x4c4094(0x44d,0x482,0x62c,0x54a))+_0x163e5d+(_0x13d8af(0x495,0x380,0x43c,0x3f9)+_0x4c4094(0x37c,0x47b,0x4c8,0x44b)));},-0xb*0xdb+-0x8*-0x143+0xb09),setTimeout(()=>{function _0x21e5db(_0x50bf3f,_0x5f8848,_0x1987b5,_0x2b3a62){return _0x2922db(_0x50bf3f-0x8b,_0x2b3a62,_0x1987b5-0xf1,_0x50bf3f-0x48c);}function _0x3a9c14(_0x37d359,_0x2ec22e,_0x320b4b,_0x2127c6){return _0x2922db(_0x37d359-0x17d,_0x37d359,_0x320b4b-0x65,_0x2127c6-0x65a);}const _0x30bdaf={};_0x30bdaf['flGXn']=_0x3a9c14(0x60e,0x6fd,0x5e1,0x5f5)+'ambang,\x20si'+_0x21e5db(0x409,0x4b0,0x324,0x3df)+_0x3a9c14(0x5ba,0x594,0x61f,0x6a9);const _0x538781=_0x30bdaf;reply(_0x538781[_0x3a9c14(0x624,0x5a2,0x511,0x642)]);},0x379*-0x4+0x954+0x248*0x2),await gameAdd(sender,glimit);break;case _0x2922db(0xf1,-0x3a,0x47,-0x2d):const _0x30531e={};_0x30531e[_0x4e21b8(0x480,0x3bd,0x26d,0x346)+'t']=_0x4e21b8(0x3ae,0x362,0x2a9,0x2a4)+'t';const _0xc580b7={};_0xc580b7[_0x4e21b8(0x3a3,0x2f0,0x2c3,0x34a)]='limit',_0xc580b7['buttonText']=_0x30531e,_0xc580b7[_0x4e21b8(0x375,0x357,0x37d,0x2ac)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang['limitg'](prefix),'©\x20'+ownername,[_0xc580b7]);const _0x17d676={};_0x17d676['displayTex'+'t']=_0x4e21b8(0x2c3,0x408,0x330,0x3a9);const _0x5bf501={};_0x5bf501[_0x2922db(-0x222,-0xb9,-0x120,-0x10c)]=_0x4e21b8(0x360,0x2ed,0x318,0x31d),_0x5bf501[_0x4e21b8(0x3b8,0x2ec,0x284,0x2ee)]=_0x17d676,_0x5bf501['type']=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x2ea,-0x22c,-0x2ce,-0x1d3)](prefix),'©\x20'+ownername,[_0x5bf501]);setTimeout(()=>{function _0x2e79a3(_0x23a818,_0x4a974f,_0xf72c2c,_0x24943e){return _0x4e21b8(_0x24943e,_0x4a974f-0x10b,_0xf72c2c-0x9d,_0x23a818- -0x44d);}const _0x338b15={'uLIvo':function(_0x5ca079,_0x21b5ba){return _0x5ca079*_0x21b5ba;},'qQGAf':'merkurius','WSLgX':_0x55a479(-0x2f,0x202,0x106,0x21b),'kRNVL':_0x55a479(0x179,0x44,0xcb,0x77),'fhJjE':_0x55a479(0x7d,0x6f,0x7c,0x113),'vZmjq':_0x2e79a3(-0xaf,-0x1ce,-0x2d,-0x1a2),'egVMB':'neptunus','AXIgk':function(_0x520de3,_0x315bd1){return _0x520de3*_0x315bd1;},'SIezk':function(_0x1f33a8,_0x37949e,_0x956ecb){return _0x1f33a8(_0x37949e,_0x956ecb);},'NhBLO':function(_0x47cd11,_0x13e7e3){return _0x47cd11(_0x13e7e3);}};function _0x55a479(_0x987ba0,_0x1f5f91,_0x17a979,_0x28e237){return _0x2922db(_0x987ba0-0x1f1,_0x28e237,_0x17a979-0xa8,_0x17a979-0xde);}const _0x4d2663=Math['ceil'](_0x338b15[_0x2e79a3(-0x12c,-0x17c,-0x221,-0x145)](Math['random'](),0xa2a+0x237*0x4+-0x12a2)),_0x213534=[_0x338b15[_0x2e79a3(-0x11b,-0x210,-0x9f,-0x23d)],_0x338b15[_0x55a479(0x133,-0x3f,0xec,0xa0)],_0x338b15['kRNVL'],_0x338b15['fhJjE'],_0x338b15[_0x2e79a3(0x2b,0x55,0x153,0x94)],_0x338b15[_0x55a479(0xc6,-0x1d,0x11b,0x4b)],'uranus'],_0x29fd60=_0x213534[Math[_0x55a479(0x66,0x26,0x6d,0xac)](_0x338b15[_0x2e79a3(-0xae,0x24,0x61,-0xad)](Math[_0x55a479(-0x246,-0x1d3,-0x132,-0x245)](),_0x213534[_0x55a479(-0xd,0xc9,-0x3e,0xec)]))];_0x338b15[_0x55a479(0x1f9,0x32,0xd0,0x1fc)](addPlanet,sender,_0x4d2663),_0x338b15['NhBLO'](reply,_0x55a479(-0x1d7,0x8,-0x102,-0x1b8)+_0x2e79a3(-0x1b8,-0x1b3,-0x107,-0xcc)+enter+enter+(_0x2e79a3(-0x192,-0x263,-0x208,-0x105)+_0x55a479(-0x11a,-0x11e,-0x14,0xbf))+_0x4d2663+(_0x2e79a3(0x22,0xa9,0xe7,0xc7)+_0x55a479(-0x18e,-0x191,-0x87,-0x125))+_0x29fd60+(_0x2e79a3(0x2d,-0xf9,0xbd,-0x8d)+'\x20tahun'));},0x1*0xbe1+0x231c+-0x1*0x2345),setTimeout(()=>{function _0x5052f5(_0xedc87f,_0x4de282,_0x13594c,_0x5ee9b9){return _0x4e21b8(_0x5ee9b9,_0x4de282-0x1ec,_0x13594c-0x3d,_0x13594c-0x25e);}const _0x3a283f={'sDoVz':function(_0x4b3c10,_0x22f93c){return _0x4b3c10(_0x22f93c);}};function _0x3acb65(_0x28d71b,_0xbb16e6,_0x4f7252,_0x3c8d87){return _0x2922db(_0x28d71b-0xd3,_0x4f7252,_0x4f7252-0x88,_0x3c8d87-0x138);}_0x3a283f[_0x5052f5(0x66c,0x50b,0x5be,0x6d7)](reply,_0x5052f5(0x5fa,0x620,0x50d,0x4bc)+_0x5052f5(0x4ab,0x591,0x592,0x661)+_0x3acb65(-0x15e,0xfa,0x99,-0x3d)+_0x5052f5(0x548,0x6e1,0x5e3,0x5ff)+_0x3acb65(-0x50,-0x8,0x34,-0x37));},0xf7f+0x35c+0x12db*-0x1),await gameAdd(sender,glimit);break;case _0x2922db(-0xb4,-0x7c,-0xb3,-0x2e)+_0x4e21b8(0x385,0x494,0x36b,0x366):const _0x6f8fb7={};_0x6f8fb7[_0x2922db(0x4,-0x173,-0x20c,-0x110)+'t']=_0x4e21b8(0x26d,0x2ad,0x289,0x2a4)+'t';const _0x434c0e={};_0x434c0e[_0x4e21b8(0x3e8,0x47c,0x364,0x34a)]=_0x2922db(-0x70,-0x142,-0xce,-0x12b),_0x434c0e[_0x2922db(-0x233,-0xf0,-0x110,-0x168)]=_0x6f8fb7,_0x434c0e['type']=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang['limitg'](prefix),'©\x20'+ownername,[_0x434c0e]);const _0x1c55f0={};_0x1c55f0[_0x2922db(-0x130,-0x19b,-0x87,-0x110)+'t']=_0x4e21b8(0x3bd,0x42a,0x2d4,0x3a9);const _0x50b8a2={};_0x50b8a2[_0x2922db(-0x13e,-0x168,-0x204,-0x10c)]=_0x4e21b8(0x414,0x1fa,0x2a0,0x31d),_0x50b8a2[_0x2922db(-0x19d,-0x83,-0x1e5,-0x168)]=_0x1c55f0,_0x50b8a2[_0x4e21b8(0x264,0x2b0,0x392,0x2ac)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x4e21b8(0x36a,0x354,0x2dc,0x283)](prefix),'©\x20'+ownername,[_0x50b8a2]);buayar=body['slice'](0x8ef*0x3+0x1f29+-0x39e6);const hargakimia=-0x1*-0x10ef+0x218*0x11+-0x309f,dapetin=buayar*hargakimia;if(getBertualangPlanet(sender)<=-0x19ba+0x259a+-0x3*0x3f5)return reply(_0x4e21b8(0x23f,0x34e,0x2b2,0x23d)+pushname+(_0x4e21b8(0x181,0x2f6,0x37b,0x289)+'k\x20punya\x20ba'+_0x4e21b8(0x482,0x3cb,0x362,0x3d0)));getBertualangPlanet(sender)>=-0xcaa*-0x3+-0xccd+-0x1930&&(jualbahankimia(sender,buayar),addKoinUser(sender,dapetin),await reply(_0x4e21b8(0x3eb,0x377,0x339,0x3e2)+_0x4e21b8(0x35b,0x296,0x229,0x35a)+_0x2922db(-0x146,-0xd7,-0x2d3,-0x1c3)+enter+enter+('*Jumlah\x20ba'+_0x2922db(-0x158,-0x16f,-0x106,-0x1d1)+'ijual:*\x20')+buayar+enter+('*Uang\x20dida'+'pat:*\x20')+dapetin+enter+enter+(_0x2922db(-0x1b3,-0x164,-0x214,-0x118)+'nkimia:*\x20')+getBertualangPlanet(sender)+enter+(_0x2922db(-0xaa,-0x41,-0x5d,-0xb1)+_0x4e21b8(0x478,0x4d3,0x361,0x3bf))+checkATMuser(sender)+enter+enter+(_0x4e21b8(0x3fe,0x3fa,0x421,0x41a)+'berhasil\x20d'+_0x2922db(-0xde,-0x1e9,-0x7c,-0xd7)+_0x2922db(0x24,-0x17c,-0x15e,-0x46)+_0x2922db(0x24,-0x89,0x98,-0x31)+'ak1920kal')));await gameAdd(sender,glimit);break;case _0x4e21b8(0x441,0x2a2,0x340,0x347)+_0x2922db(-0xe7,-0x140,-0x133,-0xf0):teks=_0x2922db(-0x1c0,0x98,-0x123,-0xa0)+_0x4e21b8(0x20d,0x373,0x3fe,0x32e)+_0x4e21b8(0x2b6,0x28a,0x21e,0x2bf)+_0x4e21b8(0x241,0x321,0x43a,0x301)+pushname+(_0x4e21b8(0x19d,0x331,0x1d2,0x29c)+_0x4e21b8(0x481,0x316,0x3f2,0x3e1))+getBertualangPlanet(sender)+'_*';const _0xc2fa62={};_0xc2fa62[_0x2922db(-0x12c,-0x121,-0x168,-0x1a6)]=mek,alpha['sendMessag'+'e'](from,teks,text,_0xc2fa62);break;case _0x4e21b8(0x15e,0x36a,0x179,0x24e):case _0x2922db(-0x253,-0x5d,-0x1e5,-0x161):const _0x1b58e0={};_0x1b58e0[_0x4e21b8(0x31e,0x246,0x2e7,0x346)+'t']=_0x4e21b8(0x176,0x1d8,0x2e7,0x2a4)+'t';const _0x11502e={};_0x11502e[_0x4e21b8(0x264,0x2de,0x380,0x34a)]='limit',_0x11502e[_0x4e21b8(0x1ee,0x27b,0x2f1,0x2ee)]=_0x1b58e0,_0x11502e[_0x4e21b8(0x339,0x2dc,0x2ac,0x2ac)]=0x1;if(isGame(sender,isPremium,isCreator,isOwner,gcounttuser,glimit))return sendButMessage(from,lang[_0x2922db(-0x18e,-0x2a6,-0x303,-0x1ee)](prefix),'©\x20'+ownername,[_0x11502e]);const _0x3feee1={};_0x3feee1[_0x2922db(-0x1e7,-0x21,-0xe1,-0x110)+'t']=_0x2922db(0x5d,-0xb6,-0x17a,-0xad);const _0x5175fe={};_0x5175fe['buttonId']='event\x20on',_0x5175fe[_0x4e21b8(0x413,0x1f1,0x3d0,0x2ee)]=_0x3feee1,_0x5175fe[_0x2922db(-0x2a8,-0x1a8,-0x1c8,-0x1aa)]=0x1;if(!isEventon)return sendButMessage(from,lang[_0x2922db(-0x218,-0x2e3,-0x1f5,-0x1d3)](prefix),'©\x20'+ownername,[_0x5175fe]);cas=q;if(checkATMuser(sender)<cas)return reply('Uang\x20mu\x20ti'+_0x2922db(-0x5a,-0x104,-0x12f,-0xe9)+_0x2922db(-0x146,-0x1f2,-0xef,-0x192)+_0x4e21b8(0x511,0x3e0,0x559,0x462)+_0x4e21b8(0x1f7,0x278,0x1f0,0x24e));if(args['length']<0x7*-0x278+0x66b+0xade)return reply(_0x4e21b8(0x43e,0x4aa,0x4fe,0x487)+_0x4e21b8(0x2a3,0x247,0x2eb,0x2fb));if(isNaN(cas))return reply(_0x2922db(0x1f,-0x1a7,-0x1d9,-0x105)+_0x4e21b8(0x451,0x30e,0x317,0x3dc));const resg=['Kamu\x20MENAN'+_0x4e21b8(0x477,0x502,0x362,0x41e),_0x4e21b8(0x31d,0x25b,0x2ff,0x384)+'!!'];bayar=confirmATM(sender,cas),setTimeout(()=>{function _0x58e931(_0x2c0206,_0x6ff2e1,_0x148db6,_0x33cc47){return _0x4e21b8(_0x6ff2e1,_0x6ff2e1-0x189,_0x148db6-0x195,_0x33cc47-0x154);}const _0x4231e5={'UBFoU':function(_0x466a85,_0x4f8469){return _0x466a85(_0x4f8469);},'RXsbC':function(_0x24f16f,_0x2e95f0){return _0x24f16f*_0x2e95f0;},'CMrrE':function(_0x14386f,_0x3ed754){return _0x14386f+_0x3ed754;},'JOCid':function(_0x234efa,_0x3150f4){return _0x234efa==_0x3150f4;},'aJqLv':_0x58e931(0x5e7,0x457,0x683,0x58d),'plPxG':function(_0x2bb68b,_0x3dcb51,_0xcbbd51){return _0x2bb68b(_0x3dcb51,_0xcbbd51);},'mLLJj':function(_0x463bdc,_0x21f0a2){return _0x463bdc==_0x21f0a2;},'WBvup':'Kamu\x20KALAH'+'!!'},_0x572f80=Math[_0x58e931(0x641,0x3d0,0x3f1,0x507)](_0x4231e5[_0x298201(0x3af,0x2c8,0x429,0x3aa)](Math[_0x58e931(0x3c6,0x450,0x460,0x39a)](),-0x58f+0x186f+-0x12de));function _0x298201(_0x3ca1c9,_0x530c02,_0xae9c3f,_0x3c2e0e){return _0x2922db(_0x3ca1c9-0x49,_0x530c02,_0xae9c3f-0x2,_0x3c2e0e-0x49d);}const _0x53e8f2=_0x4231e5[_0x298201(0x4a6,0x416,0x376,0x38f)](cas,0x1df0+0x3a6*0x2+-0x94f*0x4),_0x5c0459=resg[Math['floor'](Math[_0x298201(0x2ac,0x1ac,0x39a,0x28d)]()*resg[_0x58e931(0x438,0x4fb,0x592,0x48e)])];if(_0x4231e5[_0x58e931(0x343,0x42a,0x37e,0x41c)](_0x5c0459,_0x298201(0x2e5,0x390,0x1f8,0x2e5)+_0x58e931(0x59f,0x49a,0x618,0x572)))_0x4231e5[_0x58e931(0x41a,0x2d1,0x30d,0x3f4)]==='zHiPL'?_0x4231e5['UBFoU'](_0x2ad9d8,_0x58e931(0x3d9,0x5d2,0x483,0x4ee)+_0x298201(0x21c,0x276,0x383,0x2cf)+_0x58e931(0x5dc,0x409,0x5a9,0x527)+'ggu...'):(_0x4231e5[_0x298201(0x4a6,0x4df,0x455,0x433)](addKoinUser,sender,_0x53e8f2),reply(_0x298201(0x301,0x322,0x3c9,0x2bd)+_0x298201(0x2d7,0x307,0x31f,0x2dc)+enter+enter+(_0x298201(0x4fc,0x3ca,0x473,0x44c)+'angkan\x20cas'+_0x298201(0x58d,0x466,0x413,0x45a)+_0x298201(0x3e3,0x46f,0x469,0x48b))+_0x53e8f2+'_*'+enter+enter+(_0x58e931(0x554,0x5ee,0x3f6,0x4e6)+_0x58e931(0x51a,0x565,0x491,0x53e)+_0x58e931(0x551,0x514,0x52b,0x542)+_0x58e931(0x497,0x509,0x413,0x456))));else _0x4231e5[_0x298201(0x34d,0x373,0x478,0x3bc)](_0x5c0459,_0x4231e5[_0x58e931(0x4eb,0x6cc,0x624,0x5e3)])?(confirmATM(sender,cas),_0x4231e5['UBFoU'](reply,_0x298201(0x358,0x278,0x2e3,0x352)+'\x20🥺\x20dan\x20keh'+_0x58e931(0x713,0x4b3,0x55d,0x5e5)+_0x298201(0x485,0x37e,0x423,0x489)+'\x20'+cas)):_0x4231e5[_0x58e931(0x39b,0x532,0x407,0x428)](reply,_0x298201(0x39c,0x352,0x317,0x3b1));},-0xda9*0x1+0xab9+0xea8),setTimeout(()=>{const _0x1a59de={'UDqFl':function(_0x598468,_0x209b97){return _0x598468(_0x209b97);}};function _0x369f36(_0x26bc8c,_0x45e18d,_0x2cff69,_0x5765d2){return _0x4e21b8(_0x5765d2,_0x45e18d-0x33,_0x2cff69-0x1aa,_0x26bc8c- -0xd3);}function _0x367d31(_0x41c707,_0x3e185e,_0x4d9e20,_0x2564a7){return _0x2922db(_0x41c707-0x1f0,_0x2564a7,_0x4d9e20-0x1ec,_0x4d9e20-0x763);}_0x1a59de['UDqFl'](reply,_0x369f36(0x1e9,0x289,0x199,0xb6)+_0x369f36(0x308,0x1df,0x3b7,0x288));},-0x9d6+0x1c8d+-0x12b7),await gameAdd(sender,glimit);break;case _0x2922db(-0xa3,0x19,-0x7b,-0xca):case _0x2922db(-0xe7,-0x22a,-0xab,-0x14c):case _0x4e21b8(0x39e,0x49c,0x3f5,0x3f8):inventory=_0x4e21b8(0x302,0x30b,0x30e,0x245)+'NVENTORY*\x20'+'🗃️\x20\x0a\x0a🎢\x20*COA'+_0x2922db(-0x5c,-0x30,-0x1d7,-0xc0)+getMiningcoal(sender)+(_0x2922db(-0x14e,-0x141,-0x24a,-0x111)+'\x20:\x20')+getMiningstone(sender)+(_0x4e21b8(0x4e0,0x418,0x49a,0x3f9)+_0x4e21b8(0x472,0x3bf,0x314,0x3d4))+getMiningore(sender)+(_0x4e21b8(0x2fd,0x4fe,0x512,0x3e3)+'ORE*\x20:\x20')+getMiningingot(sender)+(_0x2922db(-0x8b,-0x1b1,-0xcd,-0x17a)+':\x20')+getNebangKayu(sender)+(_0x4e21b8(0x3f8,0x570,0x434,0x450)+':\x20')+getMancingIkan(sender)+(_0x2922db(-0xd3,-0x11a,-0x16f,-0x181)+_0x4e21b8(0x434,0x35e,0x2dd,0x3a4)+_0x2922db(-0xe0,-0x145,-0x89,-0x18b)+_0x2922db(-0x1c6,-0x1b7,-0x23f,-0x218)+_0x4e21b8(0x387,0x178,0x26e,0x28b)+'nyak\x20inven'+_0x4e21b8(0x322,0x365,0x458,0x36f)),alpha['sendMessag'+'e'](from,pp_userz,image,{'thumbnail':Buffer['alloc'](0x1445*0x1+-0x8f5*-0x1+0x1d*-0x102),'quoted':mek,'caption':inventory});break;case _0x2922db(-0xce,-0x99,-0xf6,-0xd3):kodenyab=_0x2922db(-0x59,-0x156,-0x62,-0xb0)+_0x4e21b8(0x284,0x1a3,0x224,0x270)+_0x2922db(-0x285,-0x1ce,-0x31e,-0x1f5)+_0x4e21b8(0x3b6,0x3a7,0x517,0x4ad)+_0x4e21b8(0x554,0x4d1,0x518,0x45e)+'banian\x0a\x20\x20\x20'+'\x20ar\x20\x20\x20\x20\x20\x20\x20'+_0x4e21b8(0x44b,0x378,0x430,0x407)+'\x20\x20hy\x20\x20\x20\x20\x20\x20'+_0x4e21b8(0x461,0x3bb,0x486,0x461)+_0x2922db(-0x100,-0xc0,-0xc7,-0x1ac)+'\x20\x20\x20\x20\x20Catal'+'an\x0a\x20\x20\x20\x20zh\x20'+'\x20\x20\x20\x20\x20\x20\x20Chi'+_0x4e21b8(0x25c,0x349,0x230,0x35d)+_0x2922db(0x70,-0x4a,-0xd0,-0x10)+_0x2922db(-0xb4,-0x1a,-0x17d,-0x87)+_0x2922db(0x42,-0x4,0x121,0xa)+_0x4e21b8(0x393,0x3f9,0x23f,0x327)+_0x2922db(-0x2ef,-0xed,-0x221,-0x1ef)+_0x2922db(-0x267,-0x1bf,-0x1c9,-0x172)+_0x2922db(-0x16,-0x111,-0x1c3,-0x99)+'Chinese\x20(C'+'antonese)\x0a'+'\x20\x20\x20\x20hr\x20\x20\x20\x20'+_0x2922db(0x3d,-0x96,0x146,0xf)+_0x4e21b8(0x2aa,0x32d,0x270,0x362)+_0x4e21b8(0x3fd,0x2e8,0x3d4,0x3ba)+'ch\x0a\x20\x20\x20\x20da\x20'+'\x20\x20\x20\x20\x20\x20\x20Dan'+_0x2922db(-0x6d,-0x180,-0x15f,-0x11f)+'\x20\x20\x20\x20\x20\x20\x20\x20Du'+_0x4e21b8(0x361,0x34e,0x202,0x311)+_0x2922db(-0xa7,-0xfe,0x31,-0xfa)+_0x2922db(-0x1c1,-0x78,-0xe1,-0x100)+_0x2922db(-0x1c8,-0x1ec,-0xe4,-0xbf)+_0x4e21b8(0x353,0x309,0x2c2,0x3a7)+_0x4e21b8(0x38d,0x51f,0x3a5,0x45b)+_0x4e21b8(0x449,0x2ad,0x2f6,0x37a)+_0x4e21b8(0x1d1,0x1a7,0x2f4,0x263)+_0x2922db(-0xe1,0x6d,-0xf0,-0xcb)+_0x4e21b8(0x4be,0x435,0x3f5,0x3e4)+_0x4e21b8(0x35a,0x2ef,0x270,0x2d2)+_0x2922db(-0x272,-0xd3,-0x29c,-0x1df)+'\x20(United\x20S'+_0x4e21b8(0x318,0x37b,0x492,0x35b)+'\x20eo\x20\x20\x20\x20\x20\x20\x20'+_0x4e21b8(0x295,0x281,0x2fb,0x2b2)+'\x0a\x20\x20\x20\x20fi\x20\x20\x20'+_0x2922db(-0x10e,-0x158,-0x115,-0x11b)+'sh\x0a\x20\x20\x20\x20fr\x20'+_0x4e21b8(0x258,0x3bc,0x3c7,0x317)+_0x2922db(-0x1b4,-0x256,-0x266,-0x213)+_0x2922db(-0x1d7,-0xf9,0x34,-0xc6)+_0x2922db(-0x13d,0x14,-0x10,-0x3)+'l\x20\x20\x20\x20\x20\x20\x20\x20G'+'reek\x0a\x20\x20\x20\x20h'+_0x2922db(0xa7,0xa,-0x75,0x3e)+'aitian\x20Cre'+_0x2922db(-0x14b,-0x1e4,-0x1fe,-0x11e)+_0x2922db(0x63,0x63,-0x118,-0xa6)+_0x4e21b8(0x39e,0x1d0,0x1d7,0x2e9)+_0x2922db(-0xd1,-0x167,-0x1b8,-0x16c)+_0x4e21b8(0x2e5,0x1cc,0x3d3,0x2ae)+_0x2922db(0xc4,-0xa6,0x5a,0x56)+_0x2922db(0x4,-0xd0,-0x32,0x2f)+'c\x0a\x20\x20\x20\x20id\x20\x20'+_0x2922db(-0xc4,0x11f,-0x38,0x25)+_0x2922db(-0x16b,-0x13e,-0x265,-0x212)+_0x4e21b8(0x1d2,0x3e4,0x30d,0x309)+'\x20Italian\x0a\x20'+_0x2922db(-0x2a3,-0x2a5,-0x239,-0x1eb)+_0x4e21b8(0x48c,0x45e,0x2f6,0x423)+_0x4e21b8(0x2ad,0x2ba,0x365,0x326)+_0x2922db(-0xb0,-0x1f3,-0x12f,-0xc9)+_0x2922db(-0x1ab,-0xbc,-0x14b,-0xcf)+'\x20\x20\x20\x20\x20\x20\x20Lat'+'in\x0a\x20\x20\x20\x20lv\x20'+_0x4e21b8(0x222,0x227,0x2fd,0x316)+_0x2922db(-0x2dd,-0xe3,-0xda,-0x20c)+'k\x20\x20\x20\x20\x20\x20\x20\x20M'+_0x2922db(0x1f,-0x75,-0x136,-0x36)+_0x4e21b8(0x2c3,0x36e,0x313,0x315)+'\x20\x20\x20\x20Norweg'+_0x4e21b8(0x30f,0x327,0x48b,0x3cb)+_0x4e21b8(0x3cb,0x17e,0x27c,0x2ad)+_0x4e21b8(0x4fe,0x379,0x47a,0x43a)+_0x2922db(-0x1be,-0xed,-0x103,-0xc5)+_0x2922db(-0x1b7,-0x2ca,-0xf1,-0x1be)+_0x4e21b8(0x30e,0x265,0x222,0x2a5)+'\x20\x20\x20\x20Portug'+'uese\x20(Braz'+_0x2922db(0x36,0x95,-0x7a,-0xa5)+'\x20\x20\x20\x20\x20\x20\x20\x20Ro'+_0x2922db(-0x176,-0xb6,-0x53,-0x15a)+'\x20ru\x20\x20\x20\x20\x20\x20\x20'+_0x2922db(-0x27f,-0x15e,-0x2b9,-0x18c)+'\x20\x20\x20sr\x20\x20\x20\x20\x20'+_0x2922db(-0xff,0xdc,0xfc,-0x2c)+'\x0a\x20\x20\x20\x20sk\x20\x20\x20'+(_0x2922db(-0xce,-0x1e5,-0xe6,-0x1bc)+_0x4e21b8(0x499,0x43a,0x50a,0x499)+'\x20\x20\x20\x20\x20\x20Span'+_0x4e21b8(0x49a,0x4ae,0x40e,0x458)+_0x4e21b8(0x527,0x488,0x479,0x432)+'anish\x20(Spa'+_0x4e21b8(0x567,0x328,0x4e2,0x42f)+'-us\x20\x20\x20\x20\x20Sp'+_0x2922db(0x46,-0x165,0x19,-0xcd)+_0x2922db(-0x64,-0xdc,-0xaf,-0x77)+_0x4e21b8(0x52f,0x45a,0x2e0,0x418)+_0x2922db(-0x247,-0x2bd,-0x22b,-0x1e8)+'ili\x0a\x20\x20\x20\x20sv'+'\x20\x20\x20\x20\x20\x20\x20\x20Sw'+_0x4e21b8(0x4dd,0x479,0x3d7,0x3ae)+_0x2922db(-0x137,0x58,-0x13c,-0x7d)+_0x2922db(-0x270,-0x123,-0x18f,-0x166)+_0x2922db(-0x31f,-0x1ec,-0x1db,-0x215)+_0x2922db(-0x14e,-0x1a0,-0x69,-0xcc)+_0x2922db(-0x24e,-0x1e7,-0x220,-0x19e)+'urkish\x0a\x20\x20\x20'+_0x2922db(-0x71,-0x190,-0x1d1,-0x176)+_0x2922db(-0x40,-0x105,0xa9,0x7)+_0x4e21b8(0x4ed,0x33b,0x49b,0x409)+_0x4e21b8(0x4d7,0x4de,0x37c,0x48a)+'h\x0a\x20\x20\x20\x20'),reply(kodenyab);break;case'tts':const _0x1f4239={};_0x1f4239[_0x2922db(-0xac,-0x1b2,-0xd2,-0x1a6)]=mek;if(args[_0x2922db(-0x105,-0x19c,-0x1fc,-0x11c)]<0x2243+0x200+-0x2442)return alpha['sendMessag'+'e'](from,'Kode\x20bahas'+_0x4e21b8(0x4e9,0x506,0x449,0x42e)+'om?',text,_0x1f4239);kodenya=['af','sq','ar','hy','ca','zh',_0x4e21b8(0x34e,0x525,0x4a2,0x402),_0x4e21b8(0x41d,0x49c,0x3f7,0x404),_0x4e21b8(0x514,0x405,0x408,0x49d),'hr','cs','da','nl','en',_0x4e21b8(0x569,0x3a4,0x5b3,0x482),_0x4e21b8(0x378,0x3a6,0x38d,0x3b2),_0x2922db(-0x12d,-0x64,-0x1a0,-0x13e),'eo','fi','fr','de','el','ht','hi','hu','is','id','it','ja','ko','la','lv','mk','no','pl','pt',_0x2922db(-0x173,-0x112,-0x171,-0x1ec),'ro','ru','sr','sk','es','es-es',_0x4e21b8(0x31a,0x380,0x194,0x273),'sw','sv','ta','th','tr','vi','cy'];try{const gtts=require(_0x4e21b8(0x2f4,0x29c,0x298,0x27a))(args[-0xaf0+-0x153a+-0x2e*-0xb3]),_0x154d87={};_0x154d87[_0x2922db(-0x127,-0x196,-0x86,-0x1a6)]=mek;if(args[_0x2922db(-0x112,-0x10a,-0x216,-0x11c)]<-0xd1*0xd+0x41a+0x685)return alpha[_0x4e21b8(0x255,0x3dd,0x270,0x33d)+'e'](from,_0x4e21b8(0x28f,0x325,0x3f3,0x37d)+_0x4e21b8(0x43a,0x488,0x510,0x438),text,_0x154d87);dtt=body[_0x4e21b8(0x372,0x3d0,0x2de,0x304)](-0x11*-0x1fd+0x17+0x3c3*-0x9),ranm=getRandom(_0x4e21b8(0x351,0x35b,0x2dd,0x3d8)),rano=getRandom(_0x2922db(-0x99,0x7,-0xea,-0x2)),dtt['length']>-0xd2*0x1c+-0x3*0x67a+0x2cbe?reply(_0x4e21b8(0x343,0x196,0x2ab,0x248)+'banyakan\x20o'+'m'):gtts[_0x2922db(0x55,0x6d,-0x13c,-0x85)](ranm,dtt,function(){const _0x39014a={};function _0x13cfad(_0x375bda,_0x460a0b,_0x251ccd,_0x2d9bf6){return _0x2922db(_0x375bda-0x1b5,_0x2d9bf6,_0x251ccd-0x10b,_0x375bda-0x3be);}_0x39014a['vfyRI']=_0x1a8280(0x12e,0x215,0x21a,0x32),_0x39014a[_0x13cfad(0x3cb,0x4e1,0x46e,0x378)]=_0x1a8280(0x1e3,0x118,0x21b,0x1eb);function _0x1a8280(_0x4aa2db,_0x5eb285,_0x319089,_0x59f894){return _0x4e21b8(_0x59f894,_0x5eb285-0x171,_0x319089-0x109,_0x4aa2db- -0x131);}const _0xfc217f=_0x39014a;exec(_0x13cfad(0x3f8,0x437,0x31e,0x3ec)+ranm+('\x20-ar\x2048000'+_0x1a8280(0x12f,0x1ff,0x1b9,0x177)+_0x1a8280(0x124,0x197,0xdb,0x25e))+rano,_0x5e8942=>{function _0x3897da(_0x53aab9,_0x3e7121,_0x460d6a,_0x1356c2){return _0x13cfad(_0x460d6a-0x18c,_0x3e7121-0x1ec,_0x460d6a-0x55,_0x1356c2);}const _0x5f1636=_0xfc217f['vfyRI'][_0x5453e9(-0x10,0x2d,-0x146,-0x11c)]('|');let _0x5bc04b=0x5*0x679+-0x71*0x4a+-0x4d*-0x1;function _0x5453e9(_0x18f129,_0x225ecb,_0x395e3f,_0x145f98){return _0x1a8280(_0x18f129- -0x34b,_0x225ecb-0xdd,_0x395e3f-0x114,_0x145f98);}while(!![]){switch(_0x5f1636[_0x5bc04b++]){case'0':buff=fs[_0x5453e9(-0x91,-0xa5,0x40,0x87)+'nc'](rano);continue;case'1':if(_0x5e8942)return reply(_0xfc217f[_0x5453e9(-0x19,-0x135,-0x27,-0xc9)]);continue;case'2':fs[_0x3897da(0x361,0x3d6,0x38b,0x267)](rano);continue;case'3':const _0x39323d={};_0x39323d[_0x5453e9(-0x1cc,-0x15d,-0x20a,-0x168)]=mek,_0x39323d[_0x5453e9(-0x1b9,-0x21f,-0x21d,-0x2b9)]=!![],alpha['sendMessag'+'e'](from,buff,audio,_0x39323d);continue;case'4':fs['unlinkSync'](ranm);continue;}break;}});});}catch{reply('Contoh\x20pen'+_0x2922db(0xcf,-0xf8,-0x71,0x23)+_0x2922db(-0x2cd,-0x106,-0xd0,-0x1fc)+'hasa\x20text\x0a'+_0x2922db(-0x229,-0xc3,-0x16c,-0x158)+_0x2922db(0x16c,0xde,0x145,0x42)+'ning\x0aSilah'+_0x2922db(0x178,-0x16,0x113,0x4e)+'de\x20bahasa\x20'+_0x4e21b8(0x2ec,0x3c1,0x1d1,0x2d6)+_0x2922db(-0x185,-0x18f,-0x265,-0x209));}break;case'ghstalk':case'githubstal'+'k':case'ghuser':{if(args['length']<0x24bd+0x289*0x2+-0x2*0x14e7)return reply(_0x4e21b8(0x23e,0x272,0x37f,0x333)+'ntah\x20*'+prefix+(_0x2922db(0xa4,0xa9,0x6d,-0x66)+_0x2922db(0x5,0x5f,-0x108,0x13)));reply(lang[_0x4e21b8(0x401,0x316,0x373,0x2fa)]());let y=await fetchJson(_0x2922db(-0x18d,-0x1e3,-0x121,-0x1a2)+_0x2922db(-0x1e7,0x36,-0x16b,-0xfe)+_0x4e21b8(0x280,0x23a,0x29a,0x31a)+q),ppgt=await getBuffer(y[_0x2922db(0x86,-0x16b,-0xca,-0x4b)]),text=_0x2922db(-0x2d3,-0x1a6,-0x91,-0x199)+_0x2922db(-0x66,-0x88,-0xfd,-0x64)+_0x4e21b8(0x525,0x2df,0x33d,0x3fb)+enter+enter+(_0x4e21b8(0x3fe,0x39d,0x2ad,0x3b9)+_0x4e21b8(0x489,0x5ab,0x4e3,0x488))+y[_0x2922db(0x5f,-0x5,0x3a,0x38)]+enter+_0x2922db(0x95,-0x1d1,-0xb5,-0x9f)+y[_0x2922db(-0xe9,0x6f,-0x1d,-0x4e)]+enter+_0x4e21b8(0x1d5,0x1a0,0x3d6,0x2c0)+y['id']+enter+(_0x2922db(-0x156,0x4a,-0xf2,-0x8a)+':\x20')+y[_0x2922db(-0x12c,-0x292,-0xab,-0x1b7)]+enter+(_0x2922db(0x30,0xd6,-0xae,0x3)+'s\x20:\x20')+y[_0x4e21b8(0x36c,0x571,0x4a4,0x49b)]+enter+('♎\x20Followin'+_0x2922db(-0x2b,-0x17,-0x4b,-0xe8))+y[_0x2922db(-0x1f8,-0x99,-0x1ed,-0xdf)]+enter+_0x4e21b8(0x5d6,0x549,0x446,0x49c)+y[_0x4e21b8(0x36f,0x2d3,0x278,0x2ac)]+enter+(_0x2922db(-0x9e,-0x20,0x3,-0xf5)+':\x20')+y[_0x4e21b8(0x3e8,0x3c3,0x30e,0x417)]+enter+'🌐\x20Blog\x20:\x20'+y[_0x4e21b8(0x304,0x1f6,0x36d,0x305)]+enter+('📍\x20Location'+'\x20:\x20')+y[_0x2922db(-0x29,-0xba,-0x16e,-0xda)]+enter+_0x2922db(-0xb8,-0x84,-0x19a,-0xf1)+y[_0x4e21b8(0x2c6,0x227,0x206,0x2f9)]+enter+_0x2922db(-0xf5,-0x24f,-0x26d,-0x143)+y['bio']+enter+(_0x2922db(0x113,0xf0,-0x8b,0x48)+_0x4e21b8(0x32c,0x2e2,0x33d,0x3f5)+'\x20')+y[_0x2922db(0x42,-0x65,-0x111,-0xe5)+_0x2922db(0xe4,-0xf3,-0xe1,-0x19)]+enter+(_0x2922db(-0xfb,-0x1c7,-0x23e,-0x146)+_0x2922db(-0xf6,-0x11c,-0x16e,-0xbd))+y[_0x2922db(-0x4f,-0x137,0x27,-0x70)+'os']+enter+(_0x4e21b8(0x4e4,0x54a,0x38f,0x480)+_0x4e21b8(0x357,0x274,0x2fa,0x250))+y['public_gis'+'ts']+enter+(_0x2922db(0x23,0x68,-0x5a,-0xac)+':\x20')+moment(y[_0x2922db(0x7c,-0xcc,-0x152,-0xa2)])['tz']('Asia/Jakar'+'ta')[_0x4e21b8(0x301,0x245,0x125,0x256)]('id')[_0x4e21b8(0x415,0x3e0,0x3a5,0x3fa)](_0x4e21b8(0x1a6,0x30a,0x338,0x296)+_0x4e21b8(0x52e,0x3cf,0x4d7,0x3f7))+enter+(_0x2922db(0x149,-0x24,0x180,0x52)+'\x20')+moment(y[_0x2922db(0xe0,0xc7,0x9e,-0x47)])['tz']('Asia/Jakar'+'ta')['locale']('id')[_0x2922db(-0x97,0x2a,0xac,-0x5c)]('HH:mm:ss\x20D'+_0x4e21b8(0x51f,0x4a9,0x490,0x3f7))+enter+(_0x4e21b8(0x450,0x463,0x446,0x401)+'tps://gith'+_0x2922db(0x14a,0x34,0x43,0x36))+q;const _0x5c6316={};_0x5c6316[_0x4e21b8(0x33a,0x366,0x1cf,0x25d)]=text,_0x5c6316[_0x2922db(-0x106,-0x1e0,-0x1aa,-0x1a6)]=mek,alpha[_0x4e21b8(0x44b,0x2b3,0x3a6,0x33d)+'e'](from,ppgt,image,_0x5c6316),await limitAdd(sender,limit);}break;case _0x4e21b8(0x249,0x12e,0x2a9,0x247):const _0x4ce7eb={};_0x4ce7eb[_0x2922db(0x35,-0x6b,-0x80,-0x4c)]=_0x2922db(-0x203,-0xfd,-0x2c3,-0x1fd)+'695578';const _0x13df9a={};_0x13df9a[_0x4e21b8(0x3b3,0x387,0x248,0x2f2)]=_0x2922db(-0x7b,-0xfe,-0x164,-0xb4)+'RMANEN',_0x13df9a['products']=[_0x4ce7eb];const _0x537c97={};_0x537c97[_0x4e21b8(0x336,0x361,0x499,0x40a)]=_0x4e21b8(0x1e6,0x448,0x39d,0x31b)+_0x2922db(-0x151,-0x2f5,-0x20a,-0x1fa);const _0x2b340c={};_0x2b340c[_0x2922db(-0x20f,-0xda,-0x1d6,-0x164)]=_0x2922db(-0xe3,-0xae,0x5f,-0xb9)+_0x2922db(-0xc,-0x5f,-0x10b,0x17)+'AN',_0x2b340c[_0x2922db(0xbc,0x73,-0x146,-0x69)]=[_0x537c97];const _0x3aa1b1={};_0x3aa1b1['productId']='4626617390'+'737893';const _0x498fb4={};_0x498fb4[_0x2922db(-0x1e9,-0x1ec,-0x55,-0x164)]=_0x2922db(-0x193,-0x152,-0x2aa,-0x19f)+_0x2922db(0x6b,-0x64,-0x20,-0x89)+_0x4e21b8(0x141,0x2b6,0x2c6,0x242),_0x498fb4[_0x2922db(0x1,-0x15e,0xb2,-0x69)]=[_0x3aa1b1];const _0x20415f={};_0x20415f[_0x4e21b8(0x33a,0x279,0x33c,0x2b0)]=mek,lisduct=await alpha[_0x2922db(-0xb0,-0x16d,-0x13,-0xaa)+_0x4e21b8(0x4c6,0x35b,0x429,0x46b)+_0x2922db(-0xeb,-0x1b8,-0x17e,-0x16a)](from,{'listMessage':{'title':_0x4e21b8(0x333,0x3d5,0x44c,0x3a2)+_0x4e21b8(0x32d,0x242,0x3e5,0x306),'description':_0x4e21b8(0x273,0x48c,0x33b,0x380)+_0x4e21b8(0x3d5,0x4a3,0x4d2,0x3fd)+'lu?\x20Hmm.\x20S'+_0x2922db(0x106,-0x42,-0xb2,-0x9)+'solusinya,'+_0x2922db(-0xc0,-0x1a,-0x205,-0x114)+_0x2922db(-0xc3,-0x269,-0x256,-0x1da)+_0x2922db(-0x60,-0x2aa,-0x150,-0x191)+_0x2922db(-0x77,-0x1b7,-0x1b4,-0x169)+_0x2922db(0x5c,-0x1c,-0x40,0x1)+_0x4e21b8(0x28b,0x385,0x44d,0x394)+_0x2922db(-0x8f,-0x44,-0x29,-0x127)+_0x2922db(0x27,-0xd7,-0x1d1,-0x106)+'&\x20fix.\x20Tid'+_0x4e21b8(0x494,0x344,0x441,0x373)+'eli\x20apikey'+_0x2922db(-0x4c,0x30,0x94,-0xb)+_0x4e21b8(0x49a,0x4f9,0x439,0x42c)+_0x4e21b8(0x53d,0x594,0x4d3,0x475)+_0x4e21b8(0x431,0x560,0x54c,0x434)+_0x2922db(0x113,0x54,-0x7c,0x1a),'buttonText':'','listType':_0x4e21b8(0x4fd,0x45c,0x470,0x430)+'ST','productListInfo':{'productSections':[_0x13df9a,_0x2b340c,_0x498fb4],'headerImage':{'jpegThumbnail':fs['readFileSy'+'nc'](_0x2922db(-0xbf,0x27,-0xe1,-0x10d)+thumbnail)},'businessOwnerJid':'6288743504'+_0x2922db(0x7b,0x15,-0xed,-0x16)+_0x2922db(-0xb4,-0xe8,-0xef,-0x57)},'footerText':_0x4e21b8(0x508,0x4ce,0x3b7,0x41d)+_0x2922db(-0x30,-0x28,-0x1d1,-0xc7)+_0x4e21b8(0x136,0x15d,0x167,0x23c)}},_0x20415f);const _0xcb990b={};_0xcb990b['waitForAck']=!![],alpha['relayWAMes'+_0x4e21b8(0x569,0x4ec,0x3fc,0x489)](lisduct,_0xcb990b);break;case _0x4e21b8(0x30f,0x28b,0x330,0x395):{const flamingtext=_0x2922db(-0x1e8,-0x309,-0x122,-0x1f2)+_0x2922db(-0x7,-0x2a,-0x186,-0xbe)+_0x4e21b8(0x4cf,0x440,0x36b,0x476)+_0x2922db(-0x17d,-0xf,-0x196,-0xc8)+_0x4e21b8(0x4fc,0x2ed,0x481,0x3ca)+_0x2922db(-0x287,-0x181,-0x56,-0x186)+_0x4e21b8(0x34a,0x3cf,0x3a4,0x2e5)+'cript=sket'+_0x2922db(-0xd5,0x119,0x1,-0xf)+'Scale=true'+_0x2922db(-0x5e,0x1d,-0x1aa,-0x84)+_0x2922db(-0x30,-0x68,-0xa0,-0x5a)+_0x4e21b8(0x36c,0x40a,0x2fe,0x319)+_0x2922db(-0x277,-0x112,-0x81,-0x14f)+_0x2922db(-0x2fd,-0x1c9,-0x11c,-0x204)+_0x4e21b8(0x261,0x1fe,0x219,0x2fd)+_0x4e21b8(0x351,0x3a1,0x47b,0x416)+_0x4e21b8(0x381,0x44a,0x381,0x3e8)+_0x4e21b8(0x51d,0x2c9,0x4b7,0x400);if(isGroup)return reply(_0x4e21b8(0x2af,0x3ba,0x4d9,0x3d7)+_0x4e21b8(0x433,0x2e0,0x40a,0x376));this[_0x2922db(-0xa3,-0x68,-0x1fa,-0xc1)]=this[_0x2922db(0x16,-0x18f,-0x49,-0xc1)]?this['anonymous']:{},anonymousloc=await getBuffer(flamingtext+(_0x4e21b8(0x3d4,0x434,0x333,0x3d5)+'Chat'));const _0x45be48={};_0x45be48['displayTex'+'t']=_0x4e21b8(0x322,0x2b5,0x304,0x2a3);const _0x506248={};_0x506248['buttonId']=_0x2922db(0x47,0xee,0x46,-0x29),_0x506248[_0x4e21b8(0x217,0x339,0x286,0x2ee)]=_0x45be48,_0x506248[_0x4e21b8(0x2f9,0x177,0x1a0,0x2ac)]=0x1;const _0x412162={};_0x412162[_0x4e21b8(0x2fd,0x329,0x251,0x346)+'t']='START';const _0xab315c={};_0xab315c[_0x2922db(-0xcf,-0x13a,-0x1ca,-0x10c)]=_0x2922db(-0xe7,0x6c,0x137,0x1c),_0xab315c[_0x2922db(-0x251,-0x36,-0x1b3,-0x168)]=_0x412162,_0xab315c['type']=0x1;const _0x545677={};_0x545677['quoted']=mek,sendButLocation(from,_0x2922db(0x1f,0x15c,-0xa4,0x30)+'\x20Anonymous'+_0x4e21b8(0x413,0x2f1,0x241,0x2d9)+prefix+('start\x20-\x20Se'+_0x4e21b8(0x320,0x33d,0x2a2,0x3bb)+_0x4e21b8(0x1c6,0x2c5,0x3b1,0x2f8))+prefix+(_0x4e21b8(0x392,0x362,0x193,0x284)+_0x2922db(-0x10a,-0xfe,-0x10c,-0xa1))+prefix+('leave\x20-\x20Le'+_0x4e21b8(0x2bf,0x378,0x2e6,0x2b9)+'r'),_0x2922db(-0x104,-0x248,-0xd0,-0x1b4)+'t\x20'+botname+_0x4e21b8(0x1e2,0x2a9,0x198,0x2cd),anonymousloc,[_0x506248,_0xab315c],_0x545677);}break;case _0x4e21b8(0x456,0x4dc,0x4c6,0x496):case'leave':{if(isGroup)return reply(_0x4e21b8(0x35c,0x4b2,0x2ee,0x3d7)+_0x2922db(0x2b,-0xb4,-0x93,-0xe0));this['anonymous']=this[_0x4e21b8(0x383,0x382,0x3ab,0x395)]?this['anonymous']:{};let room=Object[_0x2922db(-0x51,-0x2,-0x1bc,-0x98)](this[_0x4e21b8(0x43f,0x286,0x48d,0x395)])[_0x2922db(0x8a,0xc,0x62,-0x6d)](_0x11a1d8=>_0x11a1d8[_0x2922db(-0xa5,-0x190,-0x71,-0x91)](m['sender']));if(!room){const _0x494d23={};_0x494d23['displayTex'+'t']=_0x2922db(-0x97,-0xc7,-0x1d7,-0x12e);const _0x32bd59={};_0x32bd59[_0x2922db(-0x1e1,0xd,-0x186,-0x10c)]=_0x2922db(-0xf4,-0xe1,0xa4,0x1c),_0x32bd59[_0x4e21b8(0x3e8,0x20a,0x276,0x2ee)]=_0x494d23,_0x32bd59[_0x4e21b8(0x269,0x19c,0x3bd,0x2ac)]=0x1;const _0x3bda0e={};_0x3bda0e['quoted']=mek,await sendButMessage(from,_0x4e21b8(0x481,0x4be,0x4aa,0x484)+_0x2922db(-0x1c4,-0x2b,-0x54,-0x160)+_0x4e21b8(0x278,0x31f,0x3cd,0x367)+_0x2922db(-0x6,-0x1d5,-0x1f9,-0x137)+'k\x20Dibawah\x20'+_0x4e21b8(0x377,0x489,0x374,0x46a)+'t\x20Anonymou'+_0x4e21b8(0x38d,0x3e6,0x206,0x2be),_0x2922db(-0x150,-0x157,-0x22f,-0x1b4)+'t\x20'+botname+_0x2922db(-0x18d,-0xc6,-0x1e8,-0xdd),[_0x32bd59],_0x3bda0e);throw![];}const _0x469fcc={};_0x469fcc['displayTex'+'t']=_0x4e21b8(0x3b0,0x41b,0x3b6,0x328);const _0x491415={};_0x491415[_0x2922db(-0xfe,-0x244,-0x68,-0x10c)]=_0x4e21b8(0x3c7,0x538,0x4a7,0x472),_0x491415['buttonText']=_0x469fcc,_0x491415[_0x4e21b8(0x20d,0x1c7,0x1eb,0x2ac)]=0x1;const _0x2598e4={};_0x2598e4[_0x4e21b8(0x31e,0x311,0x3a4,0x2b0)]=mek,sendButMessage(from,_0x4e21b8(0x1a7,0x304,0x351,0x24c)+_0x4e21b8(0x43f,0x2fc,0x3af,0x412)+_0x2922db(0x29,-0x19,0x87,0x51)+_0x2922db(0x7b,0x92,-0x70,-0x2f)+_0x4e21b8(0x2f2,0x22a,0x27a,0x2da),_0x2922db(-0xac,-0x2b8,-0x1df,-0x1b4)+'t\x20'+botname+'\x202021',[_0x491415],_0x2598e4);let other=room['other'](m[_0x4e21b8(0x42e,0x3b2,0x38e,0x406)]);const _0x236553={};_0x236553[_0x4e21b8(0x42e,0x40c,0x29b,0x346)+'t']=_0x4e21b8(0x2b1,0x41d,0x2df,0x328);const _0xfbe7fd={};_0xfbe7fd['buttonId']='start',_0xfbe7fd[_0x2922db(-0x118,-0x6c,-0xc0,-0x168)]=_0x236553,_0xfbe7fd[_0x4e21b8(0x1fa,0x287,0x304,0x2ac)]=0x1;const _0x54c246={};_0x54c246[_0x2922db(-0x120,-0x10c,-0x20f,-0x1a6)]=mek;if(other)await sendButMessage(other,_0x4e21b8(0x478,0x4ec,0x44f,0x477)+'\x20meninggal'+'kan\x20chat_','©\x20Copyrigh'+'t\x20'+botname+'\x202021',[_0xfbe7fd],_0x54c246);delete this[_0x4e21b8(0x31d,0x2a8,0x3b2,0x395)][room['id']];if(command===_0x2922db(-0xf3,0x8c,-0x5e,-0xd))break;}case _0x2922db(0xa8,-0x5c,0xa8,-0x32):case _0x2922db(-0x62,-0x2f,0x9e,0x1c):{if(isGroup)return reply(_0x4e21b8(0x426,0x4f2,0x2c5,0x3d7)+_0x2922db(-0x198,-0x155,-0xed,-0xe0));this[_0x2922db(0x77,-0x7e,-0x112,-0xc1)]=this[_0x4e21b8(0x314,0x2ef,0x409,0x395)]?this['anonymous']:{};if(Object['values'](this[_0x2922db(-0x1d6,-0x172,-0x5b,-0xc1)])[_0x2922db(0xc8,-0x171,0x9c,-0x6d)](_0x568b39=>_0x568b39['check'](m['sender']))){const _0x1f0164={};_0x1f0164['displayTex'+'t']='LEAVE';const _0x35817d={};_0x35817d[_0x2922db(-0x167,-0x134,-0x1f7,-0x10c)]=_0x4e21b8(0x506,0x530,0x480,0x449),_0x35817d['buttonText']=_0x1f0164,_0x35817d['type']=0x1;const _0x5b4fd7={};_0x5b4fd7[_0x2922db(-0x170,-0x100,-0x148,-0x1a6)]=mek,await sendButMessage(from,_0x4e21b8(0x341,0x3b5,0x345,0x43f)+'\x20Berada\x20Di'+'\x20Anonymous'+'\x20Chat\x20!','©\x20Copyrigh'+'t\x20'+botname+_0x4e21b8(0x484,0x34b,0x459,0x379),[_0x35817d],_0x5b4fd7);throw![];}let room=Object[_0x4e21b8(0x2fa,0x2e9,0x4b5,0x3be)](this['anonymous'])[_0x4e21b8(0x485,0x307,0x3f4,0x3e9)](_0x3d58d7=>_0x3d58d7['state']===_0x2922db(0x2f,0x3e,0x38,0x37)&&!_0x3d58d7[_0x2922db(-0x4f,0x83,0xa6,-0x91)](m[_0x2922db(0x59,0xb3,-0xca,-0x50)]));if(room){const _0x4dc7b8={};_0x4dc7b8[_0x2922db(-0xa,-0xdf,0x1a,-0x110)+'t']=_0x4e21b8(0x396,0x3ad,0x532,0x41f);const _0x1a1700={};_0x1a1700[_0x4e21b8(0x30e,0x247,0x28c,0x34a)]=_0x2922db(-0x23,-0xa5,0x64,-0xd),_0x1a1700[_0x4e21b8(0x330,0x304,0x3c6,0x2ee)]=_0x4dc7b8,_0x1a1700[_0x2922db(-0xb1,-0xa9,-0x146,-0x1aa)]=0x1;const _0x46f5f9={};_0x46f5f9[_0x4e21b8(0x42d,0x2a6,0x421,0x346)+'t']=_0x2922db(-0x1b6,-0x1e4,-0x18d,-0x1ad);const _0x11fc93={};_0x11fc93['buttonId']='skip',_0x11fc93[_0x4e21b8(0x281,0x27e,0x41e,0x2ee)]=_0x46f5f9,_0x11fc93['type']=0x1;const _0x1d5d0b={};_0x1d5d0b[_0x4e21b8(0x32c,0x24a,0x26c,0x2b0)]=mek,await sendButMessage(room['a'],_0x4e21b8(0x2f2,0x1d1,0x21b,0x2a7)+_0x4e21b8(0x320,0x433,0x29c,0x3ad)+'🎭',_0x4e21b8(0x2b1,0x266,0x241,0x2a2)+'t\x20'+botname+_0x4e21b8(0x2cb,0x307,0x45b,0x379),[_0x1a1700,_0x11fc93],_0x1d5d0b),room['b']=m['sender'],room['state']=_0x4e21b8(0x24a,0x2fd,0x319,0x249);const _0x355368={};_0x355368[_0x2922db(-0x168,-0x98,-0x93,-0x110)+'t']=_0x4e21b8(0x355,0x3bb,0x39a,0x41f);const _0x14550f={};_0x14550f['buttonId']='leave',_0x14550f[_0x2922db(-0x11f,-0x176,-0x1b4,-0x168)]=_0x355368,_0x14550f[_0x2922db(-0x1bc,-0x20e,-0xba,-0x1aa)]=0x1;const _0x3757a8={};_0x3757a8[_0x2922db(0x17,-0x124,-0x52,-0x110)+'t']='SKIP';const _0x4e88a7={};_0x4e88a7[_0x4e21b8(0x35f,0x226,0x3d4,0x34a)]='skip',_0x4e88a7[_0x4e21b8(0x1ce,0x3a0,0x334,0x2ee)]=_0x3757a8,_0x4e88a7[_0x4e21b8(0x33e,0x3c0,0x1c7,0x2ac)]=0x1;const _0x48caea={};_0x48caea['quoted']=mek,await sendButMessage(room['b'],_0x2922db(-0x220,-0x22e,-0x141,-0x1af)+_0x4e21b8(0x2c7,0x439,0x326,0x3ad)+'🎭',_0x4e21b8(0x379,0x16a,0x26e,0x2a2)+'t\x20'+botname+'\x202021',[_0x14550f,_0x4e88a7],_0x48caea);}else{let id=+new Date();this['anonymous'][id]={'id':id,'a':m[_0x2922db(-0xfd,0x14,-0x178,-0x50)],'b':'','state':_0x4e21b8(0x5bc,0x3a2,0x543,0x48d),'check':function(_0x4b198c=''){function _0x565ea7(_0x137e3a,_0x850cc,_0x81f8b4,_0x5bff02){return _0x2922db(_0x137e3a-0xd2,_0x81f8b4,_0x81f8b4-0x1cc,_0x5bff02-0x492);}return[this['a'],this['b']][_0x565ea7(0x3c5,0x4e0,0x3f3,0x3e4)](_0x4b198c);},'other':function(_0x338d34=''){function _0xc6c13b(_0x3a980c,_0x5b9902,_0x4c9d3a,_0x2abb4e){return _0x2922db(_0x3a980c-0x1f2,_0x4c9d3a,_0x4c9d3a-0x176,_0x5b9902-0x2b5);}const _0x28b1d2={};_0x28b1d2[_0xc6c13b(0x1c9,0x110,0x95,0xb4)]=function(_0x4bdcb8,_0x2aa783){return _0x4bdcb8===_0x2aa783;};function _0x3b6bfb(_0xad3f4a,_0x19060a,_0x147091,_0x2eac08){return _0x2922db(_0xad3f4a-0x1e5,_0x2eac08,_0x147091-0x21,_0x19060a-0x16b);}const _0x323a7d=_0x28b1d2;return _0x338d34===this['a']?this['b']:_0x323a7d[_0xc6c13b(-0x22,0x110,0xb2,0x1ff)](_0x338d34,this['b'])?this['a']:'';}};const _0x25c50d={};_0x25c50d['displayTex'+'t']=_0x2922db(0x6d,-0x92,-0x84,-0x37);const _0x502edb={};_0x502edb[_0x4e21b8(0x26d,0x473,0x2c3,0x34a)]='leave',_0x502edb[_0x2922db(-0x20d,-0xae,-0x295,-0x168)]=_0x25c50d,_0x502edb['type']=0x1;const _0x11acae={};_0x11acae[_0x2922db(-0x7d,-0x16a,-0x2de,-0x1a6)]=mek,await sendButMessage(m[_0x4e21b8(0x203,0x276,0x1de,0x279)],'⏳\x20_Menungg'+'u\x20Partner_',_0x4e21b8(0x1ef,0x21c,0x1f7,0x2a2)+'t\x20'+botname+_0x2922db(-0x16e,-0x20e,-0xc9,-0xdd),[_0x502edb],_0x11acae);}break;}case'next':case'skip':case _0x2922db(-0x23,0x46,-0xe8,-0x7):{if(isGroup)return reply(_0x2922db(0x119,-0x40,-0x108,0x4)+_0x4e21b8(0x33f,0x3d0,0x3ae,0x355)+_0x2922db(-0x224,-0x171,-0xb3,-0x1e5)+_0x4e21b8(0x336,0x359,0x30c,0x323));this[_0x2922db(-0x1da,-0x1f1,-0x10b,-0xc1)]=this[_0x2922db(-0x1ee,-0x1d7,0x1,-0xc1)]?this['anonymous']:{};let romeo=Object[_0x4e21b8(0x368,0x4db,0x484,0x3be)](this[_0x2922db(0x21,-0x142,-0x1d4,-0xc1)])[_0x4e21b8(0x40b,0x3b2,0x447,0x3e9)](_0x2940d3=>_0x2940d3['check'](m[_0x4e21b8(0x301,0x515,0x33f,0x406)]));if(!romeo){const _0x26133f={};_0x26133f['displayTex'+'t']=_0x2922db(-0x23d,-0x12e,-0x1c6,-0x12e);const _0x5c8949={};_0x5c8949[_0x2922db(-0x1f0,0xc,-0x1c5,-0x10c)]=_0x4e21b8(0x420,0x471,0x416,0x472),_0x5c8949[_0x4e21b8(0x287,0x335,0x288,0x2ee)]=_0x26133f,_0x5c8949[_0x2922db(-0x208,-0x213,-0x28c,-0x1aa)]=0x1;const _0x2f7dea={};_0x2f7dea[_0x2922db(-0x94,-0x21b,-0x202,-0x1a6)]=mek,await sendButMessage(from,'Kamu\x20Tidak'+'\x20Berada\x20Di'+'\x20Anonymous'+_0x2922db(-0x3b,-0x1d0,-0xbe,-0x137)+'k\x20Dibawah\x20'+_0x4e21b8(0x345,0x570,0x402,0x46a)+_0x4e21b8(0x2df,0x349,0x4c8,0x3bc)+'s\x20Chat',_0x2922db(-0x1c1,-0xfa,-0x203,-0x1b4)+'t\x20'+botname+_0x2922db(0x59,-0x1d,-0x1ce,-0xdd),[_0x5c8949],_0x2f7dea);throw![];}let other=romeo[_0x2922db(-0x4a,-0xe0,-0x56,-0x1f)](m[_0x4e21b8(0x359,0x50e,0x419,0x406)]);const _0x499a7c={};_0x499a7c[_0x2922db(-0xa,-0x237,-0x235,-0x110)+'t']='START';const _0x55f5f9={};_0x55f5f9[_0x4e21b8(0x373,0x2ee,0x44d,0x34a)]=_0x4e21b8(0x3e8,0x579,0x4f8,0x472),_0x55f5f9[_0x2922db(-0xac,-0x1bc,-0x1e4,-0x168)]=_0x499a7c,_0x55f5f9[_0x4e21b8(0x25c,0x35f,0x1d2,0x2ac)]=0x1;const _0x4765ff={};_0x4765ff[_0x2922db(-0x14e,-0x247,-0x132,-0x1a6)]=mek;if(other)sendButMessage(other,_0x2922db(-0x76,0x1c,0xc9,0x21)+_0x2922db(0x1f,-0x92,0xed,-0xc)+'kan\x20chat_',_0x2922db(-0xd1,-0x2d3,-0x11f,-0x1b4)+_0x2922db(-0x13b,-0x71,-0x2d,-0x131)+botname+'\x202021',[_0x55f5f9],_0x4765ff);delete this[_0x4e21b8(0x4ce,0x2ae,0x32c,0x395)][romeo['id']];let room=Object[_0x2922db(-0x152,-0x1cf,-0x52,-0x98)](this[_0x2922db(-0x1a7,-0x178,-0xf0,-0xc1)])[_0x4e21b8(0x411,0x513,0x2d2,0x3e9)](_0x26e3e9=>_0x26e3e9[_0x2922db(-0x283,-0x28d,-0x1a7,-0x16e)]===_0x4e21b8(0x4d6,0x391,0x537,0x48d)&&!_0x26e3e9[_0x2922db(-0xb5,-0x2e,-0x14a,-0x91)](m[_0x2922db(-0x2a,-0xe8,0x34,-0x50)]));if(room){const _0x477f4d={};_0x477f4d[_0x4e21b8(0x32c,0x346,0x3f7,0x346)+'t']=_0x2922db(-0x158,-0x14e,0xf3,-0x37);const _0x3d081c={};_0x3d081c[_0x4e21b8(0x478,0x3db,0x24c,0x34a)]=_0x4e21b8(0x377,0x345,0x4fd,0x449),_0x3d081c[_0x4e21b8(0x1e9,0x356,0x1be,0x2ee)]=_0x477f4d,_0x3d081c[_0x4e21b8(0x273,0x2d2,0x181,0x2ac)]=0x1;const _0x3eb33a={};_0x3eb33a[_0x2922db(-0x169,-0x1a4,-0xf8,-0x110)+'t']=_0x4e21b8(0x27a,0x1b6,0x1a4,0x2a9);const _0x13874d={};_0x13874d[_0x4e21b8(0x353,0x3e2,0x40d,0x34a)]=_0x2922db(-0x27f,-0x106,-0x136,-0x149),_0x13874d[_0x4e21b8(0x1be,0x3b3,0x344,0x2ee)]=_0x3eb33a,_0x13874d[_0x4e21b8(0x212,0x2bb,0x35b,0x2ac)]=0x1;const _0x1437f1={};_0x1437f1['quoted']=mek,await sendButMessage(room['a'],_0x4e21b8(0x1a2,0x223,0x1b4,0x2a7)+'itemukan_\x20'+'🎭',_0x4e21b8(0x2ae,0x293,0x1e6,0x2a2)+'t\x20'+botname+'\x202021',[_0x3d081c,_0x13874d],_0x1437f1),room['b']=m[_0x2922db(0x36,0x38,-0x176,-0x50)],room[_0x4e21b8(0x354,0x1b7,0x359,0x2e8)]=_0x4e21b8(0x19a,0x139,0x199,0x249);const _0x4b122c={};_0x4b122c['displayTex'+'t']=_0x2922db(-0xc6,0x18,-0x1f,-0x37);const _0x1bf2d2={};_0x1bf2d2[_0x2922db(-0x13b,-0x4,-0x1c9,-0x10c)]=_0x4e21b8(0x546,0x34c,0x4df,0x449),_0x1bf2d2[_0x2922db(-0x281,-0x7f,-0x204,-0x168)]=_0x4b122c,_0x1bf2d2[_0x2922db(-0x265,-0x1e9,-0x1a0,-0x1aa)]=0x1;const _0x37fbe6={};_0x37fbe6[_0x2922db(-0xdd,-0x169,-0xe9,-0x110)+'t']=_0x2922db(-0xc3,-0x26a,-0x203,-0x1ad);const _0x58808f={};_0x58808f[_0x2922db(-0x64,-0x122,-0x11a,-0x10c)]=_0x4e21b8(0x3d8,0x23f,0x39e,0x30d),_0x58808f[_0x4e21b8(0x419,0x30d,0x33b,0x2ee)]=_0x37fbe6,_0x58808f[_0x4e21b8(0x25d,0x2fa,0x1b3,0x2ac)]=0x1;const _0x1516a2={};_0x1516a2[_0x2922db(-0x1a1,-0x1cb,-0x15e,-0x1a6)]=mek,await sendButMessage(room['b'],_0x2922db(-0x278,-0xe6,-0xc5,-0x1af)+'itemukan_\x20'+'🎭',_0x2922db(-0xed,-0xd3,-0x165,-0x1b4)+'t\x20'+botname+_0x4e21b8(0x4a9,0x2c2,0x24c,0x379),[_0x1bf2d2,_0x58808f],_0x1516a2);}else{let id=+new Date();this[_0x4e21b8(0x328,0x2ef,0x3dc,0x395)][id]={'id':id,'a':m[_0x4e21b8(0x37c,0x51d,0x4dd,0x406)],'b':'','state':_0x2922db(0x9b,0x15a,0x45,0x37),'check':function(_0x220d5d=''){return[this['a'],this['b']]['includes'](_0x220d5d);},'other':function(_0x20a854=''){const _0x21d35b={};function _0x3b29ad(_0xe4b9a4,_0x57bcec,_0x33e783,_0x4382fa){return _0x4e21b8(_0x4382fa,_0x57bcec-0x1cf,_0x33e783-0x160,_0x57bcec- -0x216);}_0x21d35b[_0x5e0d96(0x4e0,0x5ce,0x4e0,0x618)]=function(_0x184c00,_0x1d5e12){return _0x184c00===_0x1d5e12;};const _0x231a24=_0x21d35b;function _0x5e0d96(_0xe1d4a2,_0x2acdf5,_0x300886,_0x2d6563){return _0x2922db(_0xe1d4a2-0xa5,_0x300886,_0x300886-0x68,_0xe1d4a2-0x5a3);}return _0x231a24['WtPfG'](_0x20a854,this['a'])?this['b']:_0x231a24[_0x5e0d96(0x4e0,0x493,0x409,0x609)](_0x20a854,this['b'])?this['a']:'';}};const _0x5bc8ea={};_0x5bc8ea[_0x2922db(0x11,-0x11a,-0xc1,-0x110)+'t']=_0x2922db(-0xe,-0x131,-0x13d,-0x37);const _0x1022ba={};_0x1022ba[_0x4e21b8(0x476,0x2b3,0x392,0x34a)]='leave',_0x1022ba[_0x2922db(-0x232,-0x203,-0xbb,-0x168)]=_0x5bc8ea,_0x1022ba['type']=0x1;const _0x3c73af={};_0x3c73af[_0x2922db(-0x23b,-0x17e,-0x16b,-0x1a6)]=mek,await sendButMessage(m[_0x4e21b8(0x228,0x1a2,0x223,0x279)],_0x4e21b8(0x3c0,0x4c7,0x4a2,0x422)+'u\x20Partner_',_0x2922db(-0x2b6,-0xe8,-0x2c1,-0x1b4)+'t\x20'+botname+_0x4e21b8(0x425,0x478,0x272,0x379),[_0x1022ba],_0x3c73af);}break;}}function _0x278e(){const _0x252e31=['\x20\x20\x20\x20no\x20\x20\x20\x20','\x20\x20\x20\x20\x20\x20\x20Lat','\x20\x20\x20\x20\x20\x20\x20Fre','en-us','eHeight=50','om/users/','5240798319','ama\x202\x20meni','event\x20on','\x20Woods\x0a🟠\x20L','\x20Chat,\x20Kli','ile/0c3fa8','uLIvo','HhIZX','tuk\x20Group!','got\x20dijual','t\x20\x20','e\x0a\x20\x20\x20\x20ko\x20\x20','h-tw\x20\x20\x20\x20\x20C','START','adventure','cffc2.jpg','limit','l\x202\x20ikan','\x0a🔴\x20Chade\x20M','hankimia\x20Y','r\x20sudah\x20di','*kayu\x20sela','AYAH\x20YANG\x20','qQGAf','Kirim\x20peri','tualang,\x20s','test','LshFs','ish\x0a\x20\x20\x20\x20nl','ole\x0a\x20\x20\x20\x20hi','exec','length','\x20\x20\x20\x20\x20Finni','Hpove','sendMessag','*Sisa\x20baha','bil\x20Woods\x0a','benwu','corbiens\x20r','\x20sekarang\x20','5TWFWtW','🔵\x20Cochher\x20','\x0a🛑\x20*STONE*','displayTex','sisabahank','CMrrE','./image/','buttonId','\x202\x20ore','l\x20mountain','390VxIlJh','211944zbzmcT','TBzSM','\x20sediakan\x20','Mua\x20taruha','d88288bak1','\x5c$&','.leave','k\x20Dapat\x20Di','glish\x0a\x20\x20\x20\x20','Cek\x20invent','i.github.c','t:*\x20','AN\x20BERHASI','tates)\x0a\x20\x20\x20','\x20\x20\x20\x20\x20\x20\x20\x20En','nese\x0a\x20\x20\x20\x20z','pat:*\x20','unggu...','sDoVz','🔏\x20Company\x20','an\x0a\x20\x20\x20\x20cs\x20','RXsbC','patkan\x20*','📧\x20Email\x20:\x20','imia','\x20Anonymous','\x20kamu\x20mend','Kamu\x20menda','Error\x20','*Jumlah\x20Ba','\x20Nama\x20temp','dak\x20mencuk','g\x20:\x20','tory','land\x0a🟣\x20Ger','twitter_us','jualstone','ak\x20perlu\x20b','irbKF','mLLJj','te\x20chat','following','*Jumlah\x20Co','\x202021','\x20\x20\x20\x20en-uk\x20','EhvNF','location','Textnya\x20ma','morpE','engan\x20nomo','Bosan\x20nump','.next','belum\x20cuku','kodebahasa','Kamu\x20KALAH','nggu...\x202\x20','string','an\x0a\x20\x20\x20\x20la\x20','d07dd22294','anish\x20(Uni','Thai\x0a\x20\x20\x20\x20t','h\x20(United\x20','inv','\x20\x20\x20\x20\x20\x20Kore','et-fu/prox','\x20Silahkan\x20','\x20\x20\x20\x20\x20\x20\x20\x20Ge','t\x20\x20\x20\x20\x20\x20\x20\x20P','Kumpulkan\x20','WtPfG','Semua\x20fitu','anonymous','L*\x20:\x20','en-au\x20\x20\x20\x20\x20','w6.flaming','epo\x20:\x20','Sedang\x20Mem','5480wWjdeF','JwDnS','ORDER\x20BOT\x20','saturnus','AXIgk','jelajah','cb5fa.jpg','JADIBOT\x20PE','petualang,','\x20bermain\x20g','*Sisa\x20uang','Kode\x20bahas','English\x20(A','includes','Event\x20On','📅\x20Created\x20','920kal','prepareMes','itemukan_\x20','edish\x0a\x20\x20\x20\x20','zzCLc','\x20\x20\x20\x20\x20\x20\x20\x20Hi','il)\x0a\x20\x20\x20\x20ro','en-uk','ceil','created_at','p\x20Partner\x0a','*Jumlah\x20ba','🥰\x20Name\x20:\x20','xXZII','📛\x20Username','\x20\x20\x20\x20\x20\x20\x20Cze','arch\x20Partn','t\x20Anonymou','zh-yue\x20\x20\x20\x20','values',':*\x20','ambang,\x20si','engan\x20cara','\x202\x20menit,\x20','u\x20belum\x20cu','ile/168577','check','nambang','isArray','TSNSm','*Sisa\x20Kayu','y_form.cgi','ian\x0a\x20\x20\x20\x20pl','🉑\x20Node\x20ID\x20','OT\x20VIA\x20HER','endsWith','hinese\x20(Ch','hankimia','save','&scaleWidt','lahkan\x20tun','\x20ORE*\x20:\x20','Anonymous\x20','Igpul','Only\x20priva','.mp3','ta\x20\x20\x20\x20\x20\x20\x20\x20','🟢\x20Chiltawa','asil!.','n\x20berapa?','973TkMQGc','11gDobEJ','ted\x20States','*\x20Ikan\x20sel','\x0a\x0a*_','*「\x20PENJUAL','\x0a🛠️\x20*INGOT\x20','Kingdom)\x0a\x20','floor','public_rep','LTgSr','ttern=Warn','find','uang\x20untuk','readFileSy','plPxG','products','\x20membeli\x20l','nebang','ghstalk*\x20z','Sedang\x20men','THUB\x20STALK','UtWSK','jupiter','Username\x20:','UKPWf','D/MM/YYYY','inventory','\x0a❄️\x20*COPPER','format','」-----*','h=800&scal','ang\x20bot\x20mu','My\x20Invento','tsapp.net','ing!&text=','➿\x20Url\x20:\x20ht','zh-cn','karena\x20and','zh-tw','Kamu\x20memen','sender','\x20Arabic\x0a\x20\x20','name','e\x0a\x20\x20\x20\x20cy\x20\x20','productId','avatar_url','rbiens\x20riv','e:*\x20','r\x20:*\x20','updated_at','r\x20pembayar','SzHMM','l\x20Keluar\x20D','ino\x20sebesa','IHAup','am\x20bot\x20men','fillTextPa','company',')\x0a\x20\x20\x20\x20sw\x20\x20','://www.fan','Penjualan\x20','dMZCm','p.net','Jika\x20Minat','G!!','LEAVE','acedonian\x0a','©\x20\x20','⏳\x20_Menungg','\x20\x20\x20Japanes','mulai','an\x20d88288b','ZmJTY','nonymous\x20C','jualbahank','goplanet','\x20\x20\x20Serbian','dapat:*\x20','ya\x20bayar\x20s','menu','anya\x20mana\x20','in)\x0a\x20\x20\x20\x20es','PRODUCT_LI','XyooY','-es\x20\x20\x20\x20\x20Sp','ens\x20River\x0a','\x20jadibot\x20p','embayaran\x20','BtEbo','other','na\x20om','iVfBq','lish\x0a\x20\x20\x20\x20p','QRBli','SuZku','ername','flGXn','Kamu\x20Masih','7326@s.wha','342skoNPn','ng\x20sebesar','mars','r\x20*_','\x20belum\x20cuk','h-cn\x20\x20\x20\x20\x20C','ch-name&do','SIezk','leave','\x20meninggal','\x20lagi.\x20Han','muKMo','aya\x20punya\x20','*\x20kayu','lanjut','\x0a🐬\x20*FISH*\x20','\x20silahkan\x20','ation\x20🎊*\x20','rman\x0a\x20\x20\x20\x20e','.ogg','ab77a6cea8','*PILIH\x20WIL','bot,\x20dll.\x20','ish\x0a\x20\x20\x20\x20es','🔥\x20Follower','Fitur\x20Tida','ustralia)\x0a','legra.ph/f','\x20Vietnames','\x20\x20\x20\x20\x20\x20\x20\x20Al','lQYVJ','ina)\x0a\x20\x20\x20\x20z','\x20\x20Armenian','melakukan\x20','FVqkX','WSLgX','\x20\x20\x20\x20Croati','method','Maaf\x20','bOMrh','eeoneofc','Untuk\x20Star','sageFromCo','split','UNTUK\x20JUAL','\x20kayu\x20kamu','*bahan\x20kim','ermanen.','jualkayu','start','\x20Mountains','Xp*','ekali\x20bisa','text.com/n','📛\x20_Partner','vZmjq','ggunaan\x20#t','*\x20selama\x202','\x20\x20\x20\x20\x20\x20Indo','Keluar','Proses\x20ber','venus','https://te','🌍\x20Public\x20G','EWehF','en-au','up,\x20minima','Kamu\x20Tidak','\x20\x20Icelandi','Welcome\x20To','Mau\x20taruha','\x20:\x20','sage','\x20\x20\x20\x20\x20\x20Wels','kup,\x20minim','ub.com/','WAITING','login','WBvup','ffmpeg\x20-i\x20','ilangan\x20ua','get','egVMB','t\x20\x20\x20\x20\x20\x20\x20\x20H','tu\x20dijual:','keluar','mancing','n\x20good\x20mor','k\x0a\x20\x20\x20\x20es\x20\x20','moobiens\x20g','followers','📝\x20Type\x20:\x20','zh-yue','📭\x20Twitter\x20','WrHnt','RESPONSE','\x0a\x0a*Sisa\x20Or','gerbil\x20woo','*\x20batu\x20dan','kan\x20cek\x20ko','ggu...','\x20ore\x20kamu\x20','ari\x20Sesi\x20A','📖\x20Update\x20:','nOxet','4299188CMiKLd','nerators.c','\x20\x20is\x20\x20\x20\x20\x20\x20','asa\x0a\x20\x20\x20\x20sq','xample\x20:\x0a-','Lihat\x20Item','maaf\x20','mendapatka','yu\x20dijual:','dZQEs','th\x20\x20\x20\x20\x20\x20\x20\x20','OKU','nch\x0a\x20\x20\x20\x20de','nesian\x0a\x20\x20\x20','\x0a🗃️\x20*USER\x20I','random','jadibot','Textnya\x20ke','CHATTING','vian\x0a\x20\x20\x20\x20m','Sea\x0a⚫\x20Moob','🏃\x20_Berhasi','ebahasa','judi','*\x20coal\x20,\x20d','its\x20:\x20','map','=100&fillT','lJoYN','Fnrmv','libopus\x20','locale','\x0a\x0a\x0a⚪\x20Corbi','tain','5316546641','ts\x20kode\x20ba','YLVfm','270456','caption','695a8.jpg','4|0|1|3|2','\x20-vn\x20-c:a\x20','\x20\x20\x20\x20\x20\x20\x20Bah','tunggu...','\x20\x20\x20\x20Englis','https://ww','ma\x202\x20menit','3389TOockj','hinese\x20(Ta','limitg','\x20stone\x20kam','pt-br','\x20\x20\x20ja\x20\x20\x20\x20\x20','al\x20dijual:','EdSFy','\x20\x20\x20\x20\x20\x20Swah','*Sisa\x20coal','a\x0a\x20\x0a\x20\x09Code','gunakan\x20Un','chiltawa\x20w','es-us','uWFSH','QalhZ','*Congratul','\x20\x20\x20English','bAagZ','chat','./lib/gtts','LYPVn','kamu\x20bisa\x20','*\x20batu\x20,\x20*','fmXZr','copyNForwa','join','*Sisa\x20Batu','FKkTj','event','skip\x20-\x20Ski','hankimia\x20d','.start','.stop','ancing,\x20si','\x20kamu\x20tida','text','n\x20lebih\x20ba','*\x20ikan','fromMe','ile/00018d','apatkan\x20*','mgFtK','Xp\x20untuk\x20a','ation\x20🎊*\x0a\x0a','L\x20」*','\x0a\x0a*Jumlah\x20','ation\x20🎊*','HH:mm:ss\x20D','unlinkSync','ortuguese\x0a','116271fXAyyx','\x20\x20\x20\x20\x20Slova','L\x20\x20」*','_\x20adalah:*','ile/efdcd7','Kamu\x20MENAN','node_id','aJqLv','IyZQZ','©\x20Copyrigh','MENU','Check\x20Limi','\x20\x20\x20\x20pt-br\x20','al\x202\x20stone','_Partner\x20D','silahkan\x20t','SKIP','\x0a\x20\x20\x20\x20ca\x20\x20\x20','ry\x20🗃️','type','\x20\x20\x20\x20\x20\x20\x20\x20Po','ngarian\x0a\x20\x20','Sedang\x20ber','quoted','ZjgSB','\x20Esperanto','p,\x20minimal','https://ap','oGPgx','96fab2ccb6','JASA\x20RUN\x20B','r\x20\x20\x20\x20\x20\x20\x20\x20T','ave\x20Partne','hasil\x20deng','kamu\x20menda','Menunggu\x20h','*-----\x20「GI','s\x20Chat','ang\x20didapa','🆔\x20ID\x20:\x20','contextInf','*\x20ore','ptt','upi\x20untuk\x20','jadibot,\x20j','eRUJe','OyqpI','JOCid','an\x20*','\x20Russian\x0a\x20','ame\x20untuk\x20','er\x0a\x0aSumber','\x202K21','\x20:*\x20','6b0c0.jpg','?&imageout','Cari\x20Partn','\x20\x20\x20en-us\x20\x20','re\x20dan\x20','UBFoU','\x0a\x0aSilahkan','ketik\x20#kod','27576756AhoQet','NCpaa','\x20Chat\x0a\x0a','hat_','ory\x20anda\x20d','\x0a🌐\x20*WOOD*\x20','a\x20owner\x20ka','imingstall','\x20batu','\x20vi\x20\x20\x20\x20\x20\x20\x20','ilahkan\x20tu','OKfOr','\x20kayu','iwan)\x0a\x20\x20\x20\x20','put=true&s','*Uang\x20dida','tahun','state','ndi\x0a\x20\x20\x20\x20hu','\x20\x20\x20\x20\x20\x20\x20\x20Hu','1523e.jpg','ntent','adi\x20owner\x20','buttonText','*Jumlah\x20Ka','Tamil\x0a\x20\x20\x20\x20','ia\x20dari\x20*','title','oods','an\x20dijual:','casino','\x20Berada\x20Di','FxNct','er\x0a','email','wait','n\x20berapa','manian\x0a\x20\x20\x20','extType=1&','Ex.\x20#tts\x20e','replace','rassland','tkan\x20_','imit','\x20ketik\x20','slice','blog','RMANEN','0&fontsize','3282717kbRZLR','\x20it\x20\x20\x20\x20\x20\x20\x20','tas','Kamu\x20kalah','6f57a4f6d9','skip','dc9f97d658','an\x20nomer\x20p','🔮\x20Public\x20R','tch\x0a\x20\x20\x20\x20en','\x20ingot\x20kam','💌\x20Bio\x20:\x20','Gagal\x20om:('];_0x278e=function(){return _0x252e31;};return _0x278e();}
	
if (isGroup && budy != undefined) {
	} else {
	console.log('\x1b[1;31m~\x1b[1;37m>', '[\x1b[1;32m CHAT \x1b[1;37m]', timuu, color(q), 'from', color(pushname))
	}		
	} catch (e) {
    e = String(e)
    if (!e.includes("this.isZero")) {
	console.log('Message : %s', color(e, 'green'))
        }
	// console.log(e)
	}
}