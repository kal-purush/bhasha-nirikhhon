import {
  CommandParserType,
  DiscordEventConfig,
  GlobalInterceptorType,
  InjectToken,
  IProvider
} from '@tlp01/djs-ioc-container';
import { GatewayIntentBits, Partials, Message } from 'discord.js';
import { ConfigModule } from './config-service/config.module';
import { ConfigService } from './config-service/config.service';

export const providers: IProvider[] = [
  {
    provide: InjectToken.ClientOptions,
    useValue: {
      intents: [
        GatewayIntentBits.GuildBans,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.GuildPresences,
        GatewayIntentBits.GuildEmojisAndStickers,
        GatewayIntentBits.GuildVoiceStates,
        GatewayIntentBits.GuildWebhooks,
        GatewayIntentBits.Guilds,
        GatewayIntentBits.DirectMessages,
        GatewayIntentBits.DirectMessageReactions,
        GatewayIntentBits.GuildMessageReactions,
        GatewayIntentBits.MessageContent
      ],
      partials: [Partials.Message, Partials.Channel, Partials.Reaction]
    }
  },
  {
    provide: InjectToken.CommandParser,
    imports: [ConfigModule],
    injects: [ConfigService],
    useFactory: (config: ConfigService): CommandParserType => {
      return {
        messageCreate: ([message]: [Message]) => {
          return message.content.split(' ')[0].replace(config.prefix, '');
        }
      };
    }
  },
  {
    provide: InjectToken.GlobalInterceptors,
    imports: [ConfigModule],
    injects: [ConfigService],
    useFactory: (config: ConfigService): GlobalInterceptorType => ({
      messageCreate: [
        ([message]: [Message], customConfig: DiscordEventConfig['messageCreate']) =>
          customConfig.startsWithPrefix && message.content.startsWith(config.prefix),
        ([message]: [Message], customConfig: DiscordEventConfig['messageCreate']) =>
          customConfig.ignoreBots && !message.author.bot
      ]
    })
  },
  {
    provide: InjectToken.AppConfig,
    useClass: ConfigService
  }
];
