/*
 * Copyright 2024 Marek Kobida
 * Last Updated: 27.09.2024
 */

import fs from 'node:fs';
import repositories from './repositories';
import type { Message } from './types';

const filter = new Date(1726437600000); // 16. 9. 2024

const messages: Message[] = [];

repositories.forEach(repository => {
  const $1 = fs.readFileSync(`${repository.path}/config`).toString();
  const $2 = fs.readFileSync(`${repository.path}/logs/HEAD`).toString();

  const messagePattern = /[0-9a-f]{40}\s(?<hash>[0-9a-f]{40})\s[^<]+<[^>]+>\s(?<date>\d+)\s[+-]\d{4}\s(?<message>.+)/g;
  const repositoryUrlPattern = /url\s=\s(?<url>https:\/\/github\.com.+)/;

  messages.push(
    ...Array.from(
      $2.matchAll(messagePattern),
      $ =>
        ({
          ...$.groups,
          date: new Date(+$.groups!.date! * 1000),
          repository: {
            ...repository,
            url: $1.match(repositoryUrlPattern)?.groups?.url,
          },
        }) as Message,
    ),
  );
});

const filteredMessages = messages
  .filter(({ date }) => date >= filter)
  .filter(({ message }) => !/^(Branch:|clone:|commit\s\(initial\):)/.test(message));

let html: string[] = [];

html.push(`<!doctype html>
<html lang="sk">
  <head>
    <meta charset="utf-8" />
    <meta content="viewport-fit=cover, width=device-width" name="viewport" />

    <style>
      a {
        color: inherit;
        text-decoration-line: none;
      }
      body {
        background-color: hsl(0, 0%, 12.5%);
        color: hsl(0, 0%, 87.5%);
        font-family: ui-monospace;
        line-height: 1.25;
        margin: 32px;
      }
      h1 {
        font-size: 32px;
        font-weight: 300;
        margin: 32px 0;
      }
      p {
        margin: 16px 0;
      }
      .messages {
        display: grid;
        gap: 16px;
        grid-template-columns: repeat(3, 1fr);
      }
      @media (max-width: 1024px) {
        .messages {
          grid-template-columns: 1fr;
        }
      }
      .message {
        align-items: center;
        background-color: hsl(0, 0%, 25%);
        border: 2px solid hsl(0, 0%, 37.5%);
        border-radius: 2px;
        column-gap: 16px;
        display: flex;
        padding: 16px;
      }
      .message__date,
      .message__left,
      .message__repository {
        opacity: 0.5;
      }
    </style>

    <title>Prehľad (${filteredMessages.length})</title>
  </head>
  <body>
    <h1>Prehľad (${filteredMessages.length})</h1>

    <p>${repositories.map(({ name }, i) => `${i + 1}. ${name}`).join(', ')}</p>
    <p>Od ${filter.toLocaleDateString('sk')}</p>

    <div class="messages">`);

filteredMessages
  .sort((a, b) => +b.date - +a.date)
  .forEach(({ date, hash, message, repository }, i) => {
    const repositoryUrl = repository.url?.replace(/\.git/, '');

    html.push(`      <a class="message" href="${repositoryUrl}/commit/${hash}" target="_blank">`);
    html.push(`        <div class="message__left">${(i + 1).toString().padStart(3, '0')}</div>`);
    html.push('        <div class="message__right">');
    html.push(`          <div class="message__repository">${repository.name}</div>`);
    html.push(`          <div class="message__date">${date.toLocaleString('sk')}</div>`);
    html.push(`          <div class="message__message">${message.replace(/^commit:\s/, '')}</div>`);
    html.push('        </div>');
    html.push('      </a>');
  });

html.push(`    </div>

    <p>${new Date().toLocaleString('sk')}</p>
    <a href="https://www.instagram.com/marekkobida" target="_blank">Copyright 2024 Marek Kobida</a>
  </body>
</html>
`);

fs.writeFileSync('/Users/marekkobida/Documents/warden/leopold/dog/index.html', html.join('\n'));