import axios from 'axios'
import { format } from 'date-fns'

import { NextApiRequest, NextApiResponse } from 'next'

async function getDataProgram(id) {
  const { data } = await axios.get(`https://api.mipo.msk.ru/programs/${id}`)
  return data
}

function escapeXml(unsafe) {
  return unsafe
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&apos;");
}

interface ErrorMessage {
  message?: string
  error? : string
}

export default async function handler(req: NextApiRequest, res: NextApiResponse<string | ErrorMessage>) {
  if (req.method !== 'GET') {
    return res.status(405).json({ message: 'Метод не разрешен' })
  }

  try {
    const { data } = await axios.get(
      'https://api.mipo.msk.ru/get-static-props/programs'
    )
    const programs = data.programs

    const fullProgramsData = await Promise.all(
      programs.map(async elem => {
        const {
          description,
          yandex_feed_categories: categoryFeed,
          YandexStepsFeed
        } = await getDataProgram(elem.id)

        return { ...elem, description, categoryFeed, YandexStepsFeed }
      })
    )

    const xml = `<?xml version="1.0" encoding="UTF-8"?>
  <yml_catalog date="${format(new Date(), 'yyyy-MM-dd HH:mm')}">
    <shop>
      <name>${escapeXml('Институт профессионального образования')}</name>
      <company>${escapeXml('НАНО "ИПО"')}</company>
      <url>${escapeXml('https://ipo.msk.ru/')}</url>
      <email>${escapeXml('info@ipo.msk.ru')}</email>
      <picture>${escapeXml('https://ipo.msk.ru/image/catalog/main-logo.png')}</picture>
      <description>${escapeXml('Российский институт дополнительного профессионального образования «ИПО» – это удобное и быстрое получение дополнительного профобразования и повышения квалификации в сети интернет по востребованным на рынке гуманитарным, техническим и бизнес направлениям.')}</description>
      <currencies>
        <currency id="RUR" rate="1"/>
      </currencies>
    </shop>
    <offers>
      ${fullProgramsData
        .map(elem => {
          return `<offer id="${escapeXml(elem.id)}">
            <name>${escapeXml(elem.title)}</name>
            <url>${escapeXml(`https://mipo.msk.ru/professions/${elem.study_field.slug}/${elem.slug}`)}</url>
            <description>${escapeXml(elem.description)}</description>
            <price>${escapeXml(elem.timenprice[0].ref.price)}</price>
            <currencyId>RUR</currencyId>
            <param unit="месяц" name="Продолжительность">${escapeXml(elem.timenprice[0].ref.studyMonthsDuration)}</param>
            ${elem.YandexStepsFeed.length ? elem.YandexStepsFeed.map((steps) => `<param unit="План">${escapeXml(steps.step)}</param>`).join('') : ''}
            ${elem.categoryFeed.length ? elem.categoryFeed.map((element) => `<categoryId>${element.idCategory}</categoryId>`).join('') : ''}
          </offer>`;
        }).join('')}
    </offers>
  </yml_catalog>`;

    res.setHeader('Content-Type', 'application/xml; charset=utf-8');
    return res.status(200).send(xml);

  } catch (error) {
    console.error(error)
    res.status(500).json({ error: 'Произошла ошибка при генерации XML' })
  }
}