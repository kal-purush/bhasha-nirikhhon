import { ipcMain } from 'electron'
import fs from 'fs/promises'
import JSZip from 'jszip'
import { DOMParser } from 'xmldom'

type FileData = {
  title: string
  slides: string[][]
}

export const readContentFromFile = () => {
  ipcMain.handle(
    'read-content-from-file',
    async (_event, filePath: string): Promise<FileData | undefined> => {
      try {
        const fileData = await fs.readFile(filePath)

        const uint8Array = new Uint8Array(fileData)

        const zip = await JSZip.loadAsync(uint8Array)

        const slideNames = Object.keys(zip.files).filter(
          (name) => name.startsWith('ppt/slides/slide') && name.endsWith('.xml')
        )

        const slidesContent = await Promise.all(
          slideNames
            .map((slideName) => zip.files[slideName]?.async('string'))
            .filter((content): content is Promise<string> => !!content)
        )

        const slides = slidesContent.map(extractVersesFromXML)

        return {
          title: 'This is the title',
          slides
        }
      } catch (err) {
        console.error('Eroare la citirea fișierului:', err)
        return undefined
      }
    }
  )
}

const extractVersesFromXML = (slideContent: string): string[] => {
  const parser = new DOMParser()
  const xmlDoc = parser.parseFromString(slideContent, 'application/xml')

  const verses = xmlDoc.getElementsByTagNameNS(
    'http://schemas.openxmlformats.org/drawingml/2006/main',
    't'
  )

  return Array.from(verses).map((verse) => verse.textContent || '')
}