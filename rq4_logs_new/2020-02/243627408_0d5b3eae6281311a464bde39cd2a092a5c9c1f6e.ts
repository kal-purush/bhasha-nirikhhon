import { useEffect, useState } from "react"
import footerItems from "../data/footer"

type FooterJson = {
  data: Array<{
    attributes: {
      display: string
      items: Array<Item>
    }
  }>
}

type Item = {
  label: string
  link: string
}

type FooterArray = Array<
  Array<{
    header: {
      description: string
    }
    items: Array<{
      description: string
      link: string
    }>
  }>
>

const footerUrl = process.env.REACT_APP_FOOTER_JSON || ""

/**
 * formatFooterItems is a helper function to convert the links
 * under each header into the accepted footer data format.
 */
const formatFooterItems = (items: Array<Item>) =>
  items.map(c => ({
    description: c.label,
    link: c.link,
  }))

/**
 * formatFooterData converts the received footer JSON data and
 * converts it into the dicty-footer data format.
 */
const formatFooterData = (json: FooterJson) =>
  json.data.map(item => [
    {
      header: {
        description: item.attributes.display,
      },
      items: formatFooterItems(item.attributes.items),
    },
  ])

/**
 * useFooter is a hook for fetching dictyBase footer
 * JSON data. It uses an included array of data as its
 * initial state then replaces it with fetched data
 * on a successful request.
 */

const useFooter = () => {
  const [footerData, setFooterData] = useState<FooterArray>(footerItems)
  const [error, setError] = useState<null | string>(null)

  useEffect(() => {
    const fetchFooter = async () => {
      try {
        const res = await fetch(footerUrl)
        const json = await res.json()
        if (res.ok) {
          const footerArr = formatFooterData(json)
          setFooterData(footerArr)
        } else {
          setError(res.statusText)
        }
      } catch (error) {
        setError(error.toString())
      }
    }
    fetchFooter()
  }, [])

  return { footerData, error }
}

export { formatFooterData }
export default useFooter