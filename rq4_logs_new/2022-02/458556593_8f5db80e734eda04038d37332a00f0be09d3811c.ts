export type ListMenuLink = {
  text: string
  href?: string
  icon?: any
  isExpanded?: boolean
  subLinks?: Array<ListMenuLink>
  onClick?: () => void
}