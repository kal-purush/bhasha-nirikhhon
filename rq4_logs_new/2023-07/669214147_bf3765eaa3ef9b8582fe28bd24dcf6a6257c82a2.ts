import {
  AddedNewsletter as AddedNewsletterEvent,
  ChangedNewsletterPrice as ChangedNewsletterPriceEvent
} from "../../generated/Sanscript/Sanscript"
import {
  Newsletter,
  Owner
} from "../../generated/schema"
import { ONE_BI } from "./../utils/constants"
import { log } from '@graphprotocol/graph-ts'

export function handleAddedNewsletter(event: AddedNewsletterEvent): void {

  let owner = Owner.load(event.params.newsletterOwner.toString())

  if (owner == null) {
    owner = new Owner(event.params.newsletterOwner.toHexString())
    owner.newslettersAmount = ONE_BI
  } else {
    owner.newslettersAmount = owner.newslettersAmount.plus(ONE_BI)
  }

  owner.save()

  let newsletter = new Newsletter(
    event.params.newsletterOwner.toHexString().concat("_").concat(event.params.newsletterNonce.toString())
  )
  newsletter.newsletterOwner = event.params.newsletterOwner.toHexString()
  newsletter.newsletterNonce = event.params.newsletterNonce
  newsletter.image = event.params.image
  newsletter.title = event.params.title
  newsletter.description = event.params.description
  newsletter.pricePerMonth = event.params.pricePerMonth

  newsletter.blockNumber = event.block.number
  newsletter.blockTimestamp = event.block.timestamp
  newsletter.transactionHash = event.transaction.hash

  newsletter.save()
}

export function handleChangedNewsletterPrice(
  event: ChangedNewsletterPriceEvent
): void {

  let newsletter = Newsletter.load(
    event.params.newsletterOwner.toHexString().concat("_").concat(event.params.newsletterNonce.toString())
  )

  if (newsletter == null) {
    log.error("NewsletterOwner {} Newsletter {}nonce not found. tx_hash: {}", [
      event.params.newsletterOwner.toHexString(),
      event.params.newsletterNonce.toString(),
      event.transaction.hash.toHexString()
    ]);
    return
  } else {
    newsletter.pricePerMonth = event.params.newPrice
  }

  newsletter.save()
}
