import { gql, useMutation, useQuery } from '@apollo/client'
import { useApp } from 'lodestar-app-element/src/contexts/AppContext'
import hasura from '../hasura'
import { CardProducts, MemberShipCards } from '../types/programPlan'

export const useMembershipCardByAppId = () => {
  const { id: appId } = useApp()
  const { data: cardData } = useQuery<hasura.GetCardTitleByAppId, hasura.GetCardTitleByAppIdVariables>(
    gql`
      query GetCardTitleByAppId($appId: String) {
        card(where: { app_id: { _eq: $appId } }) {
          title
          id
        }
      }
    `,
    {
      variables: { appId },
    },
  )

  const membershipCards: MemberShipCards = (cardData && cardData.card.map(card => card)) || []

  return {
    membershipCards,
  }
}

export const useMembershipCardByTargetId = (productType: string, targetId: string) => {
  const { data: cardData, refetch: refetchMembershipCard } = useQuery<
    hasura.GetCardByTargetId,
    hasura.GetCardByTargetIdVariables
  >(
    gql`
      query GetCardByTargetId($productType: String!, $targetId: uuid!) {
        card_product(where: { product_type: { _eq: $productType }, target: { _eq: $targetId } }) {
          id
          target
          product_type
          card_id
          card {
            id
            title
          }
        }
      }
    `,
    {
      variables: { productType, targetId },
    },
  )
  const cardTitle: string =
    (cardData && cardData.card_product.map(cardProduct => cardProduct.card.title).toString()) || ''

  const cardProducts: CardProducts = cardData
    ? cardData.card_product.map(card => ({
        cardProductId: card.id,
        cardId: card.card_id,
        cardTitle: card.card.title,
        targetId: card.target,
        productType: card.product_type,
      }))
    : []

  return {
    cardTitle,
    cardProducts,
    refetchMembershipCard,
  }
}

export const useProgramPlanEnrollmentCount = (programPlanId: string) => {
  const { loading, error, data, refetch } = useQuery<
    hasura.GET_PROGRAM_SUBSCRIPTION_PLAN_COUNT,
    hasura.GET_PROGRAM_SUBSCRIPTION_PLAN_COUNTVariables
  >(
    gql`
      query GET_PROGRAM_SUBSCRIPTION_PLAN_COUNT($programPlanId: uuid!) {
        program_plan_enrollment_aggregate(where: { program_plan_id: { _eq: $programPlanId } }) {
          aggregate {
            count
          }
        }
      }
    `,
    { variables: { programPlanId } },
  )

  const enrollmentCount: number = data?.program_plan_enrollment_aggregate.aggregate?.count || 0

  return {
    loadingEnrollmentCount: loading,
    errorEnrollmentCount: error,
    enrollmentCount,
    refetchEnrollmentCount: refetch,
  }
}

export const useUpsertProgramPlan = () => {
  const [upsertProgramPlan] = useMutation<hasura.UpsertMembershipPlan, hasura.UpsertMembershipPlanVariables>(
    gql`
      mutation UpsertMembershipPlan(
        $programId: uuid!
        $id: uuid!
        $type: Int!
        $title: String!
        $autoRenewed: Boolean!
        $listPrice: numeric!
        $description: String
        $salePrice: numeric
        $soldAt: timestamptz
        $discountDownPrice: numeric
        $periodAmount: numeric
        $periodType: String
        $remindPeriodAmount: Int
        $remindPeriodType: String
        $currencyId: String
        $publishedAt: timestamptz
        $groupBuyingPeople: numeric
        $isCountdownTimerVisible: Boolean
        $isParticipantsVisible: Boolean
      ) {
        insert_program_plan(
          objects: {
            id: $id
            type: $type
            title: $title
            description: $description
            list_price: $listPrice
            sale_price: $salePrice
            period_amount: $periodAmount
            period_type: $periodType
            remind_period_amount: $remindPeriodAmount
            remind_period_type: $remindPeriodType
            discount_down_price: $discountDownPrice
            sold_at: $soldAt
            program_id: $programId
            currency_id: $currencyId
            auto_renewed: $autoRenewed
            published_at: $publishedAt
            group_buying_people: $groupBuyingPeople
            is_countdown_timer_visible: $isCountdownTimerVisible
            is_participants_visible: $isParticipantsVisible
          }
          on_conflict: {
            constraint: program_plan_pkey
            update_columns: [
              type
              title
              description
              list_price
              sale_price
              discount_down_price
              period_amount
              period_type
              remind_period_amount
              remind_period_type
              sold_at
              currency_id
              auto_renewed
              published_at
              is_countdown_timer_visible
              is_participants_visible
            ]
          }
        ) {
          affected_rows
          returning {
            id
          }
        }
      }
    `,
  )
  return { upsertProgramPlan }
}

export const useUpsertCardProduct = () => {
  const [upsertCardProduct] = useMutation<hasura.UpsertCardProduct, hasura.UpsertCardProductVariables>(gql`
    mutation UpsertCardProduct($id: uuid!, $productType: String!, $targetId: uuid!, $cardId: uuid!) {
      insert_card_product(
        objects: { id: $id, product_type: $productType, target: $targetId, card_id: $cardId }
        on_conflict: { constraint: card_product_pkey, update_columns: [card_id] }
      ) {
        affected_rows
        returning {
          id
          target
          card_id
        }
      }
    }
  `)
  return { upsertCardProduct }
}