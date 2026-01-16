import useCustomAccount from "@/hooks/use-account";
import { BEARCHAIN_API } from "@/hooks/use-bgt";
import { post } from "@/utils/http";
import { useEffect, useMemo, useState } from "react";
import useDelegationQueue from "@/sections/bgt/components/delegate/hooks/use-delegation-queue";
import _ from "lodash";

const pageSize = 10
export default function useList(currentTab: string) {

  const { loading: loadingDelegationQueue, delegationQueue, getDelegationQueue } = useDelegationQueue();
  const { account } = useCustomAccount()
  const [loading, setLoading] = useState(false)
  const [filterList, setFilterList] = useState([])
  const [sortDataIndex, setSortDataIndex] = useState("")

  const [maxPage, setMaxPage] = useState(1);
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState("")
  const [sortBy, setSortBy] = useState("boostApr")
  const [sortOrder, setSortOrder] = useState("desc")

  const variables = useMemo(() => {
    return {
      sortBy,
      sortOrder,
      search,
      pageSize,
      "chain": "BERACHAIN",
      "where": {},
      "skip": (page - 1) * pageSize,
    }
  }, [sortBy, sortOrder, search, page])


  const getValidators = async () => {
    try {
      setLoading(true)
      const response = await post(BEARCHAIN_API, {
        variables,
        "operationName": "GetValidators",
        "query": "query GetValidators($where: GqlValidatorFilter, $sortBy: GqlValidatorOrderBy = lastDayDistributedBGTAmount, $sortOrder: GqlValidatorOrderDirection = desc, $pageSize: Int, $skip: Int, $search: String, $chain: GqlChain) {\n  validators: polGetValidators(\n    where: $where\n    orderBy: $sortBy\n    orderDirection: $sortOrder\n    first: $pageSize\n    skip: $skip\n    search: $search\n    chain: $chain\n  ) {\n    pagination {\n      currentPage\n      totalCount\n      totalPages\n      pageSize\n      __typename\n    }\n    validators {\n      ...ApiValidator\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment ApiValidator on GqlValidator {\n  ...ApiValidatorMinimal\n  operator\n  rewardAllocationWeights {\n    ...ApiRewardAllocationWeight\n    __typename\n  }\n  lastBlockUptime {\n    isActive\n    __typename\n  }\n  metadata {\n    name\n    logoURI\n    website\n    description\n    __typename\n  }\n  __typename\n}\n\nfragment ApiValidatorMinimal on GqlValidator {\n  id\n  pubkey\n  operator\n  metadata {\n    name\n    logoURI\n    __typename\n  }\n  dynamicData {\n    activeBoostAmount\n    usersActiveBoostCount\n    queuedBoostAmount\n    usersQueuedBoostCount\n    allTimeDistributedBGTAmount\n    rewardRate\n    stakedBeraAmount\n    lastDayDistributedBGTAmount\n    activeBoostAmountRank\n    boostApr\n    commissionOnIncentives\n    __typename\n  }\n  __typename\n}\n\nfragment ApiRewardAllocationWeight on GqlValidatorRewardAllocationWeight {\n  percentageNumerator\n  validatorId\n  receivingVault {\n    ...ApiVault\n    __typename\n  }\n  receiver\n  startBlock\n  __typename\n}\n\nfragment ApiVault on GqlRewardVault {\n  id: vaultAddress\n  vaultAddress\n  address: vaultAddress\n  isVaultWhitelisted\n  dynamicData {\n    allTimeReceivedBGTAmount\n    apr\n    bgtCapturePercentage\n    activeIncentivesValueUsd\n    activeIncentivesRateUsd\n    __typename\n  }\n  stakingToken {\n    address\n    name\n    symbol\n    decimals\n    __typename\n  }\n  metadata {\n    name\n    logoURI\n    url\n    protocolName\n    description\n    __typename\n  }\n  activeIncentives {\n    ...ApiVaultIncentive\n    __typename\n  }\n  __typename\n}\n\nfragment ApiVaultIncentive on GqlRewardVaultIncentive {\n  active\n  remainingAmount\n  remainingAmountUsd\n  incentiveRate\n  tokenAddress\n  token {\n    address\n    name\n    symbol\n    decimals\n    __typename\n  }\n  __typename\n}"
      })
      setLoading(false)
      const { pagination, validators } = response?.data?.validators
      setFilterList(validators)
      setMaxPage(pagination?.totalPages)
    } catch (error) {
      setLoading(false)
      console.error(error)
    }
  }

  async function getUserValidators() {
    try {
      setLoading(true)
      const firstResponse = await post("https://api.goldsky.com/api/public/project_clq1h5ct0g4a201x18tfte5iv/subgraphs/pol-subgraph/mainnet-v1.1.0/gn", {
        "operationName": "GetUserValidatorInformation",
        "variables": { "address": account },
        "query": "query GetUserValidatorInformation($address: Bytes!, $block: Block_height) {\n  userValidatorInformations: userBoosts(\n    block: $block\n    where: {user: $address}\n    first: 1000\n  ) {\n    id\n    queuedBoostAmount\n    activeBoostAmount\n    queuedDropBoostAmount\n    queuedDropBoostStartBlock\n    queuedBoostStartBlock\n    user\n    validator {\n      ...ValidatorMinimal\n      __typename\n    }\n    __typename\n  }\n  _meta {\n    ...SubgraphStatusMeta\n    __typename\n  }\n}\n\nfragment ValidatorMinimal on Validator {\n  id\n  publicKey\n  activeBoostAmount: activeBoostAmount\n  __typename\n}\n\nfragment SubgraphStatusMeta on _Meta_ {\n  block {\n    timestamp\n    __typename\n  }\n  hasIndexingErrors\n  __typename\n}"
      })
      const idIn = firstResponse?.data?.userValidatorInformations?.map((userValidatorInfor) => userValidatorInfor?.validator?.id)
      const secondResponse = await post(BEARCHAIN_API, {
        "operationName": "GetValidators",
        "variables": { "sortBy": "lastDayDistributedBGTAmount", "sortOrder": "desc", "chain": "BERACHAIN", "where": { idIn } },
        "query": "query GetValidators($where: GqlValidatorFilter, $sortBy: GqlValidatorOrderBy = lastDayDistributedBGTAmount, $sortOrder: GqlValidatorOrderDirection = desc, $pageSize: Int, $skip: Int, $search: String, $chain: GqlChain) {\n  validators: polGetValidators(\n    where: $where\n    orderBy: $sortBy\n    orderDirection: $sortOrder\n    first: $pageSize\n    skip: $skip\n    search: $search\n    chain: $chain\n  ) {\n    pagination {\n      currentPage\n      totalCount\n      totalPages\n      pageSize\n      __typename\n    }\n    validators {\n      ...ApiValidator\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment ApiValidator on GqlValidator {\n  ...ApiValidatorMinimal\n  operator\n  rewardAllocationWeights {\n    ...ApiRewardAllocationWeight\n    __typename\n  }\n  lastBlockUptime {\n    isActive\n    __typename\n  }\n  metadata {\n    name\n    logoURI\n    website\n    description\n    __typename\n  }\n  __typename\n}\n\nfragment ApiValidatorMinimal on GqlValidator {\n  id\n  pubkey\n  operator\n  metadata {\n    name\n    logoURI\n    __typename\n  }\n  dynamicData {\n    activeBoostAmount\n    usersActiveBoostCount\n    queuedBoostAmount\n    usersQueuedBoostCount\n    allTimeDistributedBGTAmount\n    rewardRate\n    stakedBeraAmount\n    lastDayDistributedBGTAmount\n    activeBoostAmountRank\n    boostApr\n    commissionOnIncentives\n    __typename\n  }\n  __typename\n}\n\nfragment ApiRewardAllocationWeight on GqlValidatorRewardAllocationWeight {\n  percentageNumerator\n  validatorId\n  receivingVault {\n    ...ApiVault\n    __typename\n  }\n  receiver\n  startBlock\n  __typename\n}\n\nfragment ApiVault on GqlRewardVault {\n  id: vaultAddress\n  vaultAddress\n  address: vaultAddress\n  isVaultWhitelisted\n  dynamicData {\n    allTimeReceivedBGTAmount\n    apr\n    bgtCapturePercentage\n    activeIncentivesValueUsd\n    activeIncentivesRateUsd\n    __typename\n  }\n  stakingToken {\n    address\n    name\n    symbol\n    decimals\n    __typename\n  }\n  metadata {\n    name\n    logoURI\n    url\n    protocolName\n    description\n    __typename\n  }\n  activeIncentives {\n    ...ApiVaultIncentive\n    __typename\n  }\n  __typename\n}\n\nfragment ApiVaultIncentive on GqlRewardVaultIncentive {\n  active\n  remainingAmount\n  remainingAmountUsd\n  incentiveRate\n  tokenAddress\n  token {\n    address\n    name\n    symbol\n    decimals\n    __typename\n  }\n  __typename\n}"
      })
      const validators = firstResponse?.data?.userValidatorInformations?.map(userValidatorInfor => {
        const validator = secondResponse?.data?.validators?.validators?.find((validator) => validator?.id === userValidatorInfor?.validator?.id) ?? null
        return {
          ...userValidatorInfor,
          ...validator,
        }
      })
      setMaxPage(1)
      setLoading(false)
      setFilterList(validators)

      return validators
    } catch (error) {
      console.error(error)
    }
  }
  function handlePageChange(_page) {
    setPage(_page)
  }
  const handleSearch = _.debounce((_search) => {
    setSearch(_search)
  }, 500)

  function handleSort(_sortBy, _sortOrder) {
    setSortBy(_sortBy)
    setSortOrder(_sortOrder)
  }
  async function handleQueryList() {
    if (currentTab === "all") {
      getValidators()
    } else if (currentTab === "my") {
      getUserValidators()
    } else {
      const validators = await getUserValidators()
      validators?.length > 0 && getDelegationQueue(validators)
    }
  }
  useEffect(() => {
    handleQueryList()
  }, [currentTab, variables])
  return {
    page,
    sortBy,
    loading,
    maxPage,
    sortOrder,
    filterList,
    delegationQueue,
    loadingDelegationQueue,
    sortDataIndex,
    handleSort,
    handleSearch,
    handlePageChange,
  }
}