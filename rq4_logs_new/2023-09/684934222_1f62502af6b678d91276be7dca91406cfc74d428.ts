import type { Transaction, Account } from "./type";

export const MOCK_ACCOUNTS: { [namke: string]: Account } = {
  self: {
    address:
      "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsq0dnd72kh63g08574r38qkyph2ewl9ummcfp6znk",
    balance: "48642877372524671",
  },
  alice: {
    address:
      "ckb1qzjrnpmgmpaaz7h2zds7msave44qz9mhfhzwhjqnh7sh869vp5yx6qgqwhrzgpkxkxqd3llfvsqd0uywd6yarpkuqqu2gqs3",
    balance: "48642877372524671",
  },
  bob: {
    address:
      "ckb1qqwqfhcfmxk7medlcs8lrgu68gtler3f79w9dutt0eyxsrrqpmj6cqwqge9mksrygxm9r6zd9vstk0fvngxqtav4kpfmssy6vplx24rhtu7frlmk",
    balance: "48642877372524671",
  },
  charlie: {
    address:
      "ckb1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqdy46j0t82lw0pwlc4y3m8w5mp2k5c633c34jr96",
    balance: "48642877372524671",
  },
};

export const MOCK_TRANSACTION: Transaction[] = [
  {
    type: "from",
    txHash:
      "0xf83c104f4d478f72ec443d36efa11a4d2bee31735f8a86b07e713eea195fd328",
    txStatus: "pending",
    displayInputs: [
      {
        addressHash: MOCK_ACCOUNTS['alice'].address,
        cellType: "udt",
        capacity: "31800000000.0",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
    ],
    displayOutputs: [
      {
        addressHash: MOCK_ACCOUNTS['self'].address,
        cellType: "udt",
        capacity: "31800000000",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
    ],
  },
  {
    type: "to",
    txHash:
      "0x4ca389777c2cd4aea19b9760068be379824b956e34acd33fff5154a67c959eff",
    txStatus: "committed",
    blockTimestamp: "1695179332694",
    displayInputs: [
      {
        addressHash: MOCK_ACCOUNTS['self'].address,
        cellType: "udt",
        capacity: "31800000000",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
    ],
    displayOutputs: [
      {
        addressHash: MOCK_ACCOUNTS['alice'].address,
        cellType: "udt",
        capacity: "31800000000.0",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
    ],
  },
  {
    type: "mint",
    txHash:
      "0x1ebe22fe7d9740e56998b6b874aac985af4ca4040d82b251951fb536f405244d",
    txStatus: "committed",
    blockTimestamp: "1695141801708",
    displayInputs: [
      {
        addressHash: MOCK_ACCOUNTS['self'].address,
        cellType: "normal",
        capacity: "60000000000",
      },
    ],
    displayOutputs: [
      {
        addressHash: MOCK_ACCOUNTS['alice'].address,
        cellType: "udt",
        capacity: "30000000000.0",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
      {
        addressHash: MOCK_ACCOUNTS['self'].address,
        cellType: "udt",
        capacity: "30000000000.0",
        extraInfo: {
          symbol: "SUDT1",
          amount: "1000",
          decimal: "0",
        },
      },
    ],
  },
];