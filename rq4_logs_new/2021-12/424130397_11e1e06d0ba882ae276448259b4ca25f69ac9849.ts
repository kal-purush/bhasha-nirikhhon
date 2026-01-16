import { strict as assert } from 'assert';
import { Selector } from 'testcafe';
import { waitForReact } from 'testcafe-react-selectors';
import { SettlementsPage, SettlementFinalizingModal, SettlementFinalizationWarningModal } from '../page-objects/pages/SettlementsPage';
import { config } from '../config';
import { SideMenu } from '../page-objects/components/SideMenu';
import { VoodooClient, protocol } from 'mojaloop-voodoo-client';
import { v4 as uuidv4 } from 'uuid';
import { ledger as ledgerApi, settlement as settlementApi, reporting as reportingApi, types } from 'mojaloop-ts';
import ExcelJS from 'exceljs';

// This has really come from:
// import { extractSwitchIdentifiers } from '../../../../src/App/Settlements/helpers';
// and been copy-pasted and modified. Preferably it would be shared somehow.
function extractParticipantName(text: string): types.FspName {
  const re = /^([0-9]+) ([0-9]+) ([a-zA-Z][a-zA-Z0-9]{1,29})$/g;
  assert(
    re.test(text),
    `Unable to extract participant ID, account ID and participant name from "${text}". Matching regex: ${re}`,
  );
  const [,, name] = text.split(' ');
  return name;
}

const { voodooEndpoint, reportBasePath, settlementsBasePath, ledgerBasePath } = config;
const CURRENCY: types.Currency = 'MMK';

fixture `Settlements Feature`
  .page`${config.settlementMicrofrontendEndpoint}`
  .before(async (ctx) => {
    const cli = new VoodooClient(voodooEndpoint, { defaultTimeout: config.voodooTimeoutMs });
    await cli.connected();

    const hubAccounts: protocol.HubAccount[] = [
      {
        type: "HUB_MULTILATERAL_SETTLEMENT",
        currency: CURRENCY,
      },
      {
        type: "HUB_RECONCILIATION",
        currency: CURRENCY,
      },
    ];
    await cli.createHubAccounts(hubAccounts);
    ctx.cli = cli;
  })
  .beforeEach(async (t) => {
    const accounts: protocol.AccountInitialization[] = [
      { currency: CURRENCY, initial_position: '0', ndc: 10000 },
      { currency: CURRENCY, initial_position: '0', ndc: 10000 },
    ];
    const participants = await t.fixtureCtx.cli.createParticipants(accounts);

    t.fixtureCtx.participants = participants;

    await waitForReact();
    await t
      .click(SideMenu.settlementWindowsButton); // yes, not the settlements button
  });

test.meta({
  ID: '',
  STORY: 'MMD-440',
  description:
    `Close two settlement windows. Add them to a settlement. Settle the settlement.`,
})('Settle settlement containing two closed windows', async (t) => {
  type Context = { cli: VoodooClient; participants: protocol.ClientParticipant[] };
  const { cli, participants }  = t.fixtureCtx as Context;
  // Run a transfer to ensure the settlement window can be closed
  const transfers1: protocol.TransferMessage[] = [{
    msg_sender: participants[1].name,
    msg_recipient: participants[0].name,
    currency: CURRENCY,
    amount: '10',
    transfer_id: uuidv4(),
  }];
  await cli.completeTransfers(transfers1);
  const nullWindowQueryParams = { currency: null, participantId: null, state: null, fromDateTime: null, toDateTime: null };
  const openWindows1 = await cli.getSettlementWindows({ ...nullWindowQueryParams, state: "OPEN" });
  await t.expect(openWindows1.length).eql(1, 'Expected only a single open window');
  await settlementApi.closeSettlementWindow(
    settlementsBasePath,
    openWindows1[0].settlementWindowId,
    'Integration test',
  );

  // Run a transfer so the settlement window can be closed
  const transfers2: protocol.TransferMessage[] = [{
    msg_sender: participants[1].name,
    msg_recipient: participants[0].name,
    currency: CURRENCY,
    amount: '10',
    transfer_id: uuidv4(),
  }];
  await cli.completeTransfers(transfers2);
  const openWindows2 = await cli.getSettlementWindows({ ...nullWindowQueryParams, state: "OPEN" });
  await t.expect(openWindows2.length).eql(1, 'Expected only a single open window');
  await settlementApi.closeSettlementWindow(
    settlementsBasePath,
    openWindows2[0].settlementWindowId,
    'Integration test',
  );

  const settlementWindowIds = [
    openWindows1[0].settlementWindowId,
    openWindows2[0].settlementWindowId,
  ];

  const settlement = await settlementApi.createSettlement(
    settlementsBasePath,
    {
      reason: 'Integration test',
      settlementModel: 'DEFERREDNET',
      settlementWindows: settlementWindowIds.map((id) => ({ id })),
    },
  );

  // Get the initiation report, "simulate" some balances returned by the settlement bank, save it
  // as the finalization report.
  const participantBalances = new Map(
    participants.map((p) => [p.name, Math.trunc(Math.random() * 5000)]),
  );
  const initiationReport =
    await reportingApi.getSettlementInitiationReport(reportBasePath, settlement.id);
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(initiationReport.body);
  const ws = wb.getWorksheet(1);
  const BALANCE_COL = 'C';
  const PARTICIPANT_INFO_COL = 'A';
  const START_OF_DATA = 7;
  let firstEmptyDataRow = START_OF_DATA;
  while (ws.getCell(`A${firstEmptyDataRow}`).text !== '') {
    firstEmptyDataRow += 1;
  }
  const balanceInfo = ws.getRows(START_OF_DATA, firstEmptyDataRow - START_OF_DATA)?.map((row) => {
    const participantInfo = row.getCell(PARTICIPANT_INFO_COL);
    const name = extractParticipantName(participantInfo.text);
    // TODO: sometimes there might be extra participants in the settlement because of other tests.
    // We'll generate data for them here also.
    const balance = participantBalances.get(name) || Math.trunc(Math.random() * 5000);
    return {
      balance,
      participantInfo,
      rowNum: row.number,
      row,
    };
  });
  await t.expect(balanceInfo).notEql(
    undefined,
    'Expect some data rows in the settlement initiation report'
  );
  balanceInfo?.forEach(({ balance, row }) => {
    row.getCell(BALANCE_COL).value = balance;
  });
  const filename = __dirname + `/settlement-finalization-report-settlement-${settlement.id}.xlsx`;
  await wb.xlsx.writeFile(filename);

  await t.click(SideMenu.settlementsButton);

  const rowsBefore = await SettlementsPage.getResultRows();
  const settlementRowBefore = await Promise.any(rowsBefore.map(
    (r) => r.id.innerText.then(
      id => Number(id) === settlement.id ? Promise.resolve(r) : Promise.reject(),
    ),
  ));
  await t.expect(settlementRowBefore.state.innerText).eql('Pending Settlement');
  await t.click(settlementRowBefore.finalizeButton);

  await t.setFilesToUpload(SettlementFinalizingModal.fileInput, [filename]);
  if (!await SettlementFinalizingModal.setLiquidityAccountBalanceCheckbox.checked) {
    await t.click(SettlementFinalizingModal.setLiquidityAccountBalanceCheckbox);
  }
  if (!await SettlementFinalizingModal.increaseNdcCheckbox.checked) {
    await t.click(SettlementFinalizingModal.increaseNdcCheckbox);
  }
  if (!await SettlementFinalizingModal.decreaseNdcCheckbox.checked) {
    await t.click(SettlementFinalizingModal.decreaseNdcCheckbox);
  }
  await t.click(SettlementFinalizingModal.validateButton);
  // The warning dialog will appear, dismiss it. Validation can take some time, use a high timeout.
  await t.click(Selector(SettlementFinalizationWarningModal.closeButton, { timeout: 60000 }));
  await t.click(SettlementFinalizingModal.processButton);

  // Processing can take some time, use a high timeout
  await t.click(Selector(SettlementFinalizingModal.closeButton, { timeout: 60000 }));
  const rowsAfter = await SettlementsPage.getResultRows();
  const settlementRowAfter = await Promise.any(rowsAfter.map(
    (r) => r.id.innerText.then(id => Number(id) === settlement.id ? Promise.resolve(r) : Promise.reject()),
  ));

  await t.expect(settlementRowAfter.state.innerText).eql('Settled');

  async function getParticipantSettlementAccount(
    p: protocol.ClientParticipant
  ): Promise<[types.FspName, types.AccountWithPosition | undefined]> {
    return ledgerApi.getParticipantAccounts(ledgerBasePath, p.name).then((accs) => [
      p.name,
      accs.find(
        (acc) => acc.ledgerAccountType === 'SETTLEMENT' && acc.currency === p.account.currency
      ),
    ]);
  }
  const [limits, accounts] = await Promise.all([
    ledgerApi.getParticipantsLimits(ledgerBasePath).then((lims) => lims
      .filter((lim) => lim.currency === CURRENCY && lim.limit.type === 'NET_DEBIT_CAP')
      .reduce(
        (map, lim) => map.set(lim.name, lim),
        new Map<types.FspName, types.ParticipantLimit>(),
      )
    ),
    Promise.all(participants.map(getParticipantSettlementAccount)).then(
      (accounts) => new Map<types.FspName, types.AccountWithPosition | undefined>(accounts)
    ),
  ]);
  const expectedAccountState = Object.fromEntries(
    [...participantBalances.entries()].map(
      ([name, bal]) => [name, { balance: -bal, limit: bal }]
    ),
  );
  const actualAccountState = Object.fromEntries(
    [...participantBalances.keys()].map(
      (name) => [name, { balance: accounts.get(name)?.value, limit: limits.get(name)?.limit.value }]
    ),
  );
  await t.expect(actualAccountState).eql(
    expectedAccountState,
    'All participant settlement account balances and NDCs should have been set correctly'
  );
});


test.skip.meta({
  ID: '',
  STORY: 'MMD-440',
})(
  `Once I click Settlement tab in Side Menu, the page on the right should come up with
    Date drop-down defaulted to Today, From and To drop-down defaulted to current date in MM/DD/YYYY HH:MM:SS format
    State should be empty and Clear Filters button`,
  async (t) => {
    //Call Mojaloop Settlement API to get the current window details
    // Check that the latest window ID that displays on the page is the same
  },
);