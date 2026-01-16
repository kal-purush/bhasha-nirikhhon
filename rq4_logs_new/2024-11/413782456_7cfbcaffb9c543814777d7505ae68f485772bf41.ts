export default {
  asyncAutocomplete: {
    noResultsLabel: 'Kein Ergebnis',
    lessThen3CharacterLabel: 'Gib mindestens 3 Zeichen ein',
    lessThen11CharacterLabel: 'Gib mindestens 11 Zeichen ein',
    searchLabel: 'Körperschaft suchen',
    aooLabel: 'Gib den eindeutigen AOO-Code ein',
    uoLabel: 'Gib den eindeutigen UO-Code ein',
    ariaLabel: `Seleziona la tipologia di ricerca dell'ente`,
    businessName: 'Firmenbezeichnung',
    taxcode: 'Steuernummer der Körperschaft',
    originId: 'IVASS-Code',
  },
  partyAdvancedSelect: {
    advancedSearchLabel: 'Suchen nach',
    businessName: 'Firmenbezeichnung',
    ivassCode: 'IVASS-Code',
    taxCode: 'Steuernummer der Körperschaft',
    aooCode: 'Eindeutiger AOO-Code',
    uoCode: 'Eindeutiger UO-Code',
  },
  confirmOnboarding: {
    chooseOption: {
      download: {
        product: {
          title: 'Beitrittsvereinbarung downloaden',
          description: `Per completare l’adesione, scarica l’accordo e fai apporre la firma digitale in <1 /><2>formato p7m</2> dal Legale Rappresentante dell’ente.`,
          downloadContract: 'Vereinbarung downloaden',
        },
        user: {
          title: 'Hinzufügungsformular downloaden',
          description: `Per completare l’adesione, scarica il Modulo di aggiunta e fai apporre la <1 />firma digitale in <2>formato p7m</2> dal Legale Rappresentante dell’ente.`,
          downloadContract: 'Formular downloaden',
        },
        disclaimer:
          'Firmando l’accordo, il Legale Rappresentante dell’ente, accetta espressamente e specificamente anche le singole clausole indicate nel paragrafo “Clausole ai sensi degli artt. 1341 e 1342 c.c.”',
      },
      upload: {
        product: {
          title: 'Signierte Vereinbarung laden',
          description: `Una volta firmato l’accordo, segui le istruzioni per inviarlo e completare <1 /> l’adesione al prodotto scelto. Ricorda di caricare l’accordo <3>entro 30 giorni.</3>`,
        },
        user: {
          title: 'Signiertes Formular laden',
          description: `Una volta firmato il Modulo, segui le istruzioni per inviarlo e completare <1 /> l’aggiunta di uno o più Amministratori.`,
        },
        goToUpload: 'Zum Laden',
      },
    },
    upload: {
      product: {
        title: "Beitrittsvereinbarung laden",
        description: `Carica l’accordo di adesione, firmato digitalmente in <1 />p7m dal Legale Rappresentante.`,
        dropArea: {
          title: 'Zieh die signierte Beitrittsvereinbarung hierhin oder',
          link: 'lade die Datei',
        },
      },
      user: {
        title: 'Formular laden',
        description: `Carica il Modulo di aggiunta, firmato digitalmente in <1 />p7m dal Legale Rappresentante.`,
        dropArea: {
          title: 'Zieh das signierte Formular hierhin oder',
          link: 'lade die Datei',
        },
        continue: 'Weiter',
      },
      continue: 'Weiter',
      error: {
        title: 'Laden fehlgeschlagen',
        description:
          'Il caricamento del documento non è andato a buon fine. <1 />Carica un solo file in formato <3>p7m</3>.',
        close: 'Beenden',
        retry: 'Erneut laden',
      },
    },
  },
  fileUploadPreview: {
    loadingStatus: 'Laden läuft...',
    labelStatus: 'Versandbereit',
  },
  inlineSupportLink: {
    assistanceLink: "Kundendienst kontaktieren",
  },
  moreInformationOnRoles: 'Mehr Informationen über die Funktionen',
  onboardingStep0: {
    title: 'Willkommen auf dem Portal Self-Care',
    description: 'In wenigen Schritten kann deine Körperschaft beitreten und alle PagoPA-Produkte verwalten.',
    privacyPolicyDescription: 'Ich habe die',
    privacyPolicyLink: 'Datenschutzrichtlinie und die Nutzungsbedingungen des Dienstes gelesen und verstanden',
    actionLabel: 'Weiter',
  },
  stepVerifyOnboarding: {
    loadingText: 'Wir prüfen gerade deine Daten',
    ptAlreadyOnboarded: {
      title: 'Der Partner ist bereits registriert',
      description:
        'Per operare su un prodotto, chiedi a un Amministratore di <1/> aggiungerti nella sezione Utenti.',
      backAction: 'Beenden',
    },
    alreadyOnboarded: {
      title: 'Die gewählte Körperschaft ist bereits beigetreten',
      description:
        'Per operare sul prodotto, chiedi a un Amministratore di <1/>aggiungerti nella sezione Utenti.',
      addNewAdmin:
        'Gli attuali Amministratori non sono più disponibili e hai l’esigenza <1 />di gestire i prodotti? <3>Aggiungi un nuovo Amministratore</3>',
      backHome: 'Zurück zur Homepage',
    },
    genericError: {
      title: 'Etwas ist schiefgelaufen',
      description: `A causa di un errore del sistema non è possibile completare <br />la procedura. Ti chiediamo di riprovare più tardi.`,
      backHome: 'Zurück zur Homepage',
    },
    userNotAllowedError: {
      title: 'Du kannst diesem Produkt nicht beitreten',
      description: `Al momento, l'ente <1>{{partyName}}</1> non può aderire a <3>{{productTitle}}</3>. <5 /> Per maggiori dettagli contatta <7>l'assistenza</7>.`,
      noSelectedParty: 'angegeben',
      backToHome: 'Zurück zur Homepage',
    },
  },
  onboardingStep1: {
    loadingOverlayText: 'Wir prüfen gerade deine Daten',
    onboarding: {
      bodyTitle: 'Suche deine Körperschaft',
      codyTitleSelected: 'Bestätige die gewählte Körperschaft',
      disclaimer: {
        description: `Al momento possono aderire a SEND tramite Area Riservata solo le <1>Pubbliche <3 /> Amministrazioni Locali </1> presenti su IPA che trovi a <5>questo link</5>.`,
      },
      bodyDescription:
        'Inserisci uno dei dati richiesti e cerca dall’Indice della Pubblica <1/> Amministrazione (IPA) l’ente per cui vuoi richiedere l’adesione a <3/><4>{{productTitle}}</4>.',
      aggregator: 'Ich bin ein Aggregator',
      aggregatorModal: {
        title: 'Aggregator',
        message: `Stai richiedendo l’adesione come ente aggregatore per <1>{{partyName}}</1>.<3 />Per completare l’adesione, dovrai indicare gli enti da aggregare.`,
        back: 'Zurück',
        forward: 'Weiter',
      },
      ipaDescription: `Non trovi il tuo ente nell'IPA? <1>In questa pagina</1> trovi maggiori <3/> informazioni sull'indice e su come accreditarsi `,
      selectedInstitution:
        'Prosegui con l’adesione a <1>{{productName}}</1> per l’ente selezionato',
      gpsDescription: `Non trovi il tuo ente nell'IPA?<1 /><2>Inserisci manualmente i dati del tuo ente.</2>`,
      saSubTitle:
        'Se sei tra i gestori privati di piattaforma e-procurement e hai <1/> già ottenuto la <3>certificazione da AgID</3>, inserisci uno dei dati <5/> richiesti e cerca l’ente per cui vuoi richiedere l’adesione a <7/> <8>Interoperabilità.</8>',
      asSubTitle:
        'Se sei una società di assicurazione presente nell’Albo delle <1/>imprese IVASS, inserisci uno dei dati richiesti e cerca l’ente per<3/> cui vuoi richiedere l’adesione a <5>Interoperabilità.</5>',
      scpSubtitle:
        'Inserisci uno dei dati richiesti e cerca da InfoCamere l’ente <3/> per cui vuoi richiedere l’adesione a <5>Interoperabilità.</5>',
      asyncAutocomplete: {
        placeholder: 'Suchen',
      },
      onboardingStepActions: {
        confirmAction: 'Weiter',
        backAction: 'Zurück',
      },
    },
  },
  stepUploadAggregates: {
    title: `Indica i soggetti aggregati per {{productName}}`,
    subTitle:
      'Scarica il file di esempio, compilalo seguendo le indicazioni e carica il documento per aggiungere/dichiarare gli enti da aggregare.',
    findOutMore: 'Zweifel? Zur Anleitung',
    errors: {
      onCsv: {
        title: 'Die Datei enthält einen oder mehrere Fehler',
        description:
          '<1>Scarica il report</1> per verificare le informazioni e carica di nuovo il file.',
      },
      invalidFormat: {
        title: 'Dateiformat ungültig',
        description: 'Es können nur Dateien mit Format .csv geladen werden',
      },
    },
    dropArea: {
      title: "Ziehe die .csv-Datei mit der Liste der Aggregierten hierin oder",
      button: 'lade die Datei',
    },
    downloadExampleCsv: 'Du weißt nicht, wie die Datei vorbereitet werden muss? <1>Beispiel herunterladen</1>',
    back: 'Zurück',
    forward: 'Weiter',
  },
  stepAddManager: {
    title: 'Gib den Rechtsvertreter an',
    subTitle: {
      flow: {
        base: `Inserisci i dati del Legale Rappresentante del tuo ente. <1/> Sarà responsabile della firma del contratto per <3>{{productTitle}}</3> <4/> e avrà il ruolo di Amministratore per questo prodotto nell'Area Riservata.`,
        premium: `Inserisci i dati del Legale Rappresentante del tuo ente. <1/> La persona che indicherai sarà firmataria del contratto per <3/> <strong>Premium<strong/>.`,
        addNewUser: `La persona indicata firmerà il Modulo di aggiunta per il nuovo Amministratore e lo <1 />autorizzerà ad operare sul prodotto <3>{{productTitle}}</3> per il tuo ente.`,
      },
    },
    changedManager: {
      title: 'Du fügst einen Rechtsvertreter hinzu',
      message:
        'I dati del Legale Rappresentante inseriti sono diversi da quelli indicati in <1 />precedenza. Vuoi continuare?',
    },
    back: 'Zurück',
    continue: 'Weiter',
  },
  stepAddDelegates: {
    title: "Gib den Administrator an",
    description: {
      flow: {
        onboarding: `Puoi aggiungere da uno a tre Amministratori o suoi delegati. <1/> Saranno i responsabili della gestione di <3>{{productTitle}}</3> e presenti nel contratto di <4 />adesione come delegati dal Legale Rappresentante.`,
        pt: 'Du kannst einen bis drei Administratoren oder dessen/deren Bevollmächtigte hinzufügen.<1/> Sie kümmern sich um die Verwaltung der Benutzer und der Produkte im Auftrag der Körperschaften.',
        addNewUser: `Puoi aggiungere un Amministratore o un suo delegato. Puoi inserire anche la persona che <1 />hai già indicato come Legale Rappresentante. Se aggiungi una persona già presente con un <3 />ruolo diverso per questo prodotto, verrà inserita come Amministratore.`,
      },
    },
    addUserLabel: 'FÜGE EINEN ANDEREN ADMINISTRATOR HINZU',
    addUserLink: 'Füge einen anderen Administrator hinzu',
    backLabel: 'Zurück',
    confirmLabel: 'Weiter',
    formControl: {
      label: 'Füge mich als Administrator hinzu',
    },
  },
  additionalDataPage: {
    title: 'Gib weitere Details ein',
    subTitle:
      'Scegli l’opzione che descrive il tuo ente. Se nessuna è appropriata, seleziona “Altro” e <1 /> inserisci maggiori dettagli.',
    formQuestions: {
      textFields: {
        labels: {
          note: 'Anmerkungen',
          ipa: 'Gib den IPA-Bezugscode ein',
        },
        errors: {
          isEstabilishedRegulatoryProvision: 'Du hast keine Anmerkungen eingegeben',
          fromBelongsRegulatedMarket: 'Du hast keine Anmerkungen eingegeben',
          isFromIPA: 'Gib den IPA-Bezugscode ein',
          isConcessionaireOfPublicService: 'Du hast keine Anmerkungen eingegeben',
          optionalPartyInformations: 'Pflichtfeld',
        },
      },
      estabilishedRegulatoryProvision:
        'L’ente è una società costituita ex lege da un provvedimento normativo',
      belongsRegulatedMarket:
        'L’ente appartiene ad un mercato regolamentato (es. energia, gas, acqua, <1 />trasporti, servizi postali ecc…)',
      registratedOnIPA: 'Die Körperschaft ist in IPA registriert',
      concessionaireOfPublicService: 'Die Körperschaft ist ein Betreiber einer öffentlichen Dienstleistung',
      other: 'Sonstiges',
      optionalPartyInformations: 'Trage die Informationen über deine Körperschaft hier ein',
    },
    options: {
      yes: 'Ja',
      no: 'Nein',
    },
    addNote: 'Eine Anmerkung hinzufügen',
    allowedCharacters: 'Höchstens 300 Zeichen',
  },
  addUser: {
    title: `Aggiungi un nuovo <1 /> Amministratore`,
    subTitle: `Indica per quale prodotto vuoi aggiungere un nuovo<1 />Amministratore`,
    stepSelectProduct: {
      title: 'WÄHLE DAS PRODUKT',
    },
  },
  platformUserForm: {
    helperText: 'Das Feld ist ungültig',
    fields: {
      name: {
        label: 'Name',
        errors: {
          conflict: 'Name falsch oder stimmt nicht mit der Steuernummer überein',
        },
      },
      surname: {
        label: 'Nachname',
        errors: {
          conflict: 'Nachname falsch oder stimmt nicht mit der Steuernummer überein',
        },
      },
      taxCode: {
        label: 'Steuernummer',
        errors: {
          invalid: 'Die eingegebene Steuernummer ist ungültig',
          duplicate: 'Die eingegebene Steuernummer ist bereits vorhanden',
        },
      },
      email: {
        label: 'Institutionelle -E-Mail-Adresse',
        errors: {
          invalid: "Die E-Mail-Adresse ist ungültig",
          duplicate: "Die eingegebene E-Mail-Adresse ist bereits vorhanden",
        },
        description: 'Gib die für die Körperschaft verwendete institutionelle -E-Mail-Adresse ein',
      },
    },
  },
  completeRegistration: {
    sessionModal: {
      onConfirmLabel: 'Erneut laden',
      onCloseLabel: 'Beenden',
    },
    steps: {
      step0: {
        label: "Beitrittsakt laden",
      },
      step1: {
        label: "Beitrittsakt laden",
      },
    },
    request: {
      notFound: {
        title: 'Die von dir gesuchte Seite ist nicht verfügbar',
        description:
          'Al momento non è possibile procedere. Riprova tra qualche <1 />minuto, o contatta l’assistenza',
        contactAssistanceButton: 'Kundendienst kontaktieren',
      },
      expired: {
        product: {
          title: 'Der Beitrittsantrag ist abgelaufen',
          description: `Sono trascorsi oltre 30 giorni dalla richiesta di adesione. Se <2 />desideri ancora aderire al prodotto {{productTitle}}, invia <4 />una nuova richiesta.`,
        },
        user: {
          title: 'Der Antrag ist abgelaufen',
          description: `Sono trascorsi oltre 30 giorni dalla richiesta di aggiunta di <2 />un Amministratore. Per procedere, invia una nuova <2 /> richiesta.`,
        },
        backHome: 'Zurück zur Homepage',
      },
      alreadyCompleted: {
        product: {
          title: 'Der Beitrittsantrag wurde akzeptiert',
        },
        user: {
          title: 'Der Antrag wurde akzeptiert',
        },
        description: `Per gestire il prodotto, accedi tramite SPID o CIE`,
        logIn: 'Anmelden',
      },
      alreadyRejected: {
        product: {
          title: 'Der Beitrittsantrag wurde widerrufen',
          description: `La richiesta di adesione non è andata a buon fine. Se <2 />desideri ancora aderire al prodotto {{productTitle}}, invia <4 />una nuova richiesta.`,
        },
        user: {
          title: 'Der Antrag ist nicht mehr gültig',
          description: `Il tuo ente ha annullato la richiesta. Per aggiungere un <2 />nuovo Amministratore, inviane una nuova.`,
        },
        backHome: 'Zurück zur Homepage',
      },
    },
    outcomeContent: {
      success: {
        product: {
          title: 'Beitritt abgeschlossen!',
          description: `Comunicheremo l'avvenuta adesione all'indirizzo PEC <1/> primario dell'ente. Da questo momento è possibile <3 />accedere all'Area Riservata.`,
        },
        user: {
          title: 'Antrag abgeschlossen',
          description: `Da questo momento gli Amministratori indicati possono <1 />accedere all’Area Riservata.`,
        },
        backHome: 'Zurück zur Homepage',
      },
      error: {
        title: 'Laden fehlgeschlagen',
        description: 'Das Laden des Dokuments ist fehlgeschlagen.',
        backToUpload: 'Erneut laden',
      },
    },
    errors: {
      INVALID_DOCUMENT: {
        title: 'Prüfe das Dokument',
        product: {
          message:
            "Das geladene Dokument entspricht nicht dem Beitrittsakt. Prüfe es auf Korrektheit und lade es erneut.",
        },
        user: {
          message:
            'Il documento caricato non corrisponde al modulo che hai ricevuto via email. Verifica che sia corretto e caricalo di nuovo.',
        },
      },
      INVALID_SIGN: {
        title: 'Prüfe das Dokument',
        product: {
          message:
            'La Firma Digitale non è riconducibile al Legale Rappresentante indicato in fase di adesione. Verifica la corrispondenza e carica di nuovo il documento.',
        },
        user: {
          message:
            'La Firma Digitale non è riconducibile al Legale Rappresentante indicato in fase di richiesta. Verifica la corrispondenza e carica di nuovo il documento.',
        },
      },
      ALREADY_ONBOARDED: {
        title: `L’ente selezionato ha già aderito`,
        message:
          'Per operare sul prodotto, chiedi a un Amministratore di <1 />aggiungerti nella sezione Utenti.',
      },
      GENERIC: {
        title: 'Laden fehlgeschlagen',
        message:
          'Il caricamento del documento non è andato a buon fine. Torna indietro e caricalo di nuovo.',
      },
      INVALID_SIGN_FORMAT: {
        title: 'Laden fehlgeschlagen',
        message:
          'Il caricamento del documento non è andato a buon fine. <1 />Carica un solo file in formato <3>p7m</3>.',
      },
    },
  },
  noProductPage: {
    title: 'Leider ist etwas schiefgelaufen.',
    description: 'Das gewünschte Produkt kann nicht gefunden werden',
  },
  onboarding: {
    success: {
      flow: {
        product: {
          title: 'Beitrittsantrag gesendet',
          publicAdministration: {
            description: `Invieremo un’email all’indirizzo PEC primario dell’ente. <1 /> Al suo interno, ci sono le istruzioni per completare <3 />l’adesione.`,
          },
          notPublicAdministration: {
            description: `Invieremo un’email all’indirizzo PEC indicato. <1 /> Al suo interno, ci sono le istruzioni per completare <3 />l’adesione.`,
          },
        },
        techPartner: {
          title: 'Registrierungsanfrage gesendet',
          description: `Invieremo un’email con l’esito della richiesta all’indirizzo <1 />PEC indicato.`,
        },
        user: {
          title: 'Du hast den Antrag gesendet',
          description: `Invieremo un’email all’indirizzo PEC primario dell’ente. <1 /> Al suo interno, ci sono le istruzioni per completare <3 />l’operazione.`,
        },
      },
    },
    error: {
      title: 'Etwas ist schiefgelaufen.',
      description: `A causa di un errore del sistema non è possibile completare <1 />la procedura. Ti chiediamo di riprovare più tardi.`,
    },
    backHome: 'Zurück zur Homepage',
    sessionModal: {
      title: 'Wirklich beenden?',
      message: 'Wenn du beendest, geht der Beitrittsantrag verloren.',
      onConfirmLabel: 'Beenden',
      onCloseLabel: 'Abbrechen',
    },
    confirmationModal: {
      title: 'Sendeanfrage bestätigen?',
      description: {
        flow: {
          base: 'Du sendest einen Beitrittsantrag zum Produkt  <1>{{productName}}</1> für die Körperschaft <3>{{institutionName}}</3>. <5 /> Die Beitrittsvereinbarung wird im institutionellen PEC-Postfach der Körperschaft eingehen und muss vom Rechtsvertreter unterzeichnet werden. Vergewissere dich, dass du als Mitarbeiter berechtigt bist, diesen Antrag zu stellen.',
          addNewUser: `Stai aggiungendo un nuovo Amministratore per l’ente <1>{{institutionName}}</1>. <3 />L’ente riceverà un modulo alla PEC istituzionale e dovrà essere firmato dal Legale Rappresentante che hai indicato. <3 />Assicurati di essere autorizzato dall’ente a effettuare questa richiesta.`,
        },
      },
      confirm: 'Bestätigen',
      back: 'Zurück',
    },
    loading: {
      loadingText: 'Wir prüfen gerade deine Daten',
    },
    phaseOutError: {
      title: 'Etwas ist schiefgelaufen',
      description:
        'Non puoi aderire al prodotto scelto poiché a breve non sarà <1 /> più disponibile.',
      backAction: 'Zurück zur Homepage',
    },
  },
  onboardingSubProduct: {
    alreadyOnboardedError: {
      title: 'Abonnement bereits erfolgt',
      message:
        "Die von dir gewählte Körperschaft hat das <1 /><strong>Premium</strong>-Angebot bereits abonniert.",
      closeButton: 'Beenden',
    },
    subProductStepSelectPricingPlan: {
      discountLabelData: '25% Skonto bis zum 30. Juni 2023 ',
      title: 'Upgrade auf IO Premium und verbessere die <1/> Nachrichtenleistung',
      firstCheckLabel: 'Du verkürzt die Einnahmezeiten',
      secondCheckLabel: 'Du verbesserst die Einnahmeleistungen',
      thirdCheckLabel: 'Du reduziert Außenstände',
      infoSectionLabel: `Se il tuo ente ha già aderito ad IO, scegli qual è il piano che più soddisfa le sue esigenze. <1/> Il piano a carnet è attivabile una sola volta. Una volta terminato il numero di messaggi del piano a  <3/> carnet, si attiverà automaticamente il piano a consumo. `,
      btnRejectLabel: 'Bin nicht interessiert',
      pricingPlanExitModal: {
        title: 'Möchtest du auf die Premium Angebote verzichten?',
        subtitle: 'Wenn du beendest, hast du Zugang zum reservierten Bereich.',
        closeBtnLabel: 'Beenden',
        confirmBtnLabel: 'Zurück zu den Premium Angeboten',
      },
      headerPlanCard: {
        from: 'Von',
        to: 'bis',
        beyond: 'über',
        mess: '/ Nachrichten',
      },
      carnetPlan: {
        caption: 'CARNET-PLAN - PAUSCHAL',
        discountBoxLabel: '25% Skonto',
        title: 'Wähle unter den {{carnetCount}} verschiedenen und genau auf deine Bedürfnisse zugeschnitten Carnets',
        showMore: 'Mehr hierzu',
        showLess: 'Weniger anzeigen',
        description:
          'Una volta selezionato il carnet non potrà essere modificato per via della sottoscrizione del contratto.',
        carnetLabelsDiscount: {
          c1: 'Spare 55 €',
          c2: 'Spare 543,75 €',
          c3: 'Spare 2.687,50 €',
          c4: 'Spare 5.312,50 €',
          c5: 'Spare 13.125 €',
          c6: 'Spare 25.625 €',
          c7: 'Spare 50.000 €',
        },
        btnActionLabel: 'Aktiviere den Plan',
      },
      consumptionPlan: {
        caption: 'BEDARFSPLAN',
        discountBoxLabel: '25% Skonto',
        title: 'Entscheide dich dafür, nur für die tatsächlich gesendeten Nachrichten <1/> zu zahlen',
        showMore: 'Mehr hierzu',
        showLess: 'Weniger anzeigen',
        description:
          'Attivando il piano a consumo, non sarà più possibile attivare il piano carnet.',
        rangeLabelsDiscount: '25% Skonto',
        btnActionLabel: 'Aktiviere den Plan',
      },
    },
    subProductStepUserUnrelated: {
      title: 'Du kannst {{selectedProduct}} Premium nicht beitreten',
      description:
        'Il tuo ente non ha aderito ad <strong>{{selectedProduct}}</strong>, o non hai un ruolo per <3/>gestire il prodotto. <5/> Chiedi ad un Amministratore di <1/>aggiungerti nella sezione <7/>Utenti, oppure richiedi l’adesione ad <strong>{{selectedProduct}}</strong> per il tuo ente.',
      backHomeLabelBtn: 'Zurück zur Homepage',
      goToBtnLabel: 'Zum Beitritt',
    },
    selectUserPartyStep: {
      title: 'Wähle deine Körperschaft',
      subTitle:
        "Wähle die Körperschaft, für die du ein Abonnement für das <1 />Premium <3>Angebot beantragst</3>",
      searchLabel: 'Körperschaft suchen',
      notFoundResults: 'Kein Ergebnis',
      IPAsubTitle:
        "Wähle aus dem Index der öffentlichen Verwaltung (IPA) die Körperschaft, <1/> für die du den Beitritt zum {{baseProduct}} Premium beantragst",
      helperLink: 'Deine Körperschaft nicht gefunden? <1>Erfahre warum</1>',
      confirmButton: 'Weiter',
    },
    genericError: {
      title: 'Etwas ist schiefgelaufen',
      subTitle:
        'A causa di un errore del sistema non è possibile completare<0 /> la procedura. Ti chiediamo di riprovare più tardi.',
      homeButton: 'Zurück zur Homepage',
    },
    successfulAdhesion: {
      title: 'Der Beitrittsantrag wurde erfolgreich <1/>gesendet',
      message:
        "Du erhältst eine PEC an die institutionelle Adresse der Körperschaft.<1 />Darin findest du die Anweisungen, um das <3 /> Abonnement am <strong>Premium</strong>-Angebot zu vervollständigen.",
      closeButton: 'Schließen',
    },
    billingData: {
      subTitle: `Conferma, modifica o inserisci i dati richiesti, assicurandoti che siano corretti.<1 /> Verranno usati anche per richiedere l’adesione ad altri prodotti e in caso di fatturazione.`,
    },
    exitModal: {
      title: 'Wirklich beenden?',
      message: 'Wenn du beendest, geht der Beitrittsantrag verloren.',
      backButton: 'Beenden',
      cancelButton: 'Abbrechen',
    },
    loading: {
      loadingText: 'Wir prüfen gerade deine Daten',
    },
  },
  invalidPricingPlan: {
    title: 'Etwas ist schiefgelaufen',
    description:
      'Non riusciamo a trovare la pagina che stai cercando. <1 />Assicurati che l’indirizzo sia corretto o torna alla home.',
    backButton: 'Zurück zur Homepage',
  },
  stepInstitutionType: {
    title: 'Wähle die Art der Körperschaft, die du <1/> vertrittst',
    subtitle: 'Gib die Art der Körperschaft an, die <1>{{productName}} beitreten wird</1>',
    institutionTypes: {
      pa: {
        title: 'Öffentliche Verwaltung',
        description: 'Art. 2, Absatz 2, Buchstabe A del digitalen Verwaltungscodes CAD',
      },
      gsp: {
        title: 'Betreiber öffentlicher Dienstleistungen',
        description: 'Art. 2, Absatz 2, Buchstabe B del digitalen Verwaltungscodes CAD',
      },
      scp: {
        title: 'Öffentliche kontrollierte Gesellschaft',
        description: 'Art. 2, Absatz 2, Buchstabe C del digitalen Verwaltungscodes CAD',
      },
      pt: {
        title: 'Technologischer Partner',
        description:
          'Ai sensi di IO - Paragrafo 6.1.3 delle “Linee Guida sul punto di accesso telematico ai servizi della Pubblica Amministrazione” emanate da AgID ai sensi dell’art- 64-bis del CAD',
      },
      psp: {
        title: 'Zahlungsverkehrsdienstleister',
      },
      sa: {
        title: 'Privater Betreiber einer E-Beschaffungsplattform',
      },
      as: {
        title: 'Versicherungsgesellschaft',
      },
      prv: {
        title: 'Private'
      },
      oth: {
        title: 'Sonstiges',
        description: 'Freiwillig beigetretene forderungsberechtigte Körperschaft'
      }
    },
    backLabel: 'Zurück',
    confirmLabel: 'Weiter',
  },
  onboardingFormData: {
    title: 'Gib die Daten der Körperschaft ein',
    pspAndProdPagoPATitle: 'Daten eingeben',
    backLabel: 'Zurück',
    confirmLabel: 'Weiter',
    closeBtnLabel: 'Schließen',
    billingDataPt: {
      title: 'Daten eingeben',
      subTitle:
        'Inserisci le informazioni richieste e assicurati che siano corrette.<1 /> Serviranno a registrarti come Partner tecnologico per il<3 /> prodotto <5>{{nameProduct}}</5>.',
    },
    billingDataSection: {
      invalidFiscalCode: 'Die Steuernummer ist ungültig',
      invalidTaxCodeInvoicing: 'Die eingegebene Steuernummer betrifft nicht deine Körperschaft',
      invalidZipCode: 'Die PLZ ist ungültig',
      invalidVatNumber: 'Die USt-IdNr ist ungültig',
      invalidEmail: 'Die E-Mail-Adresse ist ungültig',
      invalidReaField: 'Das Feld REA ist ungültig',
      invalidMailSupport: 'Die E-Mail-Adresse ist ungültig',
      invalidShareCapitalField: 'Das Feld Stammkapital ist ungültig',
      invalidRecipientCodeNoAssociation: 'Die eingegebene Nummer ist nicht mit deiner Körperschaft verknüpft',
      invalidRecipientCodeNoBilling:
        'Il codice inserito è associato al codice fiscale di un ente che non ha il servizio di fatturazione attivo',
      vatNumberAlreadyRegistered: 'Die eingegebene USt-IdNr. ist bereits registriert worden.',
      vatNumberVerificationErrorTitle: 'Die Prüfung ist fehlgeschlagen',
      vatNumberVerificationErrorDescription:
        'Non è stato possibile verificare la P.IVA al momento. Riprova più tardi.',
      centralPartyLabel: 'Zentrale Körperschaft',
      businessName: 'Firmenbezeichnung',
      aooName: 'AOO-Bezeichnung',
      uoName: 'UO-Bezeichnung',
      aooUniqueCode: 'Eindeutiger AOO-Code',
      uoUniqueCode: 'Eindeutiger UO-Code',
      fullLegalAddress: 'Adresse und Hausnummer des Rechtssitzes',
      zipCode: 'PLZ',
      city: 'Stadt',
      noResult: 'Kein Ergebnis',
      county: 'Provinz',
      country: 'Land',
      digitalAddress: 'PEC-Adresse',
      taxCodeEquals2PIVAdescription: 'Die USt.-IdNr. ist mit der Steuernummer identisch',
      partyWithoutVatNumber: 'Meine Körperschaft hat keine USt.-IdNr.',
      partyWIthoutVatNumberSubtitle: `Indica solo il Codice Fiscale se il tuo ente non agisce nell'esercizio d'impresa,
      arte o professione <1 />(cfr. art. 21, comma 2, lett. f, DPR n. 633/1972)`,
      vatNumberGroup: 'Die USt.-IdNr. ist die einer Gruppe',
      taxCode: 'Steuernummer',
      taxCodeCentralParty: 'Steuernummer der zentralen Körperschaft',
      vatNumber: 'USt.-IdNr.',
      taxCodeInvoicing: 'SFE-Steuernummer',
      originId: 'IVASS-Code',
      sdiCode: 'SDI-Code',
      sdiCodePaAooUo: 'Eindeutiger oder SDI-Code',
      sdiCodePaAooUoDescription:
        'È il codice univoco necessario per ricevere le fatture elettroniche. Può essere del tuo ente o della sua Unità Organizzativa di riferimento.',
      recipientCodeDescription: 'Dieser Code ist für den Empfang elektronischer Rechnungen notwendig',
      gspDescription: 'Ich bin Betreiber mindestens einer der öffentlichen Dienstleistungen: Gas, Energie, Telekommunikation.',
      pspDataSection: {
        commercialRegisterNumber: 'Eintragungsnummer in das Handelsregister',
        invalidCommercialRegisterNumber: 'Die Eintragungsnummer in das Handelsregister ist ungültig',
        registrationInRegister: 'Eintragung im Register',
        registerNumber: 'Nummer des Registers',
        invalidregisterNumber: 'Die Nummer des Registers ist ungültig',
        abiCode: 'ABI-Code',
        invalidabiCode: 'Der ABI-Code ist ungültig',
      },
      informationCompanies: {
        commercialRegisterNumber: 'Eintragungsort in das Handelsregister (fakultativ)',
        requiredRea: 'REA',
        rea: 'REA (fakultativ)',
        shareCapital: 'Stammkapital (fakultativ)',
        requiredCommercialRegisterNumber: 'Eintragungsort in das Handelsregister',
        requiredShareCapital: 'Stammkapital',
      },
      assistanceContact: {
        supportEmail: 'E-Mail-Adresse für Bürger sichtbar',
        supportEmailOptional: 'E-Mail-Adresse für Bürger sichtbar (fakultativ)',
        supportEmailDescriprion:
          'È il contatto che i cittadini visualizzano per richiedere assistenza all’ente',
      },
    },
    taxonomySection: {
      title: 'GEOGRAFISCHES GEBIET ANGEBEN',
      nationalLabel: 'National',
      localLabel: 'Lokal',
      infoLabel:
        'Seleziona il territorio in cui opera il tuo ente. Se locale, puoi scegliere una o più aree di competenza. Se l’ente ha già aderito ad altri prodotti PagoPA, troverai l’area già impostata.',
      localSection: {
        addButtonLabel: 'Gebiet hinzufügen',
        inputLabel: 'Gemeinde, Provinz oder Region',
      },
      error: {
        notMatchedArea: 'Wähle einen Ort aus der Liste',
      },
      modal: {
        addModal: {
          title: 'Du fügst weitere Gebiete für deine Körperschaft hinzu',
          description: `Le aree geografiche verrano aggiunte a tutti i prodotti PagoPA a cui l’ente ha già aderito. Vuoi continuare?`,
          confirmButton: 'Weiter',
          backButton: 'Zurück',
        },
        modifyModal: {
          title: 'Du bearbeitest das geografische Gebiet deiner Körperschaft',
          description:
            'La modifica verrà applicata a tutti i prodotti PagoPA a cui l’ente ha già aderito. Vuoi continuare?',
          confirmButton: 'Weiter',
          backButton: 'Zurück',
        },
      },
    },
    dpoDataSection: {
      dpoTitle: 'KONTAKTE DES DATENSCHUTZBEAUFTRAGTEN',
      dpoAddress: 'Adresse',
      dpoPecAddress: 'PEC-Adresse',
      dpoEmailAddress: 'E-Mail-Adresse',
    },
  },
  rejectRegistration: {
    outcomeContent: {
      success: {
        title: 'Beitrittsantrag gelöscht',
        description:
          'Nella home dell’Area Riservata puoi vedere i prodotti<1 />disponibili e richiedere l’adesione per il tuo ente.',
        backActionLabel: 'Zurück zur Homepage',
      },
      error: {
        title: 'Etwas ist schiefgelaufen.',
        description:
          'A causa di un errore del sistema non è possibile completare la procedura. <1 /> Ti chiediamo di riprovare più tardi.',
        backActionLabel: 'Zurück zur Homepage',
      },
      verify: {
        loadingText: 'Wir prüfen gerade deine Daten',
      },
      delete: {
        loadingText: 'Wir löschen deine Anmeldung',
      },
      jwtNotValid: {
        title: 'Beitrittsantrag nicht mehr <1 /> gültig',
        subtitle: 'Dieser Antrag wurde bewilligt, widerrufen oder ist abgelaufen.',
        backActionLabel: 'Zurück zur Homepage',
      },
    },
    confirmCancellatione: {
      title: 'Möchtest du den <1 /> Beitrittsantrag löschen?',
      subtitle: 'Beim Löschen gehen alle eingegebenen Daten verloren. ',
      confirmActionLabel: 'Antrag löschen',
      backActionLabel: 'Zurück zur Homepage',
    },
  },
  app: {
    sessionModal: {
      title: 'Sitzung abgelaufen',
      message: 'Du wirst zur Anmeldeseite weitergeleitet...',
    },
  },
};