import {Observable, ReplaySubject, Subject} from "rxjs";
import {getIsMeFunction, ICard} from "../Interfaces/ICard";
import {Dictionary, flatten} from "lodash";
import {TrieWrapper} from "../TrieWrapper";
import trie from "trie-prefix-tree";
import {Manager} from "../Manager";
import {buffer, concatMap, debounceTime, filter, flatMap, map, scan, shareReplay, startWith} from "rxjs/operators";
import {Settings} from "../Interfaces/Message";

export default class CardManager {
    addPersistedCards$: Subject<ICard[]> = new Subject<ICard[]>();
    addUnpersistedCards$ = new Subject<ICard[]>();
    cardIndex$!: Observable<Dictionary<ICard[]>>;
    cardLoadingSignal$ = new ReplaySubject<boolean>(1);
    trie = new TrieWrapper(trie([]));

    static mergeCardIntoCardDict(newICard: ICard, o: { [p: string]: ICard[] }) {
        const detectDuplicateCard = getIsMeFunction(newICard);
        let presentCards = o[newICard.learningLanguage];
        if (presentCards) {
            const indexOfDuplicateCard = presentCards.findIndex(detectDuplicateCard);
            if (indexOfDuplicateCard >= 0) {
                const presentCard = presentCards[indexOfDuplicateCard];
                if (newICard.timestamp > presentCard.timestamp) {
                    presentCards[indexOfDuplicateCard] = newICard;
                }
            } else {
                presentCards.push(newICard)
            }
        } else {
            o[newICard.learningLanguage] = [newICard]
        }
    }

    constructor(public m: Manager) {
        this.cardIndex$ = this.getCardIndex();
        this.addUnpersistedCards$.pipe(
            flatMap(async cards => {
                for (let i = 0; i < cards.length; i++) {
                    const card = cards[i];
                    card.id = await this.m.db.cards.add(card);
                }
                return cards;
            })
        ).subscribe(this.addPersistedCards$);
    }

    private getCardIndex() {
        return this.addPersistedCards$.pipe(
            startWith([]),
            buffer(this.cardLoadingSignal$.pipe(filter(v => !v))),
            map(flatten),
            scan((cardIndex: Dictionary<ICard[]>, newCards) => {
                const o = {...cardIndex};
                newCards.forEach(newICard => {
                    CardManager.mergeCardIntoCardDict(newICard, o);
                    this.trie.addWords(newICard.learningLanguage);
                });
                return o;
            }, {}),
            shareReplay(1)
        );
    }

    async load() {
        this.cardLoadingSignal$.next(true);
        let unloadedCardCount = await this.m.db.getCardsInDatabaseCount()
        if (unloadedCardCount) {
            await this.getCardsFromDB();
        }
        this.cardLoadingSignal$.next(false);
    }

    private async getCardsFromDB() {
        const priorityCards = await this.m.db.settings.where({key: Settings.MOST_POPULAR_WORDS}).first();
        const priorityWords = priorityCards?.value || [];
        for await (let cards of this.m.db.getCardsFromDB({learningLanguage: priorityWords}, 100)) {
            this.addPersistedCards$.next(cards);
        }
        for await (let cards of this.m.db.getCardsFromDB({}, 500)) {
            this.addPersistedCards$.next(cards);
        }
    }
}