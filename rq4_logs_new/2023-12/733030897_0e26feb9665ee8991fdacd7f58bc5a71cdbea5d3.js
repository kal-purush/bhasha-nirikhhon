import {useCurrentStudent} from "../CurrentStudentContext";
import {useKameraContext} from "../Kamera/KameraViewContext";
import "./Mobile.css"
import {useEffect, useState} from "react";
import Modal from "../Modal";
import axios from "axios";
import {useKlassenListeContext} from "../KlassenListeContext";

export default function Mobile(props) {
    const {currentStudent, setCurrentStudent} = useCurrentStudent();
    const [kameraView, setKameraView] = useKameraContext();
    const [modal, setModal] = useState(false);
    const clickCount = props.clickCount;
    const setClickCount = props.setClickCount;

    const [zimmerView, setZimmerView] = useState(false);
    const [miniKlassenzimmer, setMiniKlassenzimmer] = useState(null);

    // TODO - Pass in either a schueler object or null - conditionally rendering the image and
    const [schueler, setSchueler] = useState(
        props.getSchuelerByPosition(1)
    );

    useEffect(() => {
        if (currentStudent !== null) {
            setKameraView(true);
        }
    }, [currentStudent]); // This effect runs whenever schueler changes

    const [name, setName] = useState("")
    const nameAuswaehlen = (schueler) => {

        let freierPlatz = naechsterFreierPlatz();
        if (freierPlatz === -1) {
            return;
        }
        axios.post("http://localhost:8080/sitzplan/positionieren/" + freierPlatz, schueler, {
            headers: {
                "Content-Type": "application/json"
            }
        })
            .then(response => {
                setSchueler(response.data)
                setCurrentStudent(response.data);
            })
            .catch(error => {
                console.error("Error:", error);
            });
    }

    const naechsterFreierPlatz = () => {
        let ANZAHL_SCHUELER = props.klassenzimmer.anzahlDerReihe
            * props.klassenzimmer.anzahlDerTischeProReihe
            * props.klassenzimmer.anzahlDerSchuelerProTisch;

        for (let i = 1; i < ANZAHL_SCHUELER; i++) {
            let platzIstFrei = true;
            for (let schueler of props.klassenzimmer.schuelerListe) {
                if (schueler.position === i) {
                    platzIstFrei = false;
                }
            }
            if (platzIstFrei) {
                console.log(i);
                return i;
            }
        }
        return -1;
    }

    const [klassenzimmerListe, setKlassenzimmerListe] = useState([]);

    function makeKlassenzimmerListeKomponente() {
       return (
           <div className="zimmer-liste">
            {klassenzimmerListe.map((zimmer, index) => (
                <div className="zimmer-namen" key={index} onClick={() => props.setKlassenzimmerId(zimmer.id)}>
                    {zimmer.name}
                </div>
            ))}
        </div>)
    }

    useEffect(() => {
        axios.get("http://localhost:8080/sitzplan/meinklassenzimmer")
            .then(response => {
                setKlassenzimmerListe(response.data);
            })
    }, [props.klassenzimmerId]);

    const getKlassenzimmerName = () => {
        for (let klassenzimmer of klassenzimmerListe) {
            if (klassenzimmer.id === props.klassenzimmerId) {
                return klassenzimmer.name;
            }
        }
        return null
    }

    const miniKlassenzimmerBauen = () => {

        const reihen = Array.from(Array(props.klassenzimmer.anzahlDerReihe).keys()).map(x => x + 1);
        const schuelerProReihe = Array.from(Array(props.klassenzimmer.anzahlDerTischeProReihe
            * props.klassenzimmer.anzahlDerSchuelerProTisch).keys()).map(x => x + 1);


        let miniZimmer = (

            <div className="mini-klassenzimmer">
                {reihen.map((index) => (
                    <div className="minizimmer-reihe" key={index}>
                        {schuelerProReihe.map((index) =>
                            <div key={index} className="minizimmer-schueler">
                                B
                            </div>
                        )
                        }
                    </div>
                ))}
            </div>
        );

        setMiniKlassenzimmer(miniZimmer);
    }

        return (
            <div className="mobile-ansicht">

                {zimmerView ? (
                    <>
                        {miniKlassenzimmer}
                        <button className="mobile-button zurueck-button" onClick={() => {
                            setZimmerView(false);
                        }}
                        > Zurück
                        </button>
                    </>
                ) : (
                    <>
                        {props.klassenzimmerId === -1 ? (
                            <>
                                <h2 className="zimmerauswahl-titel">Klassenzimmer auswählen: </h2>
                                {makeKlassenzimmerListeKomponente()}
                            </>
                        ) : (
                            <div className={"KlassenzimmerRoute"}>
                                <h2> Klassenzimmer {getKlassenzimmerName()}</h2>
                                <button className="mobile-button anlegen-button" onClick={() => {
                                    setModal(true);
                                    setClickCount(clickCount + 1);
                                }}
                                > Schüler anlegen
                                </button>

                                <button className="mobile-button neuauswahl-button" onClick={() => {
                                    props.setKlassenzimmerId(-1);
                                }}
                                > Anderes Zimmer auswählen
                                </button>

                                <button className="mobile-button mini-klassenzimmer-button" onClick={() => {
                                    miniKlassenzimmerBauen();
                                    setZimmerView(true);
                                }}
                                > Mini-Klassenzimmer Anschauen
                                </button>

                                <Modal modal={modal} setModal={setModal} nameAuswaehlen={nameAuswaehlen}
                                       klassenzimmerId={props.klassenzimmerId}/>
                            </div>
                        )
                        }
                    </>
                )}
            </div>
        )
    }