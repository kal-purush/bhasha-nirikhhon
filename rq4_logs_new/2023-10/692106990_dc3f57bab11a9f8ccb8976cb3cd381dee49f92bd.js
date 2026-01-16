import styles from "@styles/TeamBuilder/TeamBuilder.module.css";
import pokemon from "@shared/pokemon.json";
import moves from "@shared/moves.json";
import { useState } from "react";

export default function TeamBuilder({ team, setTeam, onGameStart }) {
  const [customizing, setCustomizing] = useState(null);
  const [selectedMoves, setSelectedMoves] = useState([]);
  const [evStats, setEVStats] = useState(getEVStats());
  const remainingEV =
    510 -
    evStats.reduce(
      (acc, stat) => acc + parseInt(stat.val === "" ? 0 : stat.val, 10),
      0
    );

  function getEVStats() {
    return [
      { key: "hp", name: "HP", val: 0 },
      { key: "attack", name: "Attack", val: 0 },
      { key: "defense", name: "Defense", val: 0 },
    ];
  }

  return customizing ? (
    <>
      <div className={styles.editContainer}>
        <div className={styles.top}>
          <div className={styles.left}>
            <div className={styles.cardContainer}>
              <div className={styles.card}>
                <img src={customizing.sprites.front} alt={customizing.name} />
                <div>
                  #{customizing.id} {customizing.name}
                </div>
              </div>
              <div className={styles.level}>
                <div>Level</div>
                <div className={styles.numInput}>
                  <div
                    onClick={() =>
                      setCustomizing((prev) => {
                        const updatedChar = { ...prev };
                        const prevVal = prev.level === "" ? 1 : prev.level;
                        let val = prevVal > 1 ? prevVal - 1 : 1;
                        updatedChar.level = val;
                        return updatedChar;
                      })
                    }
                  >
                    -
                  </div>
                  <input
                    value={customizing.level}
                    onChange={(e) =>
                      setCustomizing((prev) => {
                        const updatedChar = { ...prev };
                        let val = e.target.value.replace(/\D/g, "");
                        val = val === "" ? "" : Math.min(100, Math.max(1, val));
                        updatedChar.level = val;
                        return updatedChar;
                      })
                    }
                  />
                  <div
                    onClick={() =>
                      setCustomizing((prev) => {
                        const updatedChar = { ...prev };
                        const prevVal = prev.level === "" ? 0 : prev.level;
                        let val = prevVal < 100 ? prevVal + 1 : 100;
                        updatedChar.level = val;
                        return updatedChar;
                      })
                    }
                  >
                    +
                  </div>
                </div>
              </div>
            </div>
            <table className={styles.statsTable}>
              <thead>
                <tr className={styles.statRow}>
                  <th></th>
                  <td></td>
                  <td></td>
                  <td>EV</td>
                </tr>
              </thead>
              <tbody>
                {evStats.map((stat, idx) => {
                  const statPerc = customizing.baseStats[stat.key] / 150;
                  return (
                    <tr key={stat.key} className={styles.statRow}>
                      <th>{stat.name}</th>
                      <td>{customizing.baseStats[stat.key]}</td>
                      <td className={styles.bar}>
                        <div
                          style={{
                            width: `${statPerc * 100}%`,
                            height: "0.5em",
                            backgroundColor:
                              statPerc < 0.2
                                ? "rgb(200, 0, 0)"
                                : statPerc < 0.5
                                ? "rgb(200, 200, 0)"
                                : "rgb(0, 200, 0)",
                          }}
                        />
                      </td>
                      <td className={styles.numInput}>
                        <div
                          onClick={() =>
                            setEVStats((prev) => {
                              const updatedEVStats = [...prev];
                              const prevVal =
                                prev[idx].val === "" ? 0 : prev[idx].val;
                              let val = prevVal > 0 ? prevVal - 1 : 0;
                              updatedEVStats[idx].val = val;
                              return updatedEVStats;
                            })
                          }
                        >
                          -
                        </div>
                        <input
                          value={stat.val}
                          onChange={(e) =>
                            setEVStats((prev) => {
                              const updatedEVStats = [...prev];
                              let val = e.target.value.replace(/\D/g, "");
                              if (val !== "") {
                                val =
                                  val > remainingEV
                                    ? prev[idx].val + remainingEV
                                    : val;
                                val = Math.min(252, Math.max(0, val));
                              }
                              updatedEVStats[idx].val = val;
                              return updatedEVStats;
                            })
                          }
                        />
                        <div
                          onClick={() =>
                            setEVStats((prev) => {
                              const updatedEVStats = [...prev];
                              const prevVal =
                                prev[idx].val === "" ? 0 : prev[idx].val;
                              let val = prevVal < 252 ? prevVal + 1 : 252;
                              if (remainingEV > 0) {
                                updatedEVStats[idx].val = val;
                              }
                              return updatedEVStats;
                            })
                          }
                        >
                          +
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className={styles.statRow}>
                  <th></th>
                  <td></td>
                  <td></td>
                  <td style={{ paddingTop: "5px" }}>{remainingEV}</td>
                </tr>
              </tfoot>
            </table>
          </div>
          <div className={styles.right}>
            <div>{selectedMoves.length}/4 moves selected</div>
            <div className={styles.moveContainer}>
              {customizing.moves.map((move) => {
                const moveData = moves[move.move];
                const selected = selectedMoves.find(
                  (selected) => selected.move === move.move
                );
                return (
                  <div
                    key={move.move}
                    className={styles.moveOption}
                    style={{
                      backgroundColor: selected
                        ? "rgb(160, 231, 160)"
                        : "white",
                    }}
                    onClick={() => {
                      if (selected) {
                        setSelectedMoves((prev) =>
                          prev.filter((selMove) => selMove.move !== move.move)
                        );
                      } else if (selectedMoves.length < 4) {
                        setSelectedMoves((prev) => [...prev, move]);
                      }
                    }}
                  >
                    <div>{moveData.name}</div>
                    <div>Lv. {move.level}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
        <div className={styles.bottom}>
          <div className={styles.button} onClick={() => setCustomizing(null)}>
            Back
          </div>
          <div
            className={`${styles.button} ${
              selectedMoves.length ? "" : styles.disabled
            }`}
            onClick={() => {
              if (selectedMoves.length) {
                setTeam((prev) => [
                  ...prev,
                  {
                    ...customizing,
                    evStats: evStats.reduce((evObject, stat) => {
                      evObject[stat.key] = stat.val === "" ? 0 : stat.val;
                      return evObject;
                    }, {}),
                    moves: selectedMoves,
                  },
                ]);
                setCustomizing(null);
              }
            }}
          >
            Save
          </div>
        </div>
      </div>
      <div className={styles.button} style={{ visibility: "hidden" }} />
    </>
  ) : (
    <>
      <div className={styles.container}>
        {pokemon.map((poke) => (
          <div
            key={poke.id}
            onClick={() => {
              setSelectedMoves([]);
              setEVStats(getEVStats());
              setCustomizing(
                poke.level !== undefined ? poke : { ...poke, level: 100 }
              );
            }}
            className={styles.card}
          >
            <img src={poke.sprites.front} alt={poke.name} />
            <div>
              #{poke.id} {poke.name}
            </div>
          </div>
        ))}
      </div>
      <div className={styles.container}>
        {Array.from({ length: 6 }, (value, index) => {
          const pokemon = team[index];
          return (
            <div
              key={index}
              className={styles.card}
              style={{ cursor: pokemon ? "pointer" : "default" }}
            >
              {pokemon ? (
                <>
                  <img src={pokemon.sprites.front} alt={pokemon.name} />
                  <div>{pokemon.name}</div>
                </>
              ) : (
                <>
                  <img src="/assets/img/pokeball.png" alt="Pokeball" />
                  <div>#{index + 1}</div>
                </>
              )}
            </div>
          );
        })}
      </div>
      <div className={styles.button} onClick={onGameStart}>
        Start
      </div>
    </>
  );
}