import { useState } from "react";
import { useTranslation } from "react-i18next";

import { COUNTER_TYPES as CTYPES } from "../utils/constants";
import Box from "./ui/Box";
import Modal from "./ui/Modal";

export default function AddCounter({
  canRestart,
  open,
  onClose,
  createCounter,
  sets,
  enableSide,
}) {
  const [custom, setCustom] = useState("");
  const { t } = useTranslation();

  const byName = (a, b) => t(a.name).localeCompare(t(b.name));

  const handleSubmitCounter = (e) => {
    e.preventDefault();
    custom && createCounter(CTYPES.CUSTOM, custom);
    setCustom("");
  };

  return (
    open && (
      <Modal onClose={onClose}>
        <Box key="Add counters" title={t("Add counters")} flat flag>
          <fieldset>
            <legend>- {t("Side schemes")}</legend>
            {sets.sideSchemes
              .filter((c) => !c.active)
              .sort(byName)
              .map((counter) => (
                <div
                  key={counter.id}
                  onClick={() => enableSide(counter)}
                  className="option"
                >
                  {counter.name}
                </div>
              ))}
          </fieldset>
          <fieldset>
            <legend>- {t("Extra counters")}</legend>
            {[CTYPES.ALLY, CTYPES.MINION, CTYPES.SUPPORT, CTYPES.UPGRADE].map(
              (type) => (
                <div
                  key={type}
                  onClick={() => createCounter(type)}
                  className="option"
                >
                  {t("Add type Counter", { type: t(type) })}
                </div>
              )
            )}
          </fieldset>
          <fieldset>
            <legend>- {t("Custom")}</legend>
            <form onSubmit={handleSubmitCounter}>
              <input
                placeholder={t("Name")}
                value={custom}
                onChange={(e) => setCustom(e.target.value)}
                type="text"
              />{" "}
              <button onClick={handleSubmitCounter} className="small">
                +
              </button>
            </form>
          </fieldset>
        </Box>
      </Modal>
    )
  );
}