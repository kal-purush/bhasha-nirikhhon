import { Picker } from "emoji-mart";
import { useState } from "react";
import { ModalBackground } from "../Modal";

import "./styles.scss";

export function ChooseAvatar({
  openModal,
  setOpenModal,
  setPlay,
  avatar,
  setAvatar,
  name,
  setName,
}) {
  const [showPicker, setShowPicker] = useState(false);

  return (
    <ModalBackground open={openModal} setOpen={setOpenModal}>
      <div className="choose-avatar">
        <h4>Informações do jogador</h4>

        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="choose-avatar-input"
          placeholder="Qual seu nome?"
        />

        <button
          className="choose-avatar-button"
          onClick={() => setShowPicker(!showPicker)}
        >
          {avatar}
        </button>

        {showPicker ? (
          <div className="choose-avatar-picker">
            <Picker
              showSkinTones={false}
              set="twitter"
              showPreview={false}
              color="#0351BD"
              exclude={["flags", "recent"]}
              search={false}
              onSelect={(event) => setAvatar(event.native)}
            />
          </div>
        ) : null}

        <button className="play" onClick={setPlay}>
          Play
        </button>
      </div>
    </ModalBackground>
  );
}