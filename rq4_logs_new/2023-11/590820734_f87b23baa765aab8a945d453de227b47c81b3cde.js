import { useNote } from "../context/NoteContext";
import { IoSettingsSharp } from "react-icons/io5";
import { MdLightMode, MdDarkMode, MdDelete } from "react-icons/md";
import { FaSortAlphaUp, FaSortAlphaUpAlt } from "react-icons/fa";
import { RiMenu4Fill } from "react-icons/ri";
import { useEffect, useRef, useState } from "react";

function Settings() {
  const {
    open,
    handleClick,
    onClearJournal,
    onSortAsc,
    onSortDesc,
    onInputSort,
  } = useNote();

  const [settings, setSettings] = useState(false);
  const settingsRef = useRef();

  useEffect(() => {
    const handleBodyClick = (e) => {
      if (settingsRef.current && !settingsRef.current.contains(e.target)) {
        setSettings(false);
      }
    };
    document.addEventListener("click", handleBodyClick);

    return () => {
      document.removeEventListener("click", handleBodyClick);
    };
  }, []);

  return (
    <>
      <span
        onClick={() => setSettings((set) => !set)}
        className="settings"
        ref={settingsRef}
      >
        <IoSettingsSharp />
      </span>

      {!settings && <span className="disp">settings</span>}

      {settings && (
        <ul className="settings-items">
          <li onClick={handleClick}>
            {open ? (
              <span className="icon">
                <MdLightMode />
              </span>
            ) : (
              <span className="icon">
                <MdDarkMode />
              </span>
            )}{" "}
            <span>light mode</span>
          </li>
          <li onClick={onSortAsc}>
            <span className="icon">
              <FaSortAlphaUp />
            </span>
            <span>sort by title</span>
          </li>
          <li onClick={onSortDesc}>
            <span className="icon">
              <FaSortAlphaUpAlt />
            </span>
            <span>sort by title</span>
          </li>
          <li onClick={onInputSort}>
            <span className="icon">
              <RiMenu4Fill />
            </span>
            <span>sort by input</span>
          </li>
          <li onClick={onClearJournal}>
            <span className="icon">
              <MdDelete />
            </span>
            <span>delete all notes</span>
          </li>
        </ul>
      )}
    </>
  );
}

export default Settings;