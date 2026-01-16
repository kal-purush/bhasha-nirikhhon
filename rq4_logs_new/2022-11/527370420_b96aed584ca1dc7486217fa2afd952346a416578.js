import React, { useState } from "react";
import ButtonComponent from "../../common/button_component/button_component";
import {
  AiOutlineDelete,
  AiOutlineDown,
  AiOutlineEdit,
  AiOutlineMenu,
  AiOutlinePlus,
  AiOutlineUp,
} from "react-icons/ai";
import IconComponent from "../../common/icon_component/icon";
import SelectComponent from "../../common/input_component/select_component/select_component";
import {
  Actions,
  dateList,
  functionList,
} from "../static/performance_data_list";
import DropdownIconComponent from "./dropdown_icon_component";
import { IoLocationSharp } from "react-icons/io5";

export const CascadedTable = ({ columns }) => {
  const [subRows, setSubRows] = useState([]);

  const toggleSubRows = (id) => {
    const shownState = subRows.slice();
    const index = shownState.indexOf(id);

    if (index >= 0) {
      shownState.splice(index, 1);
      setSubRows(shownState);
    } else {
      shownState.push(id);
      setSubRows(shownState);
    }
  };

  return (
    <div>
      <div>
        <div className="text-end">
          <span>For Revision</span>
        </div>
        <div className="button-position">
          <ButtonComponent
            buttonLogoStart={<AiOutlinePlus />}
            buttonName="NEW"
          />
          <div className="select-container">
            <SelectComponent itemList={dateList} className="select" />
            <SelectComponent itemList={functionList} className="select" />
          </div>
        </div>
        <div className="table-container">
          <table className="table-content">
            <thead>
              <tr>
                <th rowSpan={"2"} colSpan={"1"}>
                  Project/Activity
                </th>
                <th colSpan={"2"}>O/DPCR Level</th>
                <th colSpan={"3"}>IPCR Level</th>
              </tr>
              <tr>
                <th>Major Final Output</th>
                <th>Success Indicator</th>
                <th>Responsible</th>
                <th>Major Final Output</th>
                <th>Success Indicator</th>
              </tr>
            </thead>
            <tbody>
              {columns.map((item, index) => (
                <React.Fragment>
                  <tr key={item.prj_id}>
                    <td colSpan={6}>
                      <div className="flex-between">
                        <span style={{ color: "black" }}>{item.prj_title}</span>
                        <div onClick={() => toggleSubRows(item.prj_id)}>
                          <span>
                            {subRows.includes(item.prj_id) ? (
                              <AiOutlineDown />
                            ) : (
                              <AiOutlineUp />
                            )}
                          </span>
                        </div>
                      </div>
                    </td>
                  </tr>
                  {subRows.includes(item.prj_id) &&
                    item.mfo_info.map((mfo_item, mfo_index) => (
                      <React.Fragment>
                        <tr key={mfo_item.mfo_id}>
                          <td className="no-border"></td>
                          <td colSpan={5}>
                            <div className="flex-between">
                              <span style={{ color: "black" }}>
                                {mfo_item.mfo_title}
                              </span>
                              <div
                                onClick={() =>
                                  toggleSubRows(
                                    Number(item.prj_id + "." + mfo_item.mfo_id)
                                  )
                                }
                              >
                                <span>
                                  {subRows.includes(
                                    Number(item.prj_id + "." + mfo_item.mfo_id)
                                  ) ? (
                                    <AiOutlineDown />
                                  ) : (
                                    <AiOutlineUp />
                                  )}
                                </span>
                              </div>
                            </div>
                          </td>
                        </tr>
                        {subRows.includes(
                          Number(item.prj_id + "." + mfo_item.mfo_id)
                        ) &&
                          mfo_item.mfo_ind_info.map((ind_item, ind_index) => (
                            <React.Fragment>
                              <tr key={ind_item.ind_id}>
                                <td className="no-border"></td>
                                <td className="no-border"></td>
                                <td
                                  className="no-border-btm"
                                  style={{ padding: "0" }}
                                >
                                  <div className="flex">
                                    <IoLocationSharp className="red absolute right-comment-cascaded pointer" />
                                    <span
                                      style={{
                                        paddingLeft: "0.5em",
                                        color: "black",
                                      }}
                                    >
                                      {ind_item.ind_title}
                                    </span>
                                  </div>
                                </td>
                                <td>
                                  <div className="flex">
                                    <div style={{ paddingBottom: "0.5em" }}>
                                      <DropdownIconComponent
                                        dropdownLogo={<AiOutlineMenu />}
                                        itemList={Actions}
                                      />
                                    </div>
                                    <IoLocationSharp className="red absolute right-comment-cascaded-1 pointer" />
                                    <span className="black pd-left">
                                      {ind_item.ind_ipcr[0].ipcr_responsible}
                                    </span>
                                  </div>
                                </td>
                                <td>
                                  <IoLocationSharp className="red absolute right-comment-cascaded-1 pointer" />
                                  <span className="black">
                                    {ind_item.ind_ipcr[0].ipcr_mfoutput}
                                  </span>
                                </td>
                                <td>
                                  <IoLocationSharp className="red absolute right-comment-cascaded pointer" />
                                  <span className="black">
                                    {
                                      ind_item.ind_ipcr[0]
                                        .ipcr_success_indicator
                                    }
                                  </span>
                                </td>
                              </tr>

                              {ind_item.ind_ipcr
                                .slice(1)
                                .map((ipcr_item, ipcr_index) => (
                                  <React.Fragment>
                                    <tr>
                                      <td className="no-border"></td>
                                      <td className="no-border"></td>
                                      <td className="no-border"></td>
                                      <td>
                                        <div className="flex">
                                          <div
                                            style={{ paddingBottom: "0.5em" }}
                                          >
                                            <DropdownIconComponent
                                              dropdownLogo={<AiOutlineMenu />}
                                              itemList={Actions}
                                            />
                                          </div>
                                          <IoLocationSharp className="red absolute right-comment-cascaded-1 pointer" />
                                          <span className="black pd-left">
                                            {ipcr_item.ipcr_responsible}
                                          </span>
                                        </div>
                                      </td>
                                      <td>
                                        <IoLocationSharp className="red absolute right-comment-cascaded-1 pointer" />
                                        <span className="black">
                                          {ipcr_item.ipcr_mfoutput}
                                        </span>
                                      </td>
                                      <td>
                                        <IoLocationSharp className="red absolute right-comment-cascaded pointer" />
                                        <span className="black">
                                          {ipcr_item.ipcr_success_indicator}
                                        </span>
                                      </td>
                                    </tr>
                                  </React.Fragment>
                                ))}

                              <tr>
                                <td className="no-border"></td>
                                <td className="no-border"></td>
                                <td className="no-border"></td>
                                <td colSpan={3}>
                                  <IconComponent
                                    icon={<AiOutlinePlus />}
                                    onClick={() => {}}
                                  />
                                </td>
                              </tr>
                            </React.Fragment>
                          ))}
                      </React.Fragment>
                    ))}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};