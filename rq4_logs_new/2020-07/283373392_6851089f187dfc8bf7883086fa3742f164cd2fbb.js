// @flow

import React from "react";
import { trackIds, tracks, categoryColorScale } from "../constants";
import type { MilestoneMap, TrackId } from "../constants";

type Props = {
  milestoneByTrack: MilestoneMap,
  focusedTrackId: TrackId,
  setFocusedTrackIdFn: (TrackId) => void,
};

const arr = [
  {
    label: "Mobile",
    value: 1,
  },
  {
    label: "Web Client",
    value: 2,
  },
  {
    label: "Foundations",
    value: 3,
  },
  {
    label: "Project Management",
    value: 4,
  },
];

class NewTrackSelector extends React.Component<Props> {
  render() {
    return (
      <div style={{ display: "flex", flexGap: 15, maxWidth: "100px" }}>
        <style jsx>{`
          .value {
            line-height: 50px;
            width: 50px;
            padding: 10px;
            text-align: center;
            background: #eee;
            font-weight: bold;
            font-size: 24px;
            border-radius: 3px;
            cursor: pointer;
          }
          .label {
            justify-content: center;
            align-items: flex-start;
            font-size: 9px;
          }

          .col {
            margin-right: 10px;
          }

          .grid {
            width: 100%;
            display: flex;
          }
        `}</style>

        <div className="grid">
          {arr.map((a) => {
            return (
              <div className="col">
                <div className="label">{a.label}</div>
                <div className="value">{a.value}</div>
              </div>
            );
          })}
        </div>
      </div>
      //   <div style={{ display: "flex" }}>
      //     <style jsx>{`
      //       .track-selector-value {
      //         line-height: 50px;
      //         width: 50px;
      //         padding: 10px;
      //         text-align: center;
      //         background: #eee;
      //         font-weight: bold;
      //         font-size: 24px;
      //         border-radius: 3px;
      //         cursor: pointer;
      //       }
      //       .track-selector-label {
      //         justify-content: center;
      //         align-items: flex-start;
      //         font-size: 9px;
      //       }
      //     `}</style>
      //     {trackIds.map((trackId) => {
      //       return (
      //         <div>
      //           <div
      //             key={`label-${trackId}`}
      //             className="track-selector-label"
      //             onClick={() => this.props.setFocusedTrackIdFn(trackId)}
      //           >
      //             {tracks[trackId].displayName}
      //           </div>
      //           <div
      //             key={`value-${trackId}`}
      //             className="track-selector-value"
      //             style={{
      //               border:
      //                 "4px solid " +
      //                 (trackId == this.props.focusedTrackId
      //                   ? "#000"
      //                   : categoryColorScale(tracks[trackId].category)),
      //               background: categoryColorScale(tracks[trackId].category),
      //             }}
      //             onClick={() => this.props.setFocusedTrackIdFn(trackId)}
      //           >
      //             {this.props.milestoneByTrack[trackId]}
      //           </div>
      //         </div>
      //       );
      //     })}
      //   </div>
    );
  }
}

export default NewTrackSelector;