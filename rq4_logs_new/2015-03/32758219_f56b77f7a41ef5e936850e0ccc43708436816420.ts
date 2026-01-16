

module Models {
  // export interface Timestamped { }
  export interface SGV {
        /** Epoch */
        date: number;
        /** dateString, prefer ISO `8601` */
        dateString: string;
        /** The glucose reading */
        sgv: string;
        /** Enum of direction */
        trend?: number;
        /** Direction of glucose reported by Dexcom. */
        direction: string;
        /** Noise level at time of reading. */
        noise?: number;

  }

  class Timestamped {
        date: number;
        /** dateString, prefer ISO `8601` */
        dateString: string;
  }

  class Entry extends Timestamped implements SGV {
        sgv: string;
        direction: string;
  }

}
