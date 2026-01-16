/**
 * A 'Line' is a single message that the server emits.
 */
export type Line = string;

/**
 * A collapsible 'Row' has a collection of lines in the 'lines' key.
 * Header is shown in case of collapsed.
 */
export type Row = {
  lines: Line[];
  header: Line;
};