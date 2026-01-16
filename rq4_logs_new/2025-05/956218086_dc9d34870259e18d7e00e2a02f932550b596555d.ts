import Component from './render';
import rehypePlugin from './rehypePlugin';

// Define the tool use and tool result tags
export const TOOL_USE_TAG = 'tool_use';
export const TOOL_RESULT_TAG = 'tool_use_result';

// Define a consistent interface for the plugin
type MCPCallElement = {
  Component: typeof Component;
  rehypePlugin: typeof rehypePlugin;
  tag: string;
};

// Create the plugin object
const MCPCallElement: MCPCallElement = {
  Component,
  rehypePlugin,
  tag: 'pre', // We'll use 'pre' as our main tag since we'll handle both tool_use and tool_use_result
};

export default MCPCallElement;