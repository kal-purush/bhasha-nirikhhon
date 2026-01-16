import { BlockCommandCallback } from '@logseq/libs/dist/LSPlugin.user';
import { ExportFormat } from '../core/types';
import { getExporter } from '../exports';

declare const logseq: any;

/**
 * Register export commands with Logseq
 */
export function registerExportCommands(): void {
  try {
    // Register export commands for each format
    registerExportCommand('Slack');
    registerExportCommand('WhatsApp');
    registerExportCommand('GoogleDocs');
    registerExportCommand('RichText');
  } catch (error) {
    console.error('Error registering export commands:', error);
  }
}

/**
 * Register an export command for a specific format
 * @param format The format to register a command for
 */
function registerExportCommand(format: ExportFormat): void {
  // Create callback function for this format
  const exportCallback: BlockCommandCallback = async (e) => {
    try {
      // Get the appropriate exporter
      const exporter = getExporter(format);

      // Export the block
      await exporter.exportBlock(e.uuid);
    } catch (error) {
      console.error(`Error in ${format} export command:`, error);
      logseq.UI.showMsg(`Error exporting to ${format} format`);
    }
  };

  // Format the menu item text
  const menuText = formatToDisplayName(format);

  // Register with Logseq
  logseq.Editor.registerBlockContextMenuItem(`Export to ${menuText}`, exportCallback);
}

/**
 * Format export format enum to display name
 * @param format The format enum value
 * @returns Formatted display name
 */
function formatToDisplayName(format: ExportFormat): string {
  switch (format) {
    case 'GoogleDocs':
      return 'Google Docs';
    default:
      return format;
  }
}