import path from 'path';
import os from 'os';
import axios from 'axios';
import semver from 'semver';
import yaml from 'yaml'
import { VMBackend } from './backend';
import { merge } from 'lodash'
import SERVICE_SUBNET from '../assets/scripts/service-subnet.initd?raw'
import { Log } from '../utils/logging'

const SUBNET_NODE_DATA = "/var/lib/subnet-node";

class SubnetNode {
  backend: VMBackend
  logger: Log

  constructor(backend:VMBackend, log: Log) {
    this.backend = backend;
    this.logger = log;
  }


  /**
   * Read the subnet configuration from /root/.subnet-node/config.yaml
   */
  async getConfig(): Promise<any> {
    const configPath = SUBNET_NODE_DATA + '/config.yaml'
    const configContent = await this.backend.readFile(configPath)
    return yaml.parse(configContent)
  }

  /**
   * Get the current subnet-node version.
   */
  async getVersion(): Promise<string> {
    try {
      await this.getConfig();
    } catch(_) {
      this.logger.error('Failed to get version');
      return '0.0.0'; // Return a default version if the config file does not exist
    }

    try {
      const response = await axios.post('http://localhost:8080/public', {
        jsonrpc: '2.0',
        method: 'version_getVersion',
        params: [],
        id: 1
      });
  
      return response.data.result.version;
    } catch(_){
      this.logger.error('Failed to get version');
      return '0.0.0'; // Return a default version if the version cannot be retrieved
    }
  }

  /**
   * Get the latest subnet-node version from the GitHub API.
   */
  async getLatestVersion(): Promise<string> {
    const response = await axios.get('https://api.github.com/repos/unicornultrafoundation/subnet-node/releases/latest');
    return response.data.tag_name;
  }

  /**
   * Download and install the latest subnet-node version.
   */
  async downloadAndInstall(version: string) {
    await this.stopService();
    const arch = os.arch() === 'arm64' ? 'arm64' : 'amd64';
    const url = `https://github.com/unicornultrafoundation/subnet-node/releases/download/${version}/subnet-${version}-linux-${arch}`;
    const targetPath = path.join(`/usr/local/bin/subnet`);
    // Download the file using execCommand to avoid being blocked by Windows
    await this.backend.execCommand({root: true}, 'curl', '-L', '-o', targetPath, url);
    // Move the file to the target path
    await this.backend.execCommand({root: true},'chmod', '+x', targetPath);
    await this.startService();
  }

  /**
   * Check the current subnet-node version and update if a new version is available.
   */
  async checkAndUpdate() {
   
    const currentVersion = await this.getVersion();
    const latestVersion = await this.getLatestVersion();

    if (semver.lt(currentVersion, latestVersion)) {
      this.logger.log(`Updating subnet-node from version ${currentVersion} to ${latestVersion}`);
      await this.downloadAndInstall(latestVersion);
    } else {
      this.logger.log(`subnet-node is up-to-date (version ${currentVersion})`);
    }
  }

  async start() {
    await this.backend.writeFile('/etc/init.d/subnet', SERVICE_SUBNET, 0o755)
    try {
      await this.startService();
      await this.checkStatus();
    } catch(_) {
      this.logger.error('Failed to start subnet-node');
    }

    await this.checkAndUpdate();
  }

  /**
   * Start a periodic check for subnet-node version every hour.
   */
  startPeriodicCheck() {
    setInterval(async () => {
      try {
        await this.checkAndUpdate();
      } catch (error) {
        this.logger.error('Error during periodic subnet-node check:', error);
      }
    }, 3600000); // 1 hour in milliseconds
  }


  async updateConfig(newConfig: any): Promise<void> {
    const configPath = SUBNET_NODE_DATA + '/config.yaml'
    const existingConfig = await this.getConfig()
    const mergedConfig = merge({}, existingConfig, newConfig)
    const updatedConfigContent = yaml.stringify(mergedConfig)
    await this.backend.writeFile(configPath, updatedConfigContent)
    await this.restartService();
  }

  async checkStatus(retries = 5, delay = 3000): Promise<boolean> {
    for (let i = 0; i < retries; i++) {
      try {
        const response = await axios.post('http://localhost:8080/status')
        return response.data.online
      } catch (error) {
        if (i < retries - 1) {
          await new Promise((resolve) => setTimeout(resolve, delay))
        }
      }
    }
    return false
  }

  
  async restartService() {
    await this.backend.execCommand({ root: true }, '/sbin/rc-service', 'subnet', 'restart')
  }

  async startService() {
    await this.backend.execCommand({ root: true }, '/sbin/rc-service', 'subnet', 'start')
  }
  async stopService() {
    await this.backend.execCommand({ root: true }, '/sbin/rc-service', 'subnet', 'stop')
  }
}

export default SubnetNode;