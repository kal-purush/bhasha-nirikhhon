import { Input, Inputs } from '@pulumi/pulumi';
import { newAppFactory } from '../helm-app';
import { InputArray } from '../input';
import { PullPolicy, ServiceType } from '../types';

export const chart = 'longhorn';
export const defaultVersion = '1.1.0';

/**
 * Put in the 'longhorn-system' namespace
 */
export const newLonghorn = newAppFactory<LonghornArgs>('longhorn', {
  chart,
  catalog: 'rancher-charts',
  repoUrl: 'https://charts.longhorn.io',
  version: defaultVersion,
});

interface Image {
  repository?: Input<string>;
  tag?: Input<string>;
}

export interface LonghornArgs {
  global?: Input<{
    cattle: Input<{
      systemDefaultRegistry: Input<string>;
    }>;
  }>;

  image?: Input<{
    longhorn?: Input<{
      engine?: Input<Image>;
      manager?: Input<Image>;
      ui?: Input<Image>;
      instanceManager?: Input<Image>;
      shareManager?: Input<Image>;
    }>;
    csi?: Input<{
      attacher?: Input<Image>;
      provisioner?: Input<Image>;
      nodeDriverRegistrar?: Input<Image>;
      resizer?: Input<Image>;
      snapshotter?: Input<Image>;
    }>;
    pullPolicy?: Input<PullPolicy>;
  }>;

  service?: Input<{
    ui?: Input<{

      /**
       * @default 'ClusterIP'
       */
      type?: Input<ServiceType>;

      nodePort?: Input<string>;
    }>;
    manager?: Input<{

      /**
       * @default 'ClusterIP'
       */
      type?: Input<ServiceType>;

      nodePort?: Input<string>;
    }>;
  }>;

  persistence?: Input<{

    /**
     * @default true
     */
    defaultClass?: Input<boolean>;

    /**
     * @default 3
     */
    defaultClassReplicaCount?: Input<number>;

    /**
     * @default 'Delete'
     */
    reclaimPolicy?: Input<'Delete'>;

    recurringJobs?: Input<{

      /**
       * @default false
       */
      enable?: Input<boolean>;

      jobList?: InputArray<string>;
    }>;
  }>;

  csi?: Input<{
    kubeletRootDir?: Input<string>;
    attacherReplicaCount?: Input<number>;
    provisionerReplicaCount?: Input<number>;
    resizerReplicaCount?: Input<number>;
    snapshotterReplicaCount?: Input<number>;
  }>;

  defaultSettings?: Input<{
    backupTarget?: Input<string>;
    backupTargetCredentialSecret?: Input<string>;
    allowRecurringJobWhileVolumeDetached?: Input<boolean>;
    createDefaultDiskLabeledNodes?: Input<boolean>;
    defaultDataPath?: Input<string>;
    defaultDataLocality?: Input<string>;
    replicaSoftAntiAffinity?: Input<string>;
    storageOverProvisioningPercentage?: Input<string>;
    storageMinimalAvailablePercentage?: Input<string>;
    upgradeChecker?: Input<string>;
    defaultReplicaCount?: Input<number>;
    guaranteedEngineCPU?: Input<string>;
    defaultLonghornStaticStorageClass?: Input<string>;
    backupstorePollInterval?: Input<string>;
    taintToleration?: Input<string>;
    priorityClass?: Input<string>;
    autoSalvage?: Input<string>;
    autoDeletePodWhenVolumeDetachedUnexpectedly?: Input<string>;
    disableSchedulingOnCordonedNode?: Input<boolean>;
    replicaZoneSoftAntiAffinity?: Input<string>;
    volumeAttachmentRecoveryPolicy?: Input<string>;
    nodeDownPodDeletionPolicy?: Input<string>;
    allowNodeDrainWithLastHealthyReplica?: Input<boolean>;
    mkfsExt4Parameters?: Input<string>;
    disableReplicaRebuild?: Input<boolean>;
    replicaReplenishmentWaitInterval?: Input<string>;
    disableRevisionCounter?: Input<boolean>;
    systemManagedPodsImagePullPolicy?: Input<PullPolicy>;
    allowVolumeCreationWithDegradedAvailability?: Input<boolean>;
    autoCleanupSystemGeneratedSnapshot?: Input<boolean>;
  }>;

  privateRegistry?: Input<{
    registryUrl?: Input<string>;
    registryUser?: Input<string>;
    registryPasswd?: Input<string>;
    registrySecret?: Input<string>;
  }>;

  // TODO
  // resources?: Input<>;

  ingress?: Input<{

    /**
     * @default false
     */
    enabled?: Input<boolean>;

    /**
     * @default 'xip.io'
     */
    host?: Input<string>;

    /**
     * Set this to true in order to enable TLS on the ingress record
     * A side effect of this will be that the backend service will be connected at port 443
     * 
     * @default false
     */
    tls?: Input<boolean>;

    /**
     * If TLS is set to true, you must declare what secret will store the key/certificate for TLS
     * 
     * @default 'longhorn.local-tls'
     */
    tlsSecret?: Input<string>;

    /**
     * Ingress annotations done as key:value pairs
     * If you're using kube-lego, you will want to add:
     * kubernetes.io/tls-acme: true
     * 
     * For a full list of possible ingress annotations, please see
     * ref: https://github.com/kubernetes/ingress-nginx/blob/master/docs/annotations.md
     * 
     * If tls is set to true, annotation ingress.kubernetes.io/secure-backends: "true" will automatically bet set
     */
    annotations?: Inputs;

    /**
     * If you're providing your own certificates, please use this to add the certificates as secrets
     * key and certificate should start with -----BEGIN CERTIFICATE----- or
     * -----BEGIN RSA PRIVATE KEY-----
     *
     * name should line up with a tlsSecret set further up
     * If you're using kube-lego, this is unneeded, as it will create the secret for you if it is not set
     *
     * It is also possible to create and manage the certificates outside of this helm chart
     * Please see README.md for more information
     */
    secrets?: InputArray<{
      name: Input<string>;
      key: Input<string>;
      certificate: Input<string>;
    }>;
  }>;

  /**
   * Configure a pod security policy in the Longhorn namespace to allow privileged pods.
   * 
   * @default true
   */
  enablePSP?: Input<boolean>;

  /**
   * Specify override namespace, specifically this is useful for using longhorn as a sub-chart
   * and its release namespace is not the `longhorn-system`
   */
  namespaceOverride?: Input<string>;
}