declare module 'check-ip' {
  function checkIp(
    ip: string,
  ): {
    readonly isValid: boolean;
    readonly isPublicIp: boolean;
  };
  export = checkIp;
}