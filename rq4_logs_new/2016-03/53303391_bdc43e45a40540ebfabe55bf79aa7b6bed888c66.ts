declare module NodeJS  {
    interface Global {
        rootRequire: () => (name: string) => void;
    }
}