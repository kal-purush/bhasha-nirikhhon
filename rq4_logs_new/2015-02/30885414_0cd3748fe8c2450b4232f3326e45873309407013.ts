interface Function {
    name: string;
}

interface Error {
    stack?: string;
    captureStackTrace?: CaptureStackTraceImpl;
}

interface ErrorConstructor {
    captureStackTrace: CaptureStackTraceImpl;
}

interface CaptureStackTraceImpl {
    (error: Error, constructorOpt: any): void;
}