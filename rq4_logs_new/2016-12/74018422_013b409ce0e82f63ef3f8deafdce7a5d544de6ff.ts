declare module 'hbs' {
  export function registerPartials(dir: string, callback?: () => void): void
  export function registerPartials(partialName: string, partialValue: string): void
}
