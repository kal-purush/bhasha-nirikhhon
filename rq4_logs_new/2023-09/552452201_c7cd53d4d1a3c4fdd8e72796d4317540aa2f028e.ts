import { useOnFirstMount } from 'utils/onFirstMount'

export function useConsoleEasterEgg() {
  useOnFirstMount(() => {
    console.info(
      '%c██╗   ██╗███╗   ██╗██╗    ██╗   ██╗███╗   ██╗██╗\n' +
        '██║   ██║████╗  ██║██║    ██║   ██║████╗  ██║██║\n' +
        '██║   ██║██╔██╗ ██║██║    ██║   ██║██╔██╗ ██║██║\n' +
        '██║   ██║██║╚██╗██║██║    ██║   ██║██║╚██╗██║██║\n' +
        '╚██████╔╝██║ ╚████║██║    ╚██████╔╝██║ ╚████║██║\n' +
        ' ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═════╝ ╚═╝  ╚═══╝╚═╝',
      'background-color: black; color: white; font-size: 20px;',
    )
    console.info(
      '%c"雲丹うに"は人生だ...🍜 https://twitter.com/MM____uni',
      'background-color: black; color: white; font-size: 20px;',
    )
  })
}