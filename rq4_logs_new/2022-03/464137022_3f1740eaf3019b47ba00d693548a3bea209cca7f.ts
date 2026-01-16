import { registerPlugin, getPluginConfig, HandledRoute } from '@scullyio/scully';
import { JSDOM } from 'jsdom';

const Plugin = async (dom: JSDOM, route: HandledRoute): Promise<JSDOM> => {
  // Find codeblocks recognized by code syntax highlighter
  let codeblocks = dom.window.document.querySelectorAll('code[class^="language-"]');
  for (let i = 0; i < codeblocks.length; i++) {
    let codeblockFileName = null;
    let codeblockFileExtension = null;

    // Find invalid class names
    // (If the class name contains the filename or path not just extension, it will not be recognized correctly by code syntax highlighter.)
    const targetClassName = Array.from(codeblocks[i].classList).find((className: string) => {
      if (className.match(/^language-(.+\.(.+))$/)) {
        codeblockFileName = RegExp.$1;
        codeblockFileExtension = RegExp.$2;
        return true;
      }
    });

    if (!codeblockFileName || !codeblockFileExtension || !targetClassName) continue;

    // Fix the class name
    const validClassName = `language-${codeblockFileExtension}`;
    codeblocks[i].classList.replace(targetClassName, validClassName);

    // Append the filename as a new element
    const filenameLabel = dom.window.document.createElement('span');
    filenameLabel.innerHTML = codeblockFileName;
    filenameLabel.style.background = '#ffffffaa';
    filenameLabel.style.fontSize = '0.6rem';
    filenameLabel.style.padding = '0.2rem 1rem 0.2rem 1rem';
    const filenameLabelContainer = dom.window.document.createElement('div');
    filenameLabelContainer.appendChild(filenameLabel);
    filenameLabelContainer.className = 'esa-codeblock-filename';
    filenameLabelContainer.style.position = 'absolute';
    filenameLabelContainer.style.left = '0px';
    filenameLabelContainer.style.top = '0px';
    filenameLabelContainer.style.lineHeight = '1rem';
    const filenameLabelSpacer = dom.window.document.createElement('div');
    filenameLabelSpacer.style.fontSize = '0.5rem';
    filenameLabelSpacer.innerHTML = '&#8203;'; // zero width space character
    codeblocks[i].parentNode.insertBefore(filenameLabelSpacer, codeblocks[i]);
    codeblocks[i].parentNode.insertBefore(filenameLabelContainer, codeblocks[i]);
  }

  return dom;
};

registerPlugin('postProcessByDom', 'esaPostProcess', Plugin);