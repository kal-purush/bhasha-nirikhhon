import license from '../assets/license.txt?url';
import { parseCSV } from '../classes/data/datautils/csvreader';
import SetElement from '../classes/data/setutils/SetElement';

export default async function licensePlaintext() {
    let str = `
This file contains legal notices of software being used by this application.

node-stack-trace: this uses the MIT license:
Copyright (c) 2011 Felix Geisendörfer (felix@debuggable.com)

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.

`;
    const response = await fetch(license);
    const { columnNames, content } = parseCSV(
        await response.text(),
        'Licenses' as unknown as SetElement
    );
    for (const r of content) {
        for (let i = 0; i < columnNames.length; i++) {
            str += columnNames[i] + ': ' + r[i] + '\n';
        }
        str += '\n\n';
    }
    return str;
}