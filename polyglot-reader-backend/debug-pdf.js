const pdfParse = require('pdf-parse');
console.log('pdfParse.default:', pdfParse.default);
console.log('pdfParse.PDFParse:', pdfParse.PDFParse);
console.log('Is pdfParse a function?', typeof pdfParse === 'function');

try {
    if (typeof pdfParse.default === 'function') {
        console.log('pdfParse.default is the function');
    }
} catch (e) { }

try {
    if (typeof pdfParse === 'function') {
        console.log('pdfParse itself is the function');
    }
} catch (e) { }
