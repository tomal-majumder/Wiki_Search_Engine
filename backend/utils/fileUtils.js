// === utils/fileUtils.js ===
const fs = require('fs');
const path = require('path');


function getImageFilenames(fullbodyDocsList) {
    let fileNameList = [];

    for (let i = 0; i < fullbodyDocsList.length; i++) {
        try {
            const doc = fullbodyDocsList[i];

            // Check if 'images' array exists and has items
            if (doc.images && Array.isArray(doc.images)) {
                for (const imageInfo of doc.images) {
                    if (imageInfo.image_id) {
                        const fileNamePart = imageInfo.image_id;
                        fileNameList.push(fileNamePart);
                    }
                }
            }

        } catch (err) {
            console.log("Error processing document at index", i, ":", err);
        }
    }
    return fileNameList;
}

async function getBase64Images(imageNames, basePath) {
    const files = fs.readdirSync(basePath);
    const matching = files.filter(f => imageNames.some(n => f.includes(n)));
    const data = await Promise.all(matching.map(name => fs.promises.readFile(path.join(basePath, name))));
    return data.map(buf => Buffer.from(buf).toString('base64'));
}

module.exports = { getImageFilenames, getBase64Images };
