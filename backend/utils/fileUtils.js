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

module.exports = { getImageFilenames };
