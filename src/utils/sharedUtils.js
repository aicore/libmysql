import crypto from "crypto";

/**
 * It takes a string and returns a hash of that string
 * @param {string} jsonField - The JSON field you want to query.
 * @returns {string} A string of hexadecimal characters.
 */
export function getColumNameForJsonField(jsonField) {
    // ignoring sonar security error as md5 function is not used for security
    // Md5 function is used here to increase the length of jsonfield to more than 64 characters
    return crypto.createHash('md5').update(jsonField).digest('hex'); //NOSONAR
}

export function isAlphaNumChar(char) {
    char = char.charCodeAt(0);
    if (!(char > 47 && char < 58) && // numeric (0-9)
        !(char > 64 && char < 91) && // upper alpha (A-Z)
        !(char > 96 && char < 123)) { // lower alpha (a-z)
        return false;
    }
    return true;
}

export function isAlphaChar(char) {
    char = char.charCodeAt(0);
    if (!(char > 64 && char < 91) && // upper alpha (A-Z)
        !(char > 96 && char < 123)) { // lower alpha (a-z)
        return false;
    }
    return true;
}