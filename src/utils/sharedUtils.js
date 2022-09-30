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
