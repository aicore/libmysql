/* Declaring a constant variable named MAX_NUMBER_OF_DOCS_ALLOWED and assigning it the value of 1000. */
export const MAX_NUMBER_OF_DOCS_ALLOWED = 1000;
/* Defining a constant variable called PRIMARY_COLUMN and assigning it the value of 'documentID'. */
export const PRIMARY_COLUMN = 'documentID';
/* Defining a constant variable called JSON_COLUMN and assigning it the value of 'document'. */
export const JSON_COLUMN = 'document';
/* Creating a constant called DATA_TYPES. It is an object with three properties. The first property is DOUBLE, which is
 a string. The second property is VARCHAR, which is a function that takes a parameter. The third property is INT,
 which is a string. */
export const DATA_TYPES = {
    // https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
    DOUBLE: 'DOUBLE',
    VARCHAR: function (a = 50) {
        return `VARCHAR(${a})`;
    },
    INT: 'INT'
};
// https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
/* Defining a constant variable named MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME and
assigning it the value 64. */
export const MAXIMUM_LENGTH_OF_MYSQL_TABLE_NAME_AND_COLUMN_NAME = 63;
export const MAXIMUM_LENGTH_OF_MYSQL_DATABASE_NAME = 63;
