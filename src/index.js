/*
 * GNU AGPL-3.0 License
 *
 * Copyright (c) 2021 - present core.ai . All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with this program. If not, see https://opensource.org/licenses/AGPL-3.0.
 *
 */
import {createTable} from './utils/db.js';

function helloWorld(name) {
    return "Hello World " + name;
}

async function demo() {
    // getValueForKey('1', '2');
    const tableName = 'test';
    const nameOfPrimaryKey = 'customer';
    const nameOfJsonColumn = 'customer_data';
    const result = await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
    console.log(result);
   /* const result2 = await putValueForKey({column: nameOfPrimaryKey, value: 'alice'}, {
        column: nameOfJsonColumn,
        value: {Age: 20, lastname: 'bob'}
    }, tableName);
    console.log(result2);

    await putValueForKey({column: nameOfPrimaryKey, value: 'alice'}, {
        column: nameOfJsonColumn,
        value: {Age: 20, lastname: 'Dave'}
    }, tableName);
    const result3 = await getValueForKey({column: nameOfPrimaryKey, value: 'alice'}, nameOfJsonColumn, tableName);
    console.log(result3);

    */

}

demo();


export default helloWorld;
