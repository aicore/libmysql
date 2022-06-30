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
import {createTable, get, put} from './utils/db.js';

function helloWorld(name) {
    return "Hello World " + name;
}

async function demo() {
    const tableName = 'test';
    const nameOfPrimaryKey = 'customer';
    const nameOfJsonColumn = 'customer_data';
   try {
        const result = await createTable(tableName, nameOfPrimaryKey, nameOfJsonColumn);
        console.log(result);
    } catch (e) {

        console.log(`exception is ++++++++++ ${e.stack}`);
    }

    const key = 'alice';
    const value = {
        'age': 100,
        'country': 'US'
    };

    const result2 = await put(tableName, nameOfPrimaryKey, key, nameOfJsonColumn, value);
    console.log(result2);

    try {
        //console.log(nameOfJsonColumn);
        const  result3 = await  get(tableName,nameOfPrimaryKey, key, nameOfJsonColumn);
        //console.log(result3.results[0].customer_data.age);
        console.log(result3);
    } catch (e) {
        console.log(e);
    }

}

demo();


export default helloWorld;
