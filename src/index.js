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
import {createTable, getValueForKey} from './utils/db.js';

function helloWorld(name) {
    return "Hello World " + name;
}

async function demo() {
    // getValueForKey('1', '2');
    const table = 'test';
    const key = 'customer';
    const value = 'data';
    const result = await createTable(table, key, value);
    console.log(result);

}

demo();


export default helloWorld;
