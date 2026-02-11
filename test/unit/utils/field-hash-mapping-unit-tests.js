/*global describe, it, beforeEach, afterEach*/
import mockedFunctions from "../setup-mocks.js";
import * as chai from "chai";
import {
    init,
    close,
    deleteDataBase,
    deleteTable,
    createIndexForJsonField,
    put,
    setupFieldHashMappings
} from "../../../src/utils/db.js";
import { getColumNameForJsonField } from "../../../src/utils/sharedUtils.js";
import { DATA_TYPES } from "../../../src/utils/constants.js";
import { getMySqlConfigs } from "@aicore/libcommonutils";

let expect = chai.expect;

describe("Field Hash Mapping System", function () {
    beforeEach(function () {
        close();
        init(getMySqlConfigs());
    });

    afterEach(function () {
        close();
    });

    describe("Field Hash Mapping Creation", function () {
        it("should create hash mapping table during initialization", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Track SQL statements executed
            const executedStatements = [];
            mockedFunctions.connection.query = function (sql, params, callback) {
                executedStatements.push(sql);
                callback(null, [], []);
            };

            // Re-initialize to trigger hash map table creation
            close();
            init(getMySqlConfigs());

            // Wait a bit for async operations to complete
            await new Promise((resolve) => setTimeout(resolve, 50));

            // Check that the system database and hash map table were created
            expect(executedStatements.some((sql) => sql.includes("CREATE DATABASE IF NOT EXISTS system"))).to.be.true;

            expect(executedStatements.some((sql) => sql.includes("CREATE TABLE IF NOT EXISTS system.field_hash_map")))
                .to.be.true;

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });

        it("should add field mapping when creating an index", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Track SQL statements and params executed
            const executedStatements = [];
            mockedFunctions.connection.query = function (sql, params, callback) {
                executedStatements.push({ sql, params });
                callback(null, { affectedRows: 1 }, []);
            };

            const tableName = "test.users";
            const jsonField = "email";
            const dataType = DATA_TYPES.VARCHAR(255);

            await createIndexForJsonField(tableName, jsonField, dataType, true, true);

            // Check that field mapping was added
            const hash = getColumNameForJsonField(jsonField);

            // Check for REPLACE INTO system.field_hash_map
            const hasMappingInsert = executedStatements.some(
                (item) =>
                    item.sql.includes("REPLACE INTO system.field_hash_map") &&
                    item.params &&
                    item.params.includes(hash) &&
                    item.params.includes(jsonField) &&
                    item.params.includes(tableName)
            );

            expect(hasMappingInsert).to.be.true;

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });
    });

    describe("Error Enhancement", function () {
        it("should enhance error messages with field information", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Generate an error with a column hash
            const tableName = "test.users";
            const jsonField = "email";
            const hash = getColumNameForJsonField(jsonField);

            // Mock the execute function to simulate field hash mappings
            mockedFunctions.connection.query = function (sql, params, callback) {
                if (sql.includes("SELECT fieldHash, tableName, fieldName")) {
                    // Return mock field hash mapping
                    callback(null, [
                        {
                            fieldHash: hash,
                            tableName: tableName,
                            fieldName: jsonField
                        }
                    ]);
                } else if (sql.includes("INSERT INTO") || sql.includes("REPLACE INTO")) {
                    // Simulate a duplicate key error
                    const error = new Error(`Duplicate entry 'test@example.com' for key '${tableName}.${hash}'`);
                    error.code = "ER_DUP_ENTRY";
                    callback(error);
                } else {
                    // Other statements succeed
                    callback(null, { affectedRows: 1 }, []);
                }
            };

            try {
                await put(tableName, { email: "test@example.com" });
                throw new Error("Should have thrown a duplicate key error");
            } catch (error) {
                // Check that the error was enhanced
                expect(error.message).to.include("email");
                expect(error.message).to.not.include(hash); // Should use the field name instead
                expect(error.message).to.include("Duplicate entry");
            }

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });
    });

    describe("Mapping Cleanup", function () {
        it("should clean up mappings when deleting a table", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Track SQL statements executed
            const executedStatements = [];
            mockedFunctions.connection.query = function (sql, params, callback) {
                executedStatements.push({ sql, params });
                callback(null, { affectedRows: 1 }, []);
            };

            const tableName = "test.users";

            await deleteTable(tableName);

            // Check that field mappings were deleted
            const hasMappingDelete = executedStatements.some(
                (item) =>
                    item.sql.includes("DELETE FROM system.field_hash_map WHERE tableName = ?") &&
                    item.params &&
                    item.params.includes(tableName)
            );

            expect(hasMappingDelete).to.be.true;

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });

        it("should clean up mappings when deleting a database", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Track SQL statements executed
            const executedStatements = [];
            mockedFunctions.connection.query = function (sql, params, callback) {
                executedStatements.push(sql);
                callback(null, { affectedRows: 1 }, []);
            };

            const databaseName = "test";

            await deleteDataBase(databaseName);

            // Check that field mappings were deleted
            const hasMappingDelete = executedStatements.some((sql) =>
                sql.includes(`DELETE FROM system.field_hash_map WHERE tableName LIKE 'test.%'`)
            );

            expect(hasMappingDelete).to.be.true;

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });
    });

    describe("setupFieldHashMappings", function () {
        it("should add field mappings for specified tables and fields", async function () {
            // Save the original execute function
            const originalExecute = mockedFunctions.connection.query;

            // Track SQL statements and parameters
            const executedStatements = [];
            mockedFunctions.connection.query = function (sql, params, callback) {
                if (Array.isArray(params)) {
                    executedStatements.push({ sql, params });
                } else {
                    executedStatements.push({ sql });
                }

                // For table existence check
                if (sql.includes("SELECT COUNT(*) as count FROM information_schema.tables")) {
                    callback(null, [{ count: 1 }]);
                } else {
                    callback(null, { affectedRows: 1 }, []);
                }
            };

            const mappings = {
                "test.users": ["firstName", "lastName", "email"],
                "test.orders": ["orderId", "customerId", "total"]
            };

            await setupFieldHashMappings(mappings);

            // Check that field mappings were added for all fields
            let mappingCount = 0;

            for (const [tableName, fields] of Object.entries(mappings)) {
                for (const field of fields) {
                    const hasMapping = executedStatements.some(
                        (item) =>
                            item.sql &&
                            item.sql.includes("REPLACE INTO system.field_hash_map") &&
                            item.params &&
                            item.params.includes(field) &&
                            item.params.includes(tableName)
                    );

                    if (hasMapping) {
                        mappingCount++;
                    }
                }
            }

            // Should have added mappings for all 6 fields
            expect(mappingCount).to.equal(6);

            // Restore the original execute function
            mockedFunctions.connection.query = originalExecute;
        });
    });
});
