# libmysql

This library helps to model MySQL as document DB. We have simplified MySQL to have only
two columns. More columns will be added only while creating an index for JSON fields using
`createIndexForJsonField` method.

1. column1: documentId, a random alphanumeric of type `VARCHAR(32).`
2. column2: Document column to store documents in MySQL. Documents are stored as `JSON` documents.

`documentId` is created  when we put a document into the database by calling `put` method


## Code Guardian

[![<app> build verification](https://github.com/aicore/libmysql/actions/workflows/build_verify.yml/badge.svg)](https://github.com/aicore/libmysql/actions/workflows/build_verify.yml)

<a href="https://sonarcloud.io/project/issues?id=aicore_libmysql">
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=alert_status" alt="Sonar code quality check" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=security_rating" alt="Security rating" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=vulnerabilities" alt="vulnerabilities" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=coverage" alt="Code Coverage" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=bugs" alt="Code Bugs" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=reliability_rating" alt="Reliability Rating" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=sqale_rating" alt="Maintainability Rating" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=ncloc" alt="Lines of Code" />
  <img src="https://sonarcloud.io/api/project_badges/measure?project=aicore_libmysql&metric=sqale_index" alt="Technical debt" />
</a>

## Examples

### How to create a table?

```javascript
import {createTable, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
init(configs);
const tableName = 'customers';
try {
    await createTable(tableName);
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

#### How table looks after create table?

| documentID  | document   |
|--------------------------|----------------|

### How to put a document on a table?

```javascript
import {put, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
init(configs);
const tableName = 'customers';
const document = {
    'lastName': 'Alice',
    'Age': 100,
    'active': true,
    'location': {
        'city': 'Banglore',
        'state': 'Karnataka',
        'layout': {
            'block': '1stblock'
        }

    }
};
try {
    const docId = await put(tableName, document);
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```
#### How table looks after putting data to table?

| documentID  | document |
|--------------------------|---------|
|d20ab50a3e4deefe508f1b26a32e2632|`{'lastName': 'Alice','Age': 100, 'active': true, 'location': {'city': 'Banglore','state': 'Karnataka','layout': {'block': '1stblock'} }}` |

### How to delete a document from a database?

```javascript
import {deleteKey, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
init(configs);
const tableName = 'customers';
const docId = '1234';

try {
    await deleteKey(tableName, docId);
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

### How to get a document?

```javascript
import {get, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
const tableName = 'customers';
const docId = '1234';
try {
    const document = await get(tableName, docId);
    console.log(JSON.stringify(document));
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

### How to scan a database to get a list of matching documents?

```javascript
import {getFromNonIndex, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
const tableName = 'customers';
const queryObject = {
    'lastName': 'Alice',
    'Age': 100
};
try {
    const documents = await getFromNonIndex(tableName, queryObject);     
    console.log(JSON.stringify(documents));
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```
Note that only a maximum of 1000 entries will be returned. Use page options to get paginated results.
To get paginated results past 1000 results, Eg. `getFromNonIndex(tableName, queryObject, {pageOffset: 56,pageLimit: 1000});`
* pageOffset [number]: specify which row to start retrieving documents from. Eg: to get 10 documents from
  the 100'th document, you should specify pageOffset = 100 and pageLimit = 10
* pageLimit [number]: specify number of documents to retrieve. Eg: to get 10 documents from
  the 100'th document, you should specify pageOffset = 100 and pageLimit = 10

### How to delete a table?

```javascript
import {deleteTable, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
const tableName = 'customers';
try {
    await deleteTable(tableName);
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

### How to create an index for a JSON field?

```javascript
import {createIndexForJsonField, DATA_TYPES, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
const tableName = 'customers';
try {
    // To make index unique constraint for new column and index set isUnique to true;
    const isUnique = false;
    await createIndexForJsonField(tableName, 'lastName', DATA_TYPES.VARCHAR(50), isUnique);
    await createIndexForJsonField(tableName, 'Age', DATA_TYPES.INT, isUnique);
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```
#### How table looks after creating index?

| documentID  | document                                                                                                                                    | ef21925fada6dfb684b5d8ec72114bb1|9d8d2d5ab12b515182a505f54db7f538|
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|----|---|
|9d8d2d5ab12b515182a505f54db7f538| `{"Age": 100, "active": true, "lastName": "Alice", "location": {"city": "Banglore", "state": "Karnataka", "layout": {"block": "1stblock"}}}` |Alice| 100|

### How to get data from indexed fields?

```javascript
import {getFromIndex, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
const tableName = 'customers';
const queryObject = {
    'lastName': 'Alice',
    'Age': 100
};
try {
    const documents = await getFromIndex(tableName, queryObject);
    console.log(JSON.stringify(documents));
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

Note that only a maximum of 1000 entries will be returned. Use page options to get paginated results.
To get paginated results past 1000 results, Eg. `getFromIndex(tableName, queryObject, {pageOffset: 56,pageLimit: 1000});`
* pageOffset [number]: specify which row to start retrieving documents from. Eg: to get 10 documents from
  the 100'th document, you should specify pageOffset = 100 and pageLimit = 10
* pageLimit [number]: specify number of documents to retrieve. Eg: to get 10 documents from
  the 100'th document, you should specify pageOffset = 100 and pageLimit = 10

### How to update / re-write an existing document?

```javascript
import {update, init, close} from "@aicore/libmysql";
import {getMySqlConfigs} from "@aicore/libcommonutils";

const configs = getMySqlConfigs();
init(configs);

const tableName = 'customers';
const docId = 1234;
const document = {
    'FirstName': 'Alice',
    'lastName': 'Bob',
    'Age': 20,
    'active': true
};

try {
    const docId = await update(tableName, docId, document);
    // or if you want to do conditional updates, ie update the document
    // only if the condition specified is satisfied.
    const docId1 = await update(tableName, docId, document, "$.Age=20");
} catch (e) {
    console.error(JSON.stringify(e));
}
close();
```

# Commands available

## Building

Since this is a pure JS template project, build command just runs test with coverage.

```shell
> npm install   // do this only once.
> npm run build
```

## Linting

To lint the files in the project, run the following command:

```shell
> npm run lint
```

To Automatically fix lint errors:

```shell
> npm run lint:fix
```

## Testing

To run all tests:

```shell
> npm run test
```

Additionally, to run unit/integration tests only, use the commands:

```shell
> npm run test:unit
> npm run test:integ
```

## Coverage Reports

To run all tests with coverage:

```shell
> npm run cover
 
```

After running coverage, detailed reports can be found in the coverage folder listed in the output of coverage command.
Open the file in browser to view detailed reports.

To run unit/integration tests only with coverage

```shell
> npm run cover:unit
> npm run cover:integ
```

Sample coverage report:
![image](https://user-images.githubusercontent.com/5336369/148687351-6d6c12a2-a232-433d-ab62-2cf5d39c96bd.png)

### Unit and Integration coverage configs

Unit and integration test coverage settings can be updated by configs `.nycrc.unit.json` and `.nycrc.integration.json`.

See https://github.com/istanbuljs/nyc for config options.

# Publishing packages to NPM

## Preparing for release

Please run `npm run release` on the `main` branch and push the changes to main. The release command will bump the npm
version.

!NB: NPM publish will faill if there is another release with the same version.

## Publishing

To publish a package to npm, push contents to `npm` branch in
this repository.

## Publishing `@aicore/package*`

If you are looking to publish to package owned by core.ai, you will need access to the GitHub Organization
secret `NPM_TOKEN`.

For repos managed by [aicore](https://github.com/aicore) org in GitHub, Please contact your Admin to get access to
core.ai's NPM tokens.

## Publishing to your own npm account

Alternatively, if you want to publish the package to your own npm account, please follow these docs:

1. Create an automation access token by following this [link](https://docs.npmjs.com/creating-and-viewing-access-tokens)
   .
2. Add NPM_TOKEN to your repository secret by following
   this [link](https://docs.npmjs.com/using-private-packages-in-a-ci-cd-workflow)

To edit the publishing workflow, please see file: `.github/workflows/npm-publish.yml`

# Dependency updates

We use Rennovate for dependency updates: https://blog.logrocket.com/renovate-dependency-updates-on-steroids/

* By default, dep updates happen on sunday every week.
* The status of dependency updates can be viewed here if you have this repo permissions in
  github: https://app.renovatebot.com/dashboard#github/aicore/template-nodejs
* To edit rennovate options, edit the rennovate.json file in root,
  see https://docs.renovatebot.com/configuration-options/
  Refer

# Code Guardian

Several automated workflows that check code integrity are integrated into this template.
These include:

1. GitHub actions that runs build/test/coverage flows when a contributor raises a pull request
2. [Sonar cloud](https://sonarcloud.io/) integration using `.sonarcloud.properties`
    1. In sonar cloud, enable Automatic analysis from `Administration
       Analysis Method` for the first
       time ![image](https://user-images.githubusercontent.com/5336369/148695840-65585d04-5e59-450b-8794-54ca3c62b9fe.png)

## IDE setup

SonarLint is currently available as a free plugin for jetbrains, eclipse, vscode and visual studio IDEs.
Use sonarLint plugin for webstorm or any of the available
IDEs from this link before raising a pull request: https://www.sonarlint.org/ .

SonarLint static code analysis checker is not yet available as a Brackets
extension.

## Local integration testing
### Steps
#### [Ubuntu install docker instructions](https://docs.docker.com/engine/install/ubuntu/)
#### [Or Other OS Install docker](https://docs.docker.com/engine/install/)
#### Install and setup Mysql Docker image

```console
# install docker
sudo docker pull mysql
sudo docker images
sudo docker run -d --name mysql-server -p 3306:3306 -e "MYSQL_ROOT_PASSWORD=1234" mysql

# install mysql client
sudo apt-get install mysql-client
# connect to mysql running in docker
# type password as 1234
mysql -h 127.0.0.1 -u root -p

# create a database
CREATE DATABASE testdb;

# now Goto file `setupIntegTest.js`  and uncomment the config part. Tests can now be run

# list running dockers
sudo docker container ls
# stop docker
sudo docker container stop  <container id obtained from previous step>
```
#### Uncomment config in setupIntegTest.js
#### Remove docker image
```console
# get list of all avalible containers
sudo  docker container ls -a

# Remove container first
sudo docker container rm   <container id from previous step>

# Remover docker image
sudo  docker image rm mysql
```

#### How to start docker container
```console
# get mysql container id
sudo  docker container ls -a
# start Mysql
sudo docker start <contianer Id>

# Connect to MySql
mysql -h 127.0.0.1 -u root -p 
```
## Internals

### Testing framework: Mocha , assertion style: chai

See https://mochajs.org/#getting-started on how to write tests
Use chai for BDD style assertions (expect, should etc..). See move here: https://www.chaijs.com/guide/styles/#expect

### Mocks and spies: sinon

if you want to mock/spy on fn() for unit tests, use sinon. refer docs: https://sinonjs.org/

### Note on coverage suite used here:

we use c8 for coverage https://github.com/bcoe/c8. Its reporting is based on nyc, so detailed docs can be found
here: https://github.com/istanbuljs/nyc ; We didn't use nyc as it do not yet have ES module support
see: https://github.com/digitalbazaar/bedrock-test/issues/16 . c8 is drop replacement for nyc coverage reporting tool
