const { Client } = require('pg');
const fs = require('fs');

require('dotenv').config();

const client = new Client({
    host: process.env.POSTGRES_HOST,
    port: process.env.POSTGRES_PORT,
    database: process.env.POSTGRES_DATABASE,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
})

const etl = async () => {
    await client.connect();
    const csvFiles = fs.readdirSync('./datos').filter(x => x.includes('.csv'));
    addTransformationTable(client);
    for (const fileName of csvFiles) {
        const [name] = fileName.split('.csv');
        let data = require('fs').readFileSync(`./datos/${fileName}`, 'utf8');
        data = data.split('\n').slice(1);
        await client.query(`CREATE TABLE IF NOT EXISTS ${name.toLowerCase()} (id BIGSERIAL PRIMARY KEY, nombre_organismo VARCHAR, denominacion_del_cargo VARCHAR, escalafon VARCHAR, grado INTEGER, remuneracion_en_uyu NUMERIC)`);
        for (const line of data) {
            const splittedLine = splitCSVLine(line);
            if(splittedLine.length > 1) {
                let nombreOrganismo, denominacionCargo, escalafon, grado, remuneracionUyu;
                switch (name.toLowerCase()) {
                    case 'miem':
                        nombreOrganismo = splittedLine[7];
                        denominacionCargo = splittedLine[8];
                        escalafon = splittedLine[9];
                        grado = splittedLine[10] === 'S/D'? 0: parseInt(splittedLine[10]);
                        remuneracionUyu = splittedLine[11];
                        break;
                    case 'mtop':
                        if(splittedLine[8]== 'N/C' && splittedLine[9] === '-1' ) {
                            continue;
                        }
                        nombreOrganismo = `'${splittedLine[4]}'`;
                        denominacionCargo = splittedLine[7];
                        escalafon = splittedLine[8];
                        grado = parseInt(splittedLine[9]);
                        remuneracionUyu = splittedLine[11];
                        break;
                    case 'ursec':
                        nombreOrganismo = `'${splittedLine[6]}'`;
                        denominacionCargo = splittedLine[7];
                        escalafon = splittedLine[8];
                        grado = parseInt(splittedLine[9]);
                        remuneracionUyu = splittedLine[10];
                        break;
                }
                query = `INSERT INTO ${name.toLowerCase()} (nombre_organismo, denominacion_del_cargo, escalafon, grado, remuneracion_en_uyu) VALUES (${nombreOrganismo.replaceAll(`"`,`'`)}, '${denominacionCargo}', '${escalafon}', ${grado}, ${remuneracionUyu})`;
                await client.query(query);
            }
        }
    }
    await client.end();
}

const splitCSVLine = (line) => {
    if(line === '') return [];
    var matches = line.match(/(\s*"[^"]+"\s*|\s*[^,]+|,)(?=,|$)/g);
    for (var n = 0; n < matches.length; ++n) {
        matches[n] = matches[n].trim();
        if (matches[n] == ',') matches[n] = '';
    }
    if (line[0] == ',') matches.unshift("");
    return matches;
}

const addTransformationTable = async () => {
    await client.query('CREATE TABLE IF NOT EXISTS transformations (id BIGSERIAL PRIMARY KEY, read_from VARCHAR, read_type VARCHAR, write_to VARCHAR, write_type VARCHAR, action VARCHAR, created_at VARCHAR)');
}


etl();