const fetchSchema = (schemaRegistry, schemaId) => {
    return new Promise((resolve, reject) => {
        const {protocol, hostname, port, path, auth} = schemaRegistry;
        const requestOptions = {
            host: hostname,
            port: port,
            path: `${path}schemas/ids/${schemaId}`,
            auth
        };

        const request = protocol.get(requestOptions, (res) => {
            let data = '';
            res.on('data', (d) => {
                data += d;
            });
            res.on('error', (e) => {
                reject(e);
            });
            res.on('end', () => {
                const response = JSON.parse(data);
                if (res.statusCode !== 200) {
                    reject(new Error(`Schema registry error: ${response.error_code} - ${response.message}`));
                }
                const schemaStr = JSON.parse(data).schema;
                const schema = JSON.parse(schemaStr);
                resolve(schema);
            });
        });

        request.on('error', (e) => {
            reject(e);
        });
    })
}

module.exports = fetchSchema;