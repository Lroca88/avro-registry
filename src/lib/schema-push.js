const pushSchema = (schemaRegistry, topic, type, schema) => {
    return new Promise((resolve, reject) => {
        const {protocol, hostname, port, path, auth} = schemaRegistry;
        const contentType = 'application/vnd.schemaregistry.v1+json';
        const body = JSON.stringify({"schema" : JSON.stringify(schema)});
        const contentLength = Buffer.byteLength(body);
        const requestOptions = {
            host: hostname,
            port: port,
            method: 'POST',
            path: `${path}subjects/${topic}-${type}/versions`,
            headers: {
            'Content-Type': contentType,
            'Content-Length': contentLength,
            },
            auth
        };

        const request = protocol.request(requestOptions, (res) => {
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
                resolve(response.id);
            });
        });

        request.on('error', (e) => {
            reject(e);
        });

        request.write(body);
        request.end();
    })
}

module.exports = pushSchema;