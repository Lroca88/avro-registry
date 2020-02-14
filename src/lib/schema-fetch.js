const fetchSchema = (schemaRegistry, schemaId) => {
  return new Promise((resolve, reject) => {
    if (!schemaId) {
      return reject(new Error(`SchemaId is undefined`));
    }
    const { protocol, hostname, port, path, key, secret } = schemaRegistry;
    const requestOptions = {
      host: hostname,
      port: port,
      path: `${path}schemas/ids/${schemaId}`,
      auth: key && secret ? `${key}:${secret}` : null
    };

    const request = protocol.get(requestOptions, res => {
      let data = "";
      res.on("data", d => {
        data += d;
      });
      res.on("error", e => {
        reject(e);
      });
      res.on("end", () => {
        if (res.statusCode == 200) {
          const schemaStr = JSON.parse(data).schema;
          const schema = JSON.parse(schemaStr);
          return resolve(schema);
        } else {
          const error = JSON.parse(data);
          return reject(
            new Error(
              `Schema registry error: ${error.error_code} - ${error.message}`
            )
          );
        }
      });
    });

    request.on("error", e => {
      reject(e);
    });
  });
};

module.exports = fetchSchema;
