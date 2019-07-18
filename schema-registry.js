'use strict';

const url = require('url');
const http = require('http');
const https = require('https');
const avro  = require('avsc');

const SchemaCache = require('./lib/schema-cache');
const pushSchema = require('./lib/schema-push');
const fetchSchema = require('./lib/schema-fetch');


class SchemaRegistry {
    constructor(schemaRegistryUrl) {
        const urlSections = url.parse(schemaRegistryUrl);
        this.schemaRegistry = {
            protocol: urlSections.protocol == "https" ? https : http,
            hostname: urlSections.hostname,
            port: urlSections.port,
            path: urlSections.pathname,
            cache: new SchemaCache()
        }
        if (urlSections.auth) {
            this.schemaRegistry.auth = urlSections.auth;
        }
    }

    getStandardMessage(schemaId, encodedMessage, version = 0) {
        const magicByte = version;
        const message = Buffer.alloc(encodedMessage.length + 5);
        message.writeUInt8(magicByte);
        message.writeUInt32BE(schemaId, 1);
        encodedMessage.copy(message, 5);
        return message;
    }

    getMessageEncoded(schema, message, options = null) {
        const avroType = avro.Type.forSchema(schema, options);
        const encodedMessage = avroType.toBuffer(message);
        return encodedMessage;
    }

    async encodeBySchema(type, topic, schema, message, options = null) {
        let encodedMessage = this.getMessageEncoded(schema, message, options);
        let schemaId = this.schemaRegistry.cache.getIdBySchema(schema);
        if (!schemaId) {
            // Register schema in the registry and get the schemaId
            schemaId = await pushSchema(this.schemaRegistry, topic, type, schema);
            this.schemaRegistry.cache.setSchema(schemaId, schema)
        }
        const stdMessage = this.getStandardMessage(schemaId, encodedMessage, 0);
        return stdMessage;
    }

    async encodeById({schemaId, message, options = null}) {
        let schema = this.schemaRegistry.cache.getSchemaById(schemaId);
        if (!schema) {
            // Get schema from schema registry
            schema = await fetchSchema(this.schemaRegistry, schemaId);
            this.schemaRegistry.cache.setSchema(schemaId, schema);
        }
        let encodedMessage = this.getMessageEncoded(schema, message, options);
        const stdMessage = this.getStandardMessage(schemaId, encodedMessage, 0);
        return stdMessage;
    }

    async encodeMessage({topic, schema, message, options = null}) {
        const resp =  await this.encodeBySchema('value', topic, schema, message, options);
        return resp;
    }

    async encodeKey({topic, schema, message, options = null}) {
        const resp = await this.encodeBySchema('key', topic, schema, message, options);
        return resp;
    }
}

const registry  = new SchemaRegistry("https://localhost:8087")
const s = {
    "type": "record",
    "name": "value_tests",
    "namespace": "myexample.com",
    "fields": [
        {"name": "id", "type": "string"},
        {
            "name": "amount",
            "type": "double",
            "default": 0
        }
    ] 
};
const m = {
    "id": '123sad-asdsad-asdad',
    "amount": 120000
}

let data = {
    topic: "tests",
    schema: s,
    message : m
}

registry.encodeById({schemaId: 2, message: m})

const secondGetByIdNow = () => {
    registry.encodeMessage(data);
}

setTimeout(secondGetByIdNow, 5000);


// module.exports = SchemaRegistry;