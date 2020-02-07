'use strict';

/** @typedef { import('./types').EncodingValidationError } EncodingValidationError */
/** @typedef { import('./types').SchemaRegistryOptions } SchemaRegistryOptions */

const url = require('url');
const http = require('http');
const https = require('https');
const avro  = require('avsc');

const SchemaCache = require('./lib/schema-cache');
const pushSchema = require('./lib/schema-push');
const fetchSchema = require('./lib/schema-fetch');
const errors = require('./lib/schema-errors')

/**
 * @param {object} message Encoded message
 * @param {avro.Type} schema Avro schema
 * @returns {Array<EncodingValidationError>}
*/
const validateEncodedMessage = (message, schema) => {
    const errors = [];
    schema.isValid(message, {
        errorHook: (path, value, type) => {
            errors.push({
                path: path.join('.'),
                value,
                expectedType: type.toString()
            })
        }
    })

    return errors
}

/**
* SchemaRegistry Class
*/
class SchemaRegistry {
    /**
	 * Creating standard message defined by Confluent Schema Registry
     * Check https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
	 * @param {number} schemaId Id of the schema in the registry
     * @param {Buffer} encodedMessage Message already encoded with avro
     * @param {number} version Schema version (By default 0)
 	*/
    _getStandardMessage(schemaId, encodedMessage, version = 0) {
        const magicByte = version;                                          // Magic byte contains the format version, currently always 0 (1 byte)
        const message = Buffer.alloc(encodedMessage.length + 5);            // Allocating 5 bytes for headers (| MagicByte | SchemaId |)
        message.writeUInt8(magicByte);                                      
        message.writeUInt32BE(schemaId, 1);                                 // Write the schemaId with offset 1 in Big Endian
        encodedMessage.copy(message, 5);                                    // Copy encodedMessage to final message with offset 5
        return message;                                                     // | MagicByte | SchemaId | EncodedMessage |
    }                                                                       //   1byte       4bytes     Payload -> any size

    /**
	 * Encode message with Avro
	 * @param {object} schema Avro schema
     * @param {object} message Message to encode
     * @param {object} options Options for avro library
 	*/
    _getMessageEncoded(schema, message, options = null) {
        const avroType = avro.Type.forSchema(schema, options);              // Import avro schema

        if (this.options.validateEncodedMessages) {
            const schemaErrors = validateEncodedMessage(message, avroType);
            if (schemaErrors.length > 0) {
                throw new errors.ValidationError(schemaErrors)
            }
        }

        const encodedMessage = avroType.toBuffer(message)

        return encodedMessage;
    }

    /**
	 * Decode message from Avro to Object
	 * @param {Buffer} message Message properly formated, check _getStandardMessage 
     * @param {object} options Options for avro library
     * @param {num} offset Offset of the payload, by default 5
 	*/
    async _getMessageDecoded(message, options = null, offset = 5,) {
        if (message.readUInt8(0) !== 0) {
            throw new Error(`Message needs the magic byte to be compliant with Confluent.`);
        }
        const schemaId = message.readUInt32BE(1);                           // Get schemaId (4 bytes with offset 1)
        let schema = await this._getSchema(schemaId);                       // Get the schema using its Id
        const avroType = avro.Type.forSchema(schema, options)               // Import avro schema 
        const decodedMessage = avroType.decode(message, offset);            // Decode message, default offset should be 5
        return decodedMessage.value;                                        // Return the decoded value
    }

    /**
	 * Get schema 
	 * @param {number} schemaId The Schema Id in the registry
 	*/
    async _getSchema(schemaId) {
        let schema = this.schemaRegistry.cache.getSchemaById(schemaId);     // Try to get a hit in the cache first 
        if (!schema) {                                                      // If it is a miss
            schema = await fetchSchema(this.schemaRegistry, schemaId);      // Get schema from schema registry
            this.schemaRegistry.cache.setSchema(schemaId, schema);          // Set the schema in Cache for next time
        }
        return schema;                                                      // Return schema
    }

    /**
	 * Encode message when a schema is provided
	 * @param {string} type The type could be value or key
     * @param {string} topic Topic name
     * @param {object} schema The Avro schema object
     * @param {object} message The message data
     * @param {object} options Options for avro library
 	*/
    async _encodeBySchema(type, topic, schema, message, options = null) {
        let encodedMessage = this._getMessageEncoded(schema, message, options);     // Encode message with avro library
        let schemaId = this.schemaRegistry.cache.getIdBySchema(schema);             // Try to get SchemaId from cache
        if (!schemaId) {                                                            // If it is a miss
            schemaId = await pushSchema(this.schemaRegistry, topic, type, schema);  // Register new schema in the registry and get the schemaId
            this.schemaRegistry.cache.setSchema(schemaId, schema)                   // Set the new schema in Cache for next time
        }
        const stdMessage = this._getStandardMessage(schemaId, encodedMessage, 0);   // Prepare econded message to be compliant with Confluent standard
        return stdMessage;                                                          // Return the standard message
    }

    /**
	 * Encode message when a schemaId is provided
	 * @param {number} schemaId The Schema Id in the registry
     * @param {object} message The message data
     * @param {object} options Options for avro library
 	*/
    async encodeById(schemaId, message, options = null) {                   
        let schema = await this._getSchema(schemaId);                               // Get the Schema object
        let encodedMessage = this._getMessageEncoded(schema, message, options);     // Encode message with avro
        const stdMessage = this._getStandardMessage(schemaId, encodedMessage, 0);   // Prepare econded message to be compliant with Confluent standard
        return stdMessage;                                                          // // Return the standard message
    }

    /**
	 * Encode message when a schema is provided
     * @param {string} topic Topic name
     * @param {object} schema The Avro schema object
     * @param {object} message The message data
     * @param {object} options Options for avro library
 	*/
    async encodeMessage(topic, schema, message, options = null) {                   
        const resp =  await this._encodeBySchema('value', topic, schema, message, options);     // Encode message using schema with 'value' in the subject
        return resp;                                                                            // Return message encoded (buffer)
    }

    /**
	 * Encode key when a schema is provided
     * This method is for encoding the key of the message
     * in case of sending messages with keys through kafka 
	 * @param {number} schemaId The Schema Id in the registry
     * @param {object} key The key data
     * @param {object} options Options for avro library
 	*/
    async encodeKey(topic, schema, key, options = null) {
        const resp = await this._encodeBySchema('key', topic, schema, key, options);        // Encode key using schema with 'key' in the subject
        return resp;                                                                        // Return key encoded (buffer)
    }

    /**
	 * Decode message/key. Either Message or Key will be a standard buffer
	 * @param {buffer} message The buffer to decode
     * @param {object} options Options for avro library
 	*/
    async decodeMessage(message, options = null) {                                          
        const resp = await this._getMessageDecoded(message, options);                       // Decode message
        return resp;                                                                        // Return message decoded
    }

    /**
	 * Constructor
	 * @param {string} schemaRegistryUrl Url for schema registry rest api
     * @param {SchemaRegistryOptions} options
 	*/
     constructor(schemaRegistryUrl, options = {}) {
        const urlSections = url.parse(schemaRegistryUrl);                   // Parsing url in sections
        this.schemaRegistry = {                                             // Building the schemaRegistry object
            protocol: urlSections.protocol == "https" ? https : http,
            hostname: urlSections.hostname,
            port: urlSections.port,
            path: urlSections.pathname,
            cache: new SchemaCache()
        }
        this.options = options
        if (urlSections.auth) {                                             // Setting Auth in case is needed
            this.schemaRegistry.auth = urlSections.auth;
        }
    }
}

module.exports = SchemaRegistry;