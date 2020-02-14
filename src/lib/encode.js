const avro = require("avsc");
const fetchSchema = require("./schema-fetch");
const pushSchema = require("./schema-push");
const validationError = require("./validation-error");

/**
 * Encode message with Avro
 * @param {object} schema Avro schema
 * @param {object} message Message to encode
 * @param {object} options Options for avro library
 * @returns {Buffer} Encoded Message
 */
function encodeMessage(schema, message, validate, options = null) {
  const avroType = avro.Type.forSchema(schema, options);
  if (validate) {
    validateBeforeEncode(avroType, message);
  }
  const encodedMessage = avroType.toBuffer(message);
  return encodedMessage;
}

/**
 * @param {avro.Type} schema Avro schema
 * @param {object} message Encoded message
 * @throws {errors} Schema Validation Errors
 */
function validateBeforeEncode(schema, message) {
  const validationErrors = [];
  schema.isValid(message, {
    errorHook: (path, value, type) => {
      validationErrors.push({
        path: path.join("."),
        value,
        expectedType: type.toString()
      });
    }
  });
  if (validationErrors.length > 0) {
    throw new validationError(validationErrors);
  }
}

/**
 * Creating standard message defined by Confluent Schema Registry
 * | MagicByte 1Byte | SchemaId 4Bytes | EncodedMessage NBytes |
 * Check https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
 * @param {number} schemaId Id of the schema in the registry
 * @param {Buffer} encodedMessage Message already encoded with avro
 * @param {number} version Schema version (By default 0)
 * @returns {Buffer} Standard Encoded Message
 */
function getStandard(schemaId, encodedMessage, version = 0) {
  // Magic byte contains the format version, currently always 0 (1 byte)
  const magicByte = version;
  // Allocating 5 bytes for headers (| MagicByte | SchemaId |)
  const message = Buffer.alloc(encodedMessage.length + 5);
  message.writeUInt8(magicByte);
  // Write the schemaId with offset 1 in Big Endian
  message.writeUInt32BE(schemaId, 1);
  // Copy encodedMessage to final message with offset 5
  encodedMessage.copy(message, 5);

  return message;
}

/**
 * Encode message when a schemaId is provided
 * @param {number} schemaRegistry The Schema Registry object
 * @param {number} schemaId The Schema Id in the registry
 * @param {object} message The message data
 * @param {object} options Options for avro library
 */
async function encodeMessageById(
  schemaRegistry,
  schemaId,
  message,
  options = null
) {
  let schema = schemaRegistry.cache.getSchemaById(schemaId);
  if (!schema) {
    schema = await fetchSchema(schemaRegistry, schemaId);
    schemaRegistry.cache.setSchema(schemaId, schema);
  }
  let encodedMessage = encodeMessage(
    schema,
    message,
    schemaRegistry.validateMessages,
    options
  );
  const stdMessage = getStandard(schemaId, encodedMessage, 0);
  return stdMessage;
}

/**
 * Encode message when a schema is provided
 * @param {number} schemaRegistry The Schema Registry object
 * @param {string} type The type could be value or key
 * @param {string} topic Topic name
 * @param {object} schema The Avro schema object
 * @param {object} message The message data
 * @param {object} options Options for avro library
 */
async function encodeMessageBySchema(
  schemaRegistry,
  type,
  topic,
  schema,
  message,
  options = null
) {
  let encodedMessage = encodeMessage(
    schema,
    message,
    schemaRegistry.validateMessages,
    options
  );
  let schemaId = schemaRegistry.cache.getIdBySchema(schema);
  if (!schemaId) {
    schemaId = await pushSchema(schemaRegistry, topic, schema, type);
    schemaRegistry.cache.setSchema(schemaId, schema);
  }
  const stdMessage = getStandard(schemaId, encodedMessage, 0);
  return stdMessage;
}

module.exports = {
  encodeMessageById,
  encodeMessageBySchema
};
