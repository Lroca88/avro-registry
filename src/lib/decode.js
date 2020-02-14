const avro = require("avsc");
const fetchSchema = require("./schema-fetch");

/**
 * Decode message from Avro to Object
 * @param {number} schemaRegistry The Schema Registry object
 * @param {Buffer} message Message properly formated, check _getStandardMessage
 * @param {object} options Options for avro library
 * @param {num} offset Offset of the payload, by default 5
 * @returns {string} Decoded Message
 */
async function decodeMessage(
  schemaRegistry,
  message,
  options = null,
  offset = 5
) {
  if (message.readUInt8(0) !== 0) {
    throw new Error(
      `Message needs the magic byte to be compliant with Confluent.`
    );
  }
  // Get schemaId (4 bytes with offset 1)
  const schemaId = message.readUInt32BE(1);
  let schema = schemaRegistry.cache.getSchemaById(schemaId);
  if (!schema) {
    schema = await fetchSchema(schemaRegistry, schemaId);
    schemaRegistry.cache.setSchema(schemaId, schema);
  }
  const avroType = avro.Type.forSchema(schema, options);
  const decodedMessage = avroType.decode(message, offset);
  return decodedMessage.value;
}

module.exports = decodeMessage;
